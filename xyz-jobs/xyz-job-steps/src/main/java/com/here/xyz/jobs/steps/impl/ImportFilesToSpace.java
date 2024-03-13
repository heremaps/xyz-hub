/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */

package com.here.xyz.jobs.steps.impl;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.WRITER;
import static com.here.xyz.jobs.steps.execution.db.Database.loadDatabase;
import static com.here.xyz.util.web.HubWebClient.HubWebClientException;

import com.here.xyz.jobs.steps.execution.db.Database;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import io.vertx.core.json.JsonObject;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This step imports a set of user provided inputs and imports their data into a specified space.
 * This step produces exactly one output of type {@link FeatureStatistics}.
 */
public class ImportFilesToSpace extends SpaceBasedStep<ImportFilesToSpace> {
  private static final Logger logger = LogManager.getLogger();
  private static final String JOB_DATA_SUFFIX = "_job_data";
  private static int importThreadCnt = 2;

  @Override
  public List<Load> getNeededResources() {
    try {
      StatisticsResponse statistics = loadSpaceStatistics(getSpaceId(), EXTENSION);
      int acus = calculateNeededAcus(statistics.getCount().getValue(), statistics.getDataSize().getValue());
      Database db = loadDatabase(loadSpace(getSpaceId()).getStorage().getId(), WRITER);

      return Collections.singletonList(new Load().withResource(db).withEstimatedVirtualUnits(acus));
    }
    catch (HubWebClientException e) {
      //TODO: log error
      //TODO: is the step failed? Retry later? It could be a retryable error as the prior validation succeeded, depending on the type of HubWebClientException
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getTimeoutSeconds() {
    //TODO: Return an estimation based on the input data size
    return 24 * 3600;
  }

  @Override
  public String getDescription() {
    return "Imports the data to space " + getSpaceId();
  }

  @Override
  public void deleteOutputs() {
    //Nothing to do here as no outputs are produced by this step
  }

  @Override
  public boolean validate() throws ValidationException {
    try {
      //Check if the space is actually existing
      loadSpace(getSpaceId());
      StatisticsResponse statistics = loadSpaceStatistics(getSpaceId(), EXTENSION);
      if (statistics.getCount().getValue() > 0)
        throw new ValidationException("Space is not empty");
    }
    catch (HubWebClientException e) {
      throw new ValidationException("Error loading resource " + getSpaceId(), e);
    }

    //TODO: Validate the input files in S3
    return !loadInputs().isEmpty();
  }

  private int calculateNeededAcus(long featureCount, long byteSize) {
    // TODO: Calculate based on file size
    return 0;
  }

  @Override
  public void execute(){
    logger.info("Importing input files from s3://" + bucketName() + "/" + inputS3Prefix() + " in region " + bucketRegion()
        + " into space " + getSpaceId() + " ...");

    try {

      logger.info("Loading space config for space {}", getSpaceId());
      Space space = loadSpace(getSpaceId());
      logger.info("Getting storage database for space {}", getSpaceId());
      Database db = loadDatabase(space.getStorage().getId(), WRITER);

      //Prepare
      logger.info("Set readOnly for space{}" + getSpaceId());
      hubWebClient().patchSpace(getSpaceId(), new HashMap<>(){{ put("readOnly", true);}});

      logger.info("Prepare - Retrieve new Version from {}" + getSpaceId());
      long newVersion = runReadQuerySync(buildVersionSequenceIncrement(getSchema(db), getRootTableName(space)), db, calculateNeededAcus(0,0),
              rs -> {
                rs.next();
                return rs.getLong(1);
              });

      logger.info("Prepare - Create tmp import-trigger table for {}" + getSpaceId());
      runWriteQuerySync(buildCreatImportTrigger(getSchema(db), getRootTableName(space), "ANONYMOUS",newVersion), db, calculateNeededAcus(0,0));

      logger.info("Prepare - Create tmp-import-table for {}" + getSpaceId());
      runWriteQuerySync(buildTemporaryTableForImportQuery(getSchema(db), getRootTableName(space)), db, calculateNeededAcus(0,0));

      logger.info("Prepare - Fill tmp import table {}" + getSpaceId());
      fillTemporaryTableWithInputs(db, getRootTableName(space), loadInputs(), bucketName(), bucketRegion());

      //Execute
      for (int i = 0; i < importThreadCnt; i++) {
        runReadQuery(buildImportQuery(getSchema(db), getRootTableName(space), "jsonwkb", buildSuccessCallbackQuery(), i), db, calculateNeededAcus(0,0), false);
      }
    }
    catch (SQLException | TooManyResourcesClaimed | HubWebClientException e){
      //@TODO: ErrorHandling! <- Is it necessary here? Anything that should be catched / transformed?
      logger.warn("Error!",e); //TODO: Can be removed, no need to log here, as the framework will log all exceptions thrown by #execute()
      throw new RuntimeException(e); //TODO: If nothing should be handled here, better rethrow the original exception
    }
  }

  private void fillTemporaryTableWithInputs(Database db, String table, List<Input> inputs, String bucketName, String bucketRegion) throws SQLException, TooManyResourcesClaimed {
    List<SQLQuery> queryList = new ArrayList<>();
    for (Input input : inputs){
      if(input instanceof UploadUrl uploadUrl) {
        JsonObject data = new JsonObject()
                //.put("compressed", inputs.isCompressed())
                .put("filesize", uploadUrl.getByteSize());

        queryList.add(
                new SQLQuery("""                
                            INSERT INTO  ${schema}.${table} (s3_uri, state, data)
                                VALUES (aws_commons.create_s3_uri(#{bucketName},#{s3Key},#{bucketRegion}), #{state}, #{data}::jsonb);
                        """)
                        .withVariable("schema", getSchema(db))
                        .withVariable("table", table + JOB_DATA_SUFFIX)
                        .withNamedParameter("s3Key", input.getS3Key())
                        .withNamedParameter("bucketName", bucketName)
                        .withNamedParameter("bucketRegion", bucketRegion)
                        .withNamedParameter("state", "SUBMITTED")
                        .withNamedParameter("data", data.toString())
        );
      }
    }
    runBatchWriteQuerySync(SQLQuery.batchOf(queryList), db, calculateNeededAcus(0, 0));
  }

  @Override
  protected boolean onAsyncFailure(Exception e) {
    /** Failed Import
     *  onFailure (take retryable into account)
     */
    return super.onAsyncFailure(e);
  }

  @Override
  protected void onAsyncSuccess() {
    /** Finalize Import
     * - cleanUp: remove trigger / delete temporary table
     * - provide getStatistics
     * - release readOnly lock
     */
    try {
      logger.info("Loading space config for space {}", getSpaceId());
      Space space = loadSpace(getSpaceId());
      logger.info("Getting storage database for space {}", getSpaceId());
      Database db = loadDatabase(space.getStorage().getId(), WRITER);

      /** Collecting statistics */
      List<String> statistics = runReadQuerySync(buildStatisticDataOfTemporaryTableQuery(getSchema(db), getRootTableName(space)), db, calculateNeededAcus(0, 0),
              rs -> {
                List<String> result = new ArrayList<>();
                while (rs.next())
                  result.add(rs.getString(1));
                return result;
              });
      logger.info("Statistics-size:{}", statistics.size());

      logger.info("Finalize - Delete tmp import-trigger table for {}" + getSpaceId());
      runWriteQuerySync(buildDropImportTrigger(getSchema(db), getRootTableName(space)), db, calculateNeededAcus(0, 0));

      logger.info("Finalize - Delete tmp-import-table for {}" + getSpaceId());
      runWriteQuerySync(buildDropTemporaryTableForImportQuery(getSchema(db), getRootTableName(space)), db, calculateNeededAcus(0, 0));
      hubWebClient().patchSpace(getSpaceId(), new HashMap<>() {{
        put("readOnly", false);
      }});

      //TODO: Register one output of type {@link FeatureStatistics} as last step using registerOutputs()

    }
    catch (SQLException | TooManyResourcesClaimed | HubWebClientException e){
      //@TODO: ErrorHandling!
      logger.warn("Error!",e);
      throw new RuntimeException(e);
    }
    super.onAsyncSuccess();
  }

  @Override
  public void resume() throws Exception {
    /*
    No cleanup needed, in any case, sending the index creation query again will work
    as it is using the "CREATE SEQUENCE IF NOT EXISTS" semantics
     */
    execute();
  }

  @Override
  public void cancel() throws Exception {
    super.cancel();
  }

  private SQLQuery buildTemporaryTableForImportQuery(String schema, String table){
    return new SQLQuery("""
                    CREATE TABLE IF NOT EXISTS ${schema}.${table}
                           (                                                            
                                s3_uri aws_commons._s3_uri_1 NOT NULL, --s3uri
                                state text NOT NULL, --jobtype
                                execution_count int DEFAULT 0, --amount of retries
                                data jsonb COMPRESSION lz4, --statistic data
                                i SERIAL,
                                CONSTRAINT ${primaryKey} PRIMARY KEY (s3_uri)
                           );
                    """)
            .withVariable("table", table + JOB_DATA_SUFFIX)
            .withVariable("schema", schema)
            .withVariable("primaryKey", table + JOB_DATA_SUFFIX + "_primKey");
  }
  private SQLQuery buildDropTemporaryTableForImportQuery(String schema, String table){
    return new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table};")
            .withVariable("table", table + JOB_DATA_SUFFIX)
            .withVariable("schema", schema);
  }

  private SQLQuery buildCreatImportTrigger(String schema, String table, String targetAuthor, long targetSpaceVersion){
    return new SQLQuery("CREATE OR REPLACE TRIGGER insertTrigger BEFORE INSERT ON ${schema}.${table} "
            + "FOR EACH ROW EXECUTE PROCEDURE ${schema}.xyz_import_trigger_v2('${{author}}', ${{spaceVersion}});")
            .withQueryFragment("spaceVersion", "" + targetSpaceVersion)
            .withQueryFragment("author", targetAuthor)
            .withVariable("schema", schema)
            .withVariable("table", table);
  }
  private SQLQuery buildDropImportTrigger(String schema, String table){
    return new SQLQuery("DROP TRIGGER IF EXISTS insertTrigger ON ${schema}.${table};")
            .withVariable("table", table)
            .withVariable("schema", schema);
  }

  //TODO: Move to XyzSpaceTableHelper or so (it's the nth time we have that implemented somewhere)
  private SQLQuery buildVersionSequenceIncrement(String schema, String table) {
    return new SQLQuery("SELECT nextval('${schema}.${sequence}')")
            .withVariable("schema", schema)
            .withVariable("sequence", table + "_version_seq");
  }

  private SQLQuery buildStatisticDataOfTemporaryTableQuery(String schema, String table) {
    return new SQLQuery("SELECT data->'import_statistics' FROM ${schema}.${table};")
            .withVariable("schema", schema)
            .withVariable("table", table + JOB_DATA_SUFFIX);
  }

  private SQLQuery buildImportQuery(String schema, String table, String format, SQLQuery successQuery, int i) {
    return new SQLQuery("SELECT xyz_import_into_space(#{schema},#{temporary_tbl}::regclass,#{target_tbl}::regclass,#{format},'${{successQuery}}',#{i})")
            .withNamedParameter("schema", schema)
            .withNamedParameter("target_tbl", schema+".\""+table+"\"")
            .withNamedParameter("temporary_tbl",  schema+".\""+(table + JOB_DATA_SUFFIX)+"\"")
            .withNamedParameter("format", format)
            .withNamedParameter("i", i)
            .withQueryFragment("successQuery","select "+successQuery.substitute().text().replaceAll("'","''"));
  }
}
