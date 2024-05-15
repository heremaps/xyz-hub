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

package com.here.xyz.jobs.steps.impl.imp;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.WRITER;
import static com.here.xyz.jobs.steps.execution.db.Database.loadDatabase;
import static com.here.xyz.util.web.XyzWebClient.WebClientException;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.jobs.steps.execution.db.Database;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.impl.tools.ResourceAndTimeCalculator;
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
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This step imports a set of user provided inputs and imports their data into a specified space.
 * This step produces exactly one output of type {@link FeatureStatistics}.
 */
public class ImportFilesToSpace extends SpaceBasedStep<ImportFilesToSpace> {
  private static final Logger logger = LogManager.getLogger();

  private static final String JOB_DATA_PREFIX = "job_data_";

  private static final int MAX_DB_THREAD_CNT = 10;

  private Format format = Format.GEOJSON;

  private Phase phase;

  @JsonView({Internal.class})
  private long uncompressedTotalBytes;

  @JsonView({Internal.class, Static.class})
  private int fileCount = -1;

  @JsonView({Internal.class, Static.class})
  private int calculatedThreadCount;

  @JsonView({Internal.class, Static.class})
  private int estimatedSeconds = -1;

  public enum Format {
    CSV_GEOJSON,
    CSV_JSONWKB,
    GEOJSON;
  }
  public enum Phase {
    VALIDATE, CALCULATE_ACUS, SET_READONLY, RETRIEVE_NEW_VERSION, CREATE_TRIGGER, CREATE_TMP_TABLE, RESET_SUCCESS_MARKER,
    FILL_TMP_TABLE, EXECUTE_IMPORT, RETRIEVE_STATISTICS, WRITE_STATISTICS, DROP_TRIGGER, DROP_TMP_TABLE, RELEASE_READONLY;
  }
  public Format getFormat() {
    return format;
  }

  public void setFormat(Format format) {
    this.format = format;
  }

  public ImportFilesToSpace withFormat(Format format) {
    setFormat(format);
    return this;
  }

  public Phase getPhase() {
    return phase;
  }

  @Override
  public List<Load> getNeededResources() {
    try {

      double acus = calculateNeededAcus();
      Database db = loadDatabase(loadSpace(getSpaceId()).getStorage().getId(), WRITER);

      return Collections.singletonList(new Load().withResource(db).withEstimatedVirtualUnits(acus));
    }
    catch (WebClientException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getTimeoutSeconds() {
    return 15 * 3600;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    if(estimatedSeconds == -1 && getSpaceId() != null) {
      estimatedSeconds = ResourceAndTimeCalculator.getInstance().calculateImportTimeInSeconds(getSpaceId(), getUncompressedUploadBytesEstimation());
      logger.info("[{}] Import estimatedSeconds {}", getGlobalStepId(), estimatedSeconds);
    }
    return estimatedSeconds;
  }

  @Override
  public String getDescription() {
    return "Imports the data to space " + getSpaceId();
  }

  @Override
  public void deleteOutputs() {
      try {
          onAsyncSuccess();
      } catch (Exception e) {
          throw new RuntimeException(e);
      }
  }

  @Override
  public boolean validate() throws ValidationException {
    super.validate();
    try {
      logAndSetPhase(Phase.VALIDATE);
      //Check if the space is actually existing
      loadSpace(getSpaceId());
      StatisticsResponse statistics = loadSpaceStatistics(getSpaceId(), EXTENSION);
      long featureCount = statistics.getCount().getValue();

      if (featureCount > 0)
        throw new ValidationException("Space is not empty");
    }
    catch (WebClientException e) {
      throw new ValidationException("Error loading resource " + getSpaceId(), e);
    }

    List<Input> inputs = loadInputs();
    //Inputs are missing, the step is not ready to be executed
    if (inputs.isEmpty())
      return false;

    uncompressedTotalBytes = getUncompressedUploadBytesEstimation();

    for (int i = 0; i < inputs.size(); i++) {
      if(inputs.get(i) instanceof UploadUrl uploadUrl) {
        //TODO: Think about how many files we want to quick check
        if(i < 2)
          ImportFilesQuickValidator.validate(uploadUrl, format);
      }
    }

    return true;
  }

  @Override
  public void execute() throws WebClientException, SQLException, TooManyResourcesClaimed {
    logAndSetPhase(null, "Importing input files from s3://" + bucketName() + "/" + inputS3Prefix() + " in region " + bucketRegion()
        + " into space " + getSpaceId() + " ...");

    logAndSetPhase(null, "Loading space config for space "+getSpaceId());
    Space space = loadSpace(getSpaceId());
    logAndSetPhase(null, "Getting storage database for space  "+getSpaceId());
    Database db = loadDatabase(space.getStorage().getId(), WRITER);

    logAndSetPhase(Phase.SET_READONLY);
    hubWebClient().patchSpace(getSpaceId(), Map.of("readOnly", true));

    logAndSetPhase(Phase.RETRIEVE_NEW_VERSION);
    long newVersion = runReadQuerySync(buildVersionSequenceIncrement(getSchema(db), getRootTableName(space)), db, 0,
            rs -> {
              rs.next();
              return rs.getLong(1);
            });

    logAndSetPhase(Phase.CREATE_TRIGGER);
    runWriteQuerySync(buildCreatImportTrigger(getSchema(db), getRootTableName(space), "ANONYMOUS",newVersion), db, 0);

    createAndFillTemporaryTable(db);

    int dbThreadCnt = calculateDBThreadCount();
    double neededAcusForOneThread = calculateNeededAcus() / dbThreadCnt;

    logAndSetPhase(Phase.EXECUTE_IMPORT);

    for (int i = 1; i <= dbThreadCnt; i++) {
      logAndSetPhase(Phase.EXECUTE_IMPORT, "Start Import Thread number "+i);
      runReadQueryAsync(buildImportQuery(getSchema(db), getRootTableName(space)), db, neededAcusForOneThread , false);
    }
  }

  private void createAndFillTemporaryTable(Database db) throws SQLException, TooManyResourcesClaimed, WebClientException {
    boolean tmpTableNotExistsAndHasNoData = true;
    try {
      //Check if temporary table exists and has data - if yes we assume a retry and skip the creation + filling.
      tmpTableNotExistsAndHasNoData = runReadQuerySync(buildTableCheckQuery(getSchema(db)), db, 0,
              rs -> {
                rs.next();
                if(rs.getLong("count") == 0 )
                  return true;
                return false;
              });

    }catch (SQLException e){
      //We expect that
      if(e.getSQLState() != null && !e.getSQLState().equals("42P01")) {
        throw e;
      }
    }

    if(!tmpTableNotExistsAndHasNoData) {
      logAndSetPhase(Phase.RESET_SUCCESS_MARKER);
      runWriteQuerySync(resetSuccessMarkerAndRunningOnes(getSchema(db)), db, 0);
    }else {
      logAndSetPhase(Phase.CREATE_TMP_TABLE);
      runWriteQuerySync(buildTemporaryTableForImportQuery(getSchema(db)), db, 0);

      logAndSetPhase(Phase.FILL_TMP_TABLE);
      fillTemporaryTableWithInputs(db, loadInputs(), bucketName(), bucketRegion());
    }
  }

  @Override
  protected void onStateCheck() {
    try {
      logAndSetPhase(null, "Loading space config for space "+getSpaceId());
      Space space = loadSpace(getSpaceId());
      logAndSetPhase(null, "Getting storage database for space  "+getSpaceId());
      Database db = loadDatabase(space.getStorage().getId(), WRITER);

      runReadQuerySync(buildProgressQuery(getSchema(db)), db, 0,
               rs -> {
                rs.next();
                int totalCnt = rs.getInt("total_cnt");
                long processedBytes = rs.getLong("processed_bytes");
                int submittedCnt = rs.getInt("submitted_cnt");
                int finishedCnt = rs.getInt("finished_cnt");
                int failedCnt = rs.getInt("failed_cnt");

                float progress = Float.valueOf(processedBytes) / Float.valueOf(uncompressedTotalBytes);
                getStatus().setEstimatedProgress(progress);

                logAndSetPhase(null, "Progress["+progress+"] => totalCnt:"+totalCnt
                        +" ,processedBytes:"+processedBytes+" ,submittedCnt:"+submittedCnt+" ,finishedCnt:"+finishedCnt+" ,failedCnt:"+failedCnt);
                return progress;
              });
    }catch (Exception e){
      //TODO: What to do? Only log? Report Status is not that important. Further Ignore "table does not exists error" - report 0 in this case.
      logger.error(e);
    }
  }

  @Override
  protected void onAsyncSuccess() throws WebClientException,
          SQLException, TooManyResourcesClaimed, IOException {
    /** Finalize Import
     * - cleanUp: remove trigger / delete temporary table
     * - provide getStatistics
     * - release readOnly lock
     */

    logAndSetPhase(null, "Loading space config for space "+getSpaceId());
    Space space = loadSpace(getSpaceId());

    logAndSetPhase(null, "Getting storage database for space  "+getSpaceId());
    Database db = loadDatabase(space.getStorage().getId(), WRITER);

    try {
      logAndSetPhase(Phase.RETRIEVE_STATISTICS);
      FeatureStatistics statistics = runReadQuerySync(buildStatisticDataOfTemporaryTableQuery(getSchema(db)), db,
              0, rs -> rs.next()
                      ? new FeatureStatistics().withFeatureCount(rs.getLong("imported_rows")).withByteSize(rs.getLong("imported_bytes"))
                      : new FeatureStatistics());

      logAndSetPhase(null, "Statistics: bytes="+statistics.getByteSize()+" rows="+ statistics.getFeatureCount());

      registerOutputs(List.of(statistics), true);

      logAndSetPhase(Phase.WRITE_STATISTICS);
      registerOutputs(new ArrayList<>(){{ add(statistics);}}, true);

      logAndSetPhase(Phase.DROP_TRIGGER);
      runWriteQuerySync(buildDropImportTrigger(getSchema(db), getRootTableName(space)), db, 0);

      logAndSetPhase(Phase.DROP_TMP_TABLE);
      runWriteQuerySync(buildDropTemporaryTableForImportQuery(getSchema(db)), db, 0);

      logAndSetPhase(Phase.RELEASE_READONLY);
      hubWebClient().patchSpace(getSpaceId(), Map.of("readOnly", false));

    }catch (SQLException e){
      //relation "*_job_data" does not exist - can happen when we have received twice a SUCCESS_CALLBACK
      if(e.getSQLState() != null && e.getSQLState().equals("42P01")) {
        logAndSetPhase(null,"_job_data table got already deleted!");
        return;
      }
      throw e;
    }
  }

  @Override
  protected boolean onAsyncFailure() {
    //TODO: Inspect the error provided in the status and decide whether it is retryable (return-value)
    /** Failed Import
     *  onFailure (take retryable into account)
     */
    return false;
  }

  @Override
  public void resume() throws Exception {
    execute();
  }

  private SQLQuery buildTemporaryTableForImportQuery(String schema) {
    return new SQLQuery("""
                    CREATE TABLE IF NOT EXISTS ${schema}.${table}
                           (
                                --s3_uri aws_commons._s3_uri_1 NOT NULL, --s3uri
                                s3_bucket text NOT NULL,
                                s3_path text NOT NULL,
                                s3_region text NOT NULL,
                                state text NOT NULL, --jobtype
                                execution_count int DEFAULT 0, --amount of retries
                                data jsonb COMPRESSION lz4, --statistic data
                                i SERIAL,
                                CONSTRAINT ${primaryKey} PRIMARY KEY (s3_path)
                           );
                    """)
            .withVariable("table", getTemporaryTableName())
            .withVariable("schema", schema)
            .withVariable("primaryKey", getTemporaryTableName() + "_primKey");
  }

  private void fillTemporaryTableWithInputs(Database db, List<Input> inputs, String bucketName, String bucketRegion) throws SQLException, TooManyResourcesClaimed {
    List<SQLQuery> queryList = new ArrayList<>();
    for (Input input : inputs){
      if(input instanceof UploadUrl uploadUrl) {
        JsonObject data = new JsonObject()
                .put("compressed", uploadUrl.isCompressed())
                .put("filesize", uploadUrl.getByteSize());

        queryList.add(
                new SQLQuery("""                
                            INSERT INTO  ${schema}.${table} (s3_bucket, s3_path, s3_region, state, data)
                                VALUES (#{bucketName}, #{s3Key}, #{bucketRegion}, #{state}, #{data}::jsonb)
                                ON CONFLICT (s3_path) DO NOTHING;
                        """) //TODO: Why would we ever have a conflict here? Why to fill the table again on resume()?
                        .withVariable("schema", getSchema(db))
                        .withVariable("table", getTemporaryTableName())
                        .withNamedParameter("s3Key", input.getS3Key())
                        .withNamedParameter("bucketName", bucketName)
                        .withNamedParameter("bucketRegion", bucketRegion)
                        .withNamedParameter("state", "SUBMITTED")
                        .withNamedParameter("data", data.toString())
        );
      }
    }
    //Add final entry
    queryList.add(
            new SQLQuery("""                
                            INSERT INTO  ${schema}.${table} (s3_bucket, s3_path, s3_region, state, data)
                                VALUES (#{bucketName}, #{s3Key}, #{bucketRegion}, #{state}, #{data}::jsonb)
                                ON CONFLICT (s3_path) DO NOTHING;
                        """) //TODO: Why would we ever have a conflict here? Why to fill the table again on resume()?
                    .withVariable("schema", getSchema(db))
                    .withVariable("table", getTemporaryTableName())
                    .withNamedParameter("s3Key", "SUCCESS_MARKER")
                    .withNamedParameter("bucketName", bucketName)
                    .withNamedParameter("state", "SUCCESS_MARKER")
                    .withNamedParameter("bucketRegion", bucketRegion)
                    .withNamedParameter("data", "{}"));
    runBatchWriteQuerySync(SQLQuery.batchOf(queryList), db, 0);
  }

  private SQLQuery buildDropTemporaryTableForImportQuery(String schema) {
    return new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table};")
            .withVariable("table", getTemporaryTableName())
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

  private SQLQuery buildStatisticDataOfTemporaryTableQuery(String schema) {
    return new SQLQuery("""
            SELECT sum((data->'filesize')::bigint) as imported_bytes,
            	count(1) as imported_files,
            	sum(SUBSTRING((data->'import_statistics'->>'table_import_from_s3'),0,POSITION('rows' in data->'import_statistics'->>'table_import_from_s3'))::bigint) as imported_rows
            	FROM ${schema}.${table}
            WHERE POSITION('SUCCESS_MARKER' in state) = 0;
          """)
            .withVariable("schema", schema)
            .withVariable("table", getTemporaryTableName());
  }

  private SQLQuery buildProgressQuery(String schema) {
    return new SQLQuery("""
            SELECT
                sum(1::int) as total_cnt,
                sum((data->'filesize')::bigint) as processed_bytes,	
                sum((state = 'SUBMITTED')::int) as submitted_cnt,
                sum((state = 'FINISHED')::int) as finished_cnt,
                sum((state = 'FAILED')::int) as failed_cnt
              FROM ${schema}.${table}
            	 WHERE POSITION('SUCCESS_MARKER' in state) = 0
            	 AND STATE != 'SUBMITTED';
          """)
            .withVariable("schema", schema)
            .withVariable("table", getTemporaryTableName());
  }

  private SQLQuery buildImportQuery(String schema, String table) {
    SQLQuery successQuery = buildSuccessCallbackQuery();
    SQLQuery failureQuery = buildFailureCallbackQuery();
    return new SQLQuery("CALL xyz_import_start(#{schema}, #{temporary_tbl}::regclass, #{target_tbl}::regclass, #{format}, '${{successQuery}}', '${{failureQuery}}');")
        .withAsyncProcedure(true)
        .withNamedParameter("schema", schema)
        .withNamedParameter("target_tbl", schema+".\""+table+"\"")
        .withNamedParameter("temporary_tbl",  schema+".\""+(getTemporaryTableName())+"\"")
        .withNamedParameter("format", format.toString())
        .withQueryFragment("successQuery", successQuery.substitute().text().replaceAll("'","''"))
        .withQueryFragment("failureQuery", failureQuery.substitute().text().replaceAll("'","''"));
  }

  private SQLQuery buildTableCheckQuery(String schema) {
    return new SQLQuery("SELECT count(1) FROM ${schema}.${table};")
            .withVariable("schema", schema)
            .withVariable("table", getTemporaryTableName());
  }

  private SQLQuery resetSuccessMarkerAndRunningOnes(String schema) {
    return new SQLQuery("""
            UPDATE ${schema}.${table}
              SET state =
                CASE
                  WHEN state = 'SUCCESS_MARKER_RUNNING' THEN 'SUCCESS_MARKER'
                  WHEN state = 'RUNNING' THEN 'SUBMITTED'
                END
              WHERE state IN ('SUCCESS_MARKER_RUNNING', 'RUNNING');
            """)
            .withVariable("schema", schema)
            .withVariable("table", getTemporaryTableName());
  }

  private double calculateNeededAcus() {
    // Each ACU needs 2GB RAM
    final double GB_TO_BYTES = 1024 * 1024 * 1024;
    final int ACU_RAM = 2; // GB
    long bytesPerThreads;

    if(fileCount == -1)
      fileCount = loadInputs().size();

    if(fileCount == 0)
      return 0;

    int threadCount = calculateDBThreadCount();

    //Only take into account the max parallel execution
    bytesPerThreads = getUncompressedUploadBytesEstimation() / fileCount * threadCount;

    //RDS processing of 9,5GB zipped leads into ~120 GB RDS Mem
    //Calculate the needed ACUs
    double requiredRAMPerThreads = bytesPerThreads / GB_TO_BYTES;
    double neededACUs = requiredRAMPerThreads / ACU_RAM;

    logAndSetPhase(Phase.CALCULATE_ACUS, "expectedMemoryConsumption: " + getUncompressedUploadBytesEstimation()
        + ", bytesPerThreads:"+bytesPerThreads+", requiredRAMPerThreads:"+requiredRAMPerThreads
        + ", neededACUs:"+neededACUs);

    return neededACUs;
  }

  private int calculateDBThreadCount(){
    //1GB for maxThreads
    long uncompressedByteSizeForMaxThreads = 1024L * 1024 * 1024;

    if (getUncompressedUploadBytesEstimation() >= uncompressedByteSizeForMaxThreads)
      calculatedThreadCount = MAX_DB_THREAD_CNT;
    else {
      // Calculate linearly scaled thread count
      int threadCnt = (int) ((double) getUncompressedUploadBytesEstimation() / uncompressedByteSizeForMaxThreads * MAX_DB_THREAD_CNT);
      calculatedThreadCount = threadCnt == 0 ? 1 : threadCnt;
    }

    return calculatedThreadCount > fileCount ? fileCount : calculatedThreadCount;
  }

  private void logAndSetPhase(Phase newPhase, String... messages){
    if (newPhase != null)
      phase = newPhase;
    logger.info("[{}@{}] ON/INTO '{}' {}", getGlobalStepId(), getPhase(), getSpaceId(), messages.length > 0 ? messages : "");
  }

  private String getTemporaryTableName() {
    return JOB_DATA_PREFIX + getId();
  }
}
