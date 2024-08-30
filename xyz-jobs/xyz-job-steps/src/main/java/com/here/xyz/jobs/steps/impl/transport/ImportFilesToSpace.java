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

package com.here.xyz.jobs.steps.impl.transport;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.WRITER;
import static com.here.xyz.jobs.steps.execution.db.Database.loadDatabase;
import static com.here.xyz.util.web.XyzWebClient.WebClientException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.jobs.datasets.space.UpdateStrategy;
import com.here.xyz.jobs.datasets.space.UpdateStrategy.OnNotExists;
import com.here.xyz.jobs.datasets.space.UpdateStrategy.OnExists;

import com.here.xyz.jobs.steps.execution.db.Database;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.impl.tools.ResourceAndTimeCalculator;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.resources.IOResource;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.jobs.util.S3Client;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import io.vertx.core.json.JsonObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.locationtech.jts.io.ParseException;


/**
 * This step imports a set of user provided inputs and imports their data into a specified space.
 * This step produces exactly one output of type {@link FeatureStatistics}.
 */
public class ImportFilesToSpace extends SpaceBasedStep<ImportFilesToSpace> {
  private static final Logger logger = LogManager.getLogger();

  public static final int MAX_DB_THREAD_CNT = 15;

  private static final String JOB_DATA_PREFIX = "job_data_";

  private Format format = Format.GEOJSON;

  private Phase phase;

  @JsonView({Internal.class, Static.class})
  private double overallNeededAcus = -1;

  @JsonView({Internal.class, Static.class})
  private long targetTableFeatureCount = -1;

  @JsonView({Internal.class, Static.class})
  private int fileCount = -1;

  @JsonView({Internal.class, Static.class})
  private int calculatedThreadCount = -1;

  @JsonView({Internal.class, Static.class})
  private int estimatedSeconds = -1;

  @JsonView({Internal.class, Static.class})
  private UpdateStrategy updateStrategy =
          new UpdateStrategy(OnExists.REPLACE, OnNotExists.CREATE , null, null);

  @JsonView({Internal.class, Static.class})
  private EntityPerLine entityPerLine =  null;

  public enum EntityPerLine {
    Feature,
    FeatureCollection
  }

  public enum Format {
    CSV_GEOJSON,
    CSV_JSON_WKB,
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

  public UpdateStrategy getUpdateStrategy() {
    return updateStrategy;
  }

  public void setUpdateStrategy(UpdateStrategy updateStrategy) {
    this.updateStrategy = updateStrategy;
  }

  public ImportFilesToSpace withUpdateStrategy(UpdateStrategy updateStrategy) {
    setUpdateStrategy(updateStrategy);
    return this;
  }

  public void setCalculatedThreadCount(int calculatedThreadCount) {
    this.calculatedThreadCount = calculatedThreadCount;
  }

  public ImportFilesToSpace withCalculatedThreadCount(int calculatedThreadCount) {
      setCalculatedThreadCount(calculatedThreadCount);
      return this;
  }

  public void setEntityPerLine(String entityPerLine) {
    this.entityPerLine = EntityPerLine.valueOf(entityPerLine);
  }

  public ImportFilesToSpace withEntityPerLine(String entityPerLine) {
    setEntityPerLine(entityPerLine);
    return this;
  }

  public Phase getPhase() {
    return phase;
  }

  @Override
  public List<Load> getNeededResources() {
    try {
      fileCount = fileCount != -1 ? fileCount : currentInputsCount(UploadUrl.class);

      calculatedThreadCount = calculatedThreadCount != -1 ? calculatedThreadCount :
              ResourceAndTimeCalculator.getInstance().calculateNeededImportDBThreadCount(getUncompressedUploadBytesEstimation(), fileCount, MAX_DB_THREAD_CNT);
      /** Calculate estimation for ACUs for all parallel running threads */
      overallNeededAcus = overallNeededAcus != -1 ? overallNeededAcus : calculateNeededAcus(calculatedThreadCount);
      return List.of(new Load().withResource(db()).withEstimatedVirtualUnits(overallNeededAcus),
              new Load().withResource(IOResource.getInstance()).withEstimatedVirtualUnits(getUncompressedUploadBytesEstimation()));
    }
    catch (WebClientException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getTimeoutSeconds() {
    //return ResourceAndTimeCalculator.getInstance().calculateImportTimeoutSeconds(getSpaceId(), getUncompressedUploadBytesEstimation(), getExecutionMode());
    return 24 * 3600;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    if (estimatedSeconds == -1 && getSpaceId() != null) {
      estimatedSeconds = ResourceAndTimeCalculator.getInstance()
              .calculateImportTimeInSeconds(getSpaceId(), getUncompressedUploadBytesEstimation(), getExecutionMode());
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
    super.deleteOutputs();

    /** @TODO:
     * Currently we only have non-retryable import jobs. If we introduce some, we need to implement the resource
     * cleanup properly. One possibility could be to add the restriction that steps only could have temporary
     * resources during their execution. To not lose the temporary information, which is relevant for a retry,
     * it would be required to write those into a system output.
     */
  }

  @Override
  public boolean validate() throws ValidationException {
    super.validate();

    long max_byte_for_non_empty = 10 * 1024 * 1024 * 1024l;

    try {
      logAndSetPhase(Phase.VALIDATE);
      //Check if the space is actually existing
      space();

      StatisticsResponse statistics = loadSpaceStatistics(getSpaceId(), EXTENSION);
      targetTableFeatureCount = statistics.getCount().getValue();

      if(entityPerLine.equals(EntityPerLine.FeatureCollection) && format.equals(Format.CSV_JSON_WKB))
        throw new ValidationException("Combination of entityPerLine 'FeatureCollection' and type 'Csv' is not supported!");

      if (targetTableFeatureCount > 0 && getUncompressedUploadBytesEstimation() > max_byte_for_non_empty)
        throw new ValidationException("Provided files have reached the uncompressed limit of " + max_byte_for_non_empty + " bytes. Import in non empty space is not possible.");
    }
    catch (WebClientException e) {
      throw new ValidationException("Error loading resource " + getSpaceId(), e);
    }

    if (currentInputsCount(UploadUrl.class) <= 0)
      //Inputs are missing, the step is not ready to be executed
      return false;

    //Quick-validate the first UploadUrl that is found in the inputs
    ImportFilesQuickValidator.validate(loadInputsSample(1, UploadUrl.class).get(0), format, entityPerLine);

    return true;
  }

    @Override
    public ExecutionMode getExecutionMode() {
        //SYNC mode is only supported for new standard Format.
        //TODO: add support for FeatureCollection
        //TODO: check slow import times in syncmode
        if(!format.equals(Format.GEOJSON))
          return ExecutionMode.ASYNC;
        else if(format.equals(Format.GEOJSON) && entityPerLine != null && entityPerLine.equals(EntityPerLine.FeatureCollection))
          return ExecutionMode.ASYNC;

        //> 100 MB => ASYNC
        long max_sync_bytes = 100 * 1024 * 1024;
        return getUncompressedUploadBytesEstimation() > max_sync_bytes ? ExecutionMode.ASYNC : ExecutionMode.SYNC;
    }

  @Override
  public void execute() throws WebClientException, SQLException, TooManyResourcesClaimed, IOException, ParseException, InterruptedException {
    _execute(false);
  }

  private void _execute(boolean isResume) throws WebClientException, SQLException, TooManyResourcesClaimed, IOException {
      if(getExecutionMode().equals(ExecutionMode.SYNC)) {
          //@TODO: For future add threading (if validation step is in place)
          //TODO: Implement synchronous feature writing using new FeatureWriter
          syncExecution();
      } else {
        log("Importing input files for job " + getJobId() + " into space " + getSpaceId() + " ...");

          //TODO: Move resume logic into #resume()
          if (!isResume) {
              logAndSetPhase(Phase.SET_READONLY);
              hubWebClient().patchSpace(getSpaceId(), Map.of("readOnly", true));

              logAndSetPhase(Phase.RETRIEVE_NEW_VERSION);
              long newVersion = increaseVersionSequence();

              logAndSetPhase(Phase.CREATE_TRIGGER);
              runWriteQuerySync(buildCreateImportTrigger(space.getOwner(), newVersion), db(), 0);
          }

          createAndFillTemporaryTable();

          calculatedThreadCount = calculatedThreadCount != -1 ? calculatedThreadCount :
                  ResourceAndTimeCalculator.getInstance().calculateNeededImportDBThreadCount(getUncompressedUploadBytesEstimation(), fileCount, MAX_DB_THREAD_CNT);
          double neededAcusForOneThread = calculateNeededAcus(1);

          logAndSetPhase(Phase.EXECUTE_IMPORT);

          for (int i = 1; i <= calculatedThreadCount; i++) {
              logAndSetPhase(Phase.EXECUTE_IMPORT, "Start Import Thread number " + i);
              runReadQueryAsync(buildImportQueryBlock(), db(), neededAcusForOneThread, false);
          }
      }
  }

  private void syncExecution() throws WebClientException, SQLException, TooManyResourcesClaimed, IOException {
    logAndSetPhase(Phase.RETRIEVE_NEW_VERSION);
    long targetVersion = increaseVersionSequence();

    for (Input input : loadInputs()) {
      logger.info("[{}] Sync write from {} to {}", getGlobalStepId(), input.getS3Key(), getSpaceId());
      syncWriteFileToSpace(input.getS3Key(), input.isCompressed(), targetVersion);
    }
  }

  protected void syncWriteFileToSpace(String s3Path, boolean isGzipped, long targetVersion) throws IOException, WebClientException, SQLException, TooManyResourcesClaimed {
    final S3Client s3Client = S3Client.getInstance();

    InputStream decompressedStream = isGzipped ? new GZIPInputStream(s3Client.streamObjectContent(s3Path))
            : s3Client.streamObjectContent(s3Path);

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(decompressedStream, StandardCharsets.UTF_8))) {
      StringBuilder fileContent = new StringBuilder();
      fileContent.append("[");
      String line;
      while ((line = reader.readLine()) != null) {
        fileContent.append(line).append(",");
      }
      //cut comma if file was empty
      if(fileContent.length() > 1) {
        fileContent.setLength(fileContent.length() - 1);
      }
      fileContent.append("]");

      runBatchWriteQuerySync(SQLQuery.batchOf(buildFeatureWriterQuery(fileContent.toString(), targetVersion)), db(), 0);
    }finally {
      decompressedStream.close();
    }
  }

  private long increaseVersionSequence() throws SQLException, TooManyResourcesClaimed, WebClientException {
    return runReadQuerySync(buildVersionSequenceIncrement(), db(), 0, rs -> {
      rs.next();
      return rs.getLong(1);
    });
  }

  @JsonIgnore
  private Space space;
  private Space space() throws WebClientException {
    if (space == null) {
      log("Loading space config for space " + getSpaceId());
      space = loadSpace(getSpaceId());
    }
    return space;
  }

  @JsonIgnore
  private Space superSpace;
  private Space superSpace() throws WebClientException {
    if (superSpace == null) {
      log("Loading space config for super-space " + getSpaceId());
      if (space().getExtension() == null)
        throw new IllegalStateException("The space does not extend some other space. Could not load the super space.");
      superSpace = loadSpace(space().getExtension().getSpaceId());
    }
    return superSpace;
  }

  @JsonIgnore
  private Database db;
  private Database db() throws WebClientException {
    if (db == null) {
      log("Loading storage database for space " + getSpaceId());
      db = loadDatabase(space().getStorage().getId(), WRITER);
    }
    return db;
  }

    private void createAndFillTemporaryTable() throws SQLException, TooManyResourcesClaimed, WebClientException {
        boolean tmpTableNotExistsAndHasNoData = true;
    try {
      //Check if the temporary table exists and has data - if yes we assume a retry and skip the creation + filling.
      tmpTableNotExistsAndHasNoData = runReadQuerySync(buildTableCheckQuery(getSchema(db())), db(), 0,
              rs -> {
                rs.next();
                if(rs.getLong("count") == 0 )
                  return true;
                return false;
              });

    }catch (SQLException e){
      //We expect that
      if (e.getSQLState() != null && !e.getSQLState().equals("42P01"))
        throw e;
      //TODO: Should we really ignore all other SQLExceptions?
    }

    if(!tmpTableNotExistsAndHasNoData) {
      logAndSetPhase(Phase.RESET_SUCCESS_MARKER);
      runWriteQuerySync(resetSuccessMarkerAndRunningOnes(getSchema(db)), db, 0);
    }else {
      logAndSetPhase(Phase.CREATE_TMP_TABLE);
      runWriteQuerySync(buildTemporaryTableForImportQuery(getSchema(db)), db, 0);

      logAndSetPhase(Phase.FILL_TMP_TABLE);
      fillTemporaryTableWithInputs(db, loadInputs(), bucketRegion());
    }
  }

  @Override
  protected void onStateCheck() {
    try {
      runReadQuerySync(buildProgressQuery(getSchema(db())), db(), 0,
               rs -> {
                rs.next();

                float progress = rs.getFloat("progress");
                long processedBytes = rs.getLong("processed_bytes");
                int finishedCnt = rs.getInt("finished_cnt");
                int failedCnt = rs.getInt("failed_cnt");

                getStatus().setEstimatedProgress(progress);

                 log( "Progress["+progress+"] => "
                        +" processedBytes:"+processedBytes+" ,finishedCnt:"+finishedCnt+" ,failedCnt:"+failedCnt);
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
    try {

    logAndSetPhase(Phase.RETRIEVE_STATISTICS);
    FeatureStatistics statistics = runReadQuerySync(buildStatisticDataOfTemporaryTableQuery(), db(),
            0, rs -> rs.next()
                    ? new FeatureStatistics().withFeatureCount(rs.getLong("imported_rows")).withByteSize(rs.getLong("imported_bytes"))
                    : new FeatureStatistics());

    log("Statistics: bytes="+statistics.getByteSize()+" rows="+ statistics.getFeatureCount());
    registerOutputs(List.of(statistics), true);

    logAndSetPhase(Phase.WRITE_STATISTICS);
    registerOutputs(new ArrayList<>(){{ add(statistics);}}, true);

    cleanUpDbRelatedResources();

    logAndSetPhase(Phase.RELEASE_READONLY);
    hubWebClient().patchSpace(getSpaceId(), Map.of("readOnly", false));

    }catch (SQLException e){
      //relation "*_job_data" does not exist - can happen when we have received twice a SUCCESS_CALLBACK
      if(e.getSQLState() != null && e.getSQLState().equals("42P01")) {
        log("_job_data table got already deleted!");
        return;
      }
      throw e;
    }
  }

  private void cleanUpDbRelatedResources() throws TooManyResourcesClaimed, SQLException, WebClientException {
      logAndSetPhase(Phase.DROP_TRIGGER);
      runWriteQuerySync(buildDropImportTrigger(), db(), 0);

      logAndSetPhase(Phase.DROP_TMP_TABLE);
      runWriteQuerySync(buildDropTemporaryTableForImportQuery(), db(), 0);
  }

  @Override
  protected boolean onAsyncFailure() {
    try {
      //TODO: Inspect the error provided in the status and decide whether it is retryable (return-value)
      boolean isRetryable = false;

      if(!isRetryable)
        cleanUpDbRelatedResources();

      return isRetryable;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void resume() throws Exception {
    _execute(true);
  }

  private SQLQuery buildTemporaryTableForImportQuery(String schema) {
    return new SQLQuery("""
                    CREATE TABLE IF NOT EXISTS ${schema}.${table}
                           (
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
            .withVariable("table", TransportTools.getTemporaryTableName(this))
            .withVariable("schema", schema)
            .withVariable("primaryKey", TransportTools.getTemporaryTableName(this) + "_primKey");
  }

  private void fillTemporaryTableWithInputs(Database db, List<Input> inputs, String bucketRegion) throws SQLException, TooManyResourcesClaimed {
    List<SQLQuery> queryList = new ArrayList<>();
    for (Input input : inputs) {
      if (input instanceof UploadUrl uploadUrl) {
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
                        .withVariable("table", TransportTools.getTemporaryTableName(this))
                        .withNamedParameter("s3Key", input.getS3Key())
                        .withNamedParameter("bucketName", input.getS3Bucket())
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
                    .withVariable("table", TransportTools.getTemporaryTableName(this))
                    .withNamedParameter("s3Key", "SUCCESS_MARKER")
                    .withNamedParameter("bucketName", "SUCCESS_MARKER")
                    .withNamedParameter("state", "SUCCESS_MARKER")
                    .withNamedParameter("bucketRegion", "SUCCESS_MARKER")
                    .withNamedParameter("data", "{}"));
    runBatchWriteQuerySync(SQLQuery.batchOf(queryList), db, 0);
  }

  private SQLQuery buildDropTemporaryTableForImportQuery() throws WebClientException {
    return new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table};")
            .withVariable("table", TransportTools.getTemporaryTableName(this))
            .withVariable("schema", getSchema(db()));
  }

  private SQLQuery buildCreateImportTrigger(String targetAuthor, long targetSpaceVersion) throws WebClientException {
    if(targetTableFeatureCount == 0)
      return buildCreateImportTriggerForEmptyLayer(targetAuthor, targetSpaceVersion);
    return buildCreateImportTriggerForNonEmptyLayer(targetAuthor, targetSpaceVersion);
  }

  private SQLQuery buildCreateImportTriggerForEmptyLayer(String targetAuthor, long targetSpaceVersion) throws WebClientException {
    return new SQLQuery("CREATE OR REPLACE TRIGGER insertTrigger BEFORE INSERT ON ${schema}.${table} "
            + "FOR EACH ROW EXECUTE PROCEDURE ${schema}.xyz_import_trigger_for_empty_layer('${{author}}', ${{spaceVersion}});")
            .withQueryFragment("spaceVersion", "" + targetSpaceVersion)
            .withQueryFragment("author", targetAuthor)
            .withVariable("schema", getSchema(db()))
            .withVariable("table", getRootTableName(space()));
  }

  private SQLQuery buildCreateImportTriggerForNonEmptyLayer(String targetAuthor, long targetSpaceVersion) throws WebClientException {
    String superTable = space().getExtension() != null ? getRootTableName(superSpace()) : null;
    //TODO: Check if we can forward the whole transaction to the FeatureWriter rather than doing it for each row
    return new SQLQuery("""
        CREATE OR REPLACE TRIGGER insertTrigger BEFORE INSERT ON ${schema}.${table} 
          FOR EACH ROW EXECUTE PROCEDURE ${schema}.xyz_import_trigger_for_non_empty_layer(
             ${{author}},
             ${{spaceVersion}},
             false, --isPartial
             ${{onExists}},
             ${{onNotExists}},
             ${{onVersionConflict}},
             ${{onMergeConflict}},
             ${{historyEnabled}},
             ${{context}},
             ${{extendedTable}}
             )
        """)
        .withQueryFragment("spaceVersion", Long.toString(targetSpaceVersion))
        .withQueryFragment("author", "'" + targetAuthor + "'" )
        .withQueryFragment("onExists", updateStrategy.onExists() == null ? "NULL" : "'" + updateStrategy.onExists() + "'" )
        .withQueryFragment("onNotExists", updateStrategy.onNotExists() == null ? "NULL" : "'" + updateStrategy.onNotExists() + "'" )
        .withQueryFragment("onVersionConflict", updateStrategy.onVersionConflict() == null ? "NULL" : "'" + updateStrategy.onVersionConflict() + "'" )
        .withQueryFragment("onMergeConflict", updateStrategy.onMergeConflict() == null ? "NULL" : "'" + updateStrategy.onMergeConflict() + "'" )
        .withQueryFragment("historyEnabled", "" + (space().getVersionsToKeep() > 1))
        .withQueryFragment("context", superTable != null ? "'DEFAULT'" : "NULL")
        .withQueryFragment("extendedTable", superTable != null ? "'" + superTable + "'" : "NULL")
        .withVariable("schema", getSchema(db()))
        .withVariable("table", getRootTableName(space()));
  }

  private SQLQuery buildContextQuery() throws WebClientException {
    String table = getRootTableName(space());

    JSONObject context = new JSONObject()
            .put("schema", getSchema(db()))
            .put("table", table)
            .put("historyEnabled", space().getVersionsToKeep() > 0);

    if (space().getExtension() != null) {
      context.put("extendedTable", getRootTableName(superSpace()));
      context.put("context", DEFAULT);
    }

    return new SQLQuery("""
              DO
              $context$
              BEGIN
                PERFORM context(#{context}::JSONB);
              END
              $context$;
            """)
            .withNamedParameter("context", context.toString());
  }

  private SQLQuery buildDropImportTrigger() throws WebClientException {
    return new SQLQuery("DROP TRIGGER IF EXISTS insertTrigger ON ${schema}.${table};")
            .withVariable("table", getRootTableName(space()))
            .withVariable("schema", getSchema(db()));
  }

  //TODO: Move to XyzSpaceTableHelper or so (it's the nth time we have that implemented somewhere)
  private SQLQuery buildVersionSequenceIncrement() throws WebClientException {
    return new SQLQuery("SELECT nextval('${schema}.${sequence}')")
            .withVariable("schema", getSchema(db()))
            .withVariable("sequence", getRootTableName(space()) + "_version_seq");
  }

  private SQLQuery buildStatisticDataOfTemporaryTableQuery() throws WebClientException {
    return new SQLQuery("""
            SELECT sum((data->'filesize')::bigint) as imported_bytes,
            	count(1) as imported_files,
            	sum(SUBSTRING((data->'import_statistics'->>'table_import_from_s3'),0,POSITION('rows' in data->'import_statistics'->>'table_import_from_s3'))::bigint) as imported_rows
            	FROM ${schema}.${table}
            WHERE POSITION('SUCCESS_MARKER' in state) = 0;
          """)
            .withVariable("schema", getSchema(db()))
            .withVariable("table", TransportTools.getTemporaryTableName(this));
  }

  private SQLQuery buildProgressQuery(String schema) {
    return new SQLQuery("""
            SELECT 
            	COALESCE(processed_bytes/overall_bytes, 0) as progress,
            	COALESCE(processed_bytes,0) as processed_bytes,
              	COALESCE(finished_cnt,0) as finished_cnt,
              	COALESCE(failed_cnt,0) as failed_cnt
              FROM(
              	SELECT
                    (SELECT sum((data->'filesize')::bigint ) FROM ${schema}.${table}) as overall_bytes,
                    sum((data->'filesize')::bigint ) as processed_bytes,
                    sum((state = 'FINISHED')::int) as finished_cnt,
                    sum((state = 'FAILED')::int) as failed_cnt
                  FROM ${schema}.${table}
                	 WHERE POSITION('SUCCESS_MARKER' in state) = 0
              	   AND state IN ('FINISHED','FAILED')
              )A          
          """)
            .withVariable("schema", schema)
            .withVariable("table", TransportTools.getTemporaryTableName(this));
  }

  private SQLQuery buildImportQuery() throws WebClientException {
    String schema = getSchema(db());
    SQLQuery successQuery = buildSuccessCallbackQuery();
    SQLQuery failureQuery = buildFailureCallbackQuery();
    return new SQLQuery("CALL xyz_import_start(#{schema}, #{temporary_tbl}::regclass, #{target_tbl}::regclass, #{format}, '${{successQuery}}', '${{failureQuery}}');")
        .withAsyncProcedure(true)
        .withNamedParameter("schema", schema)
        .withNamedParameter("target_tbl", schema+".\""+getRootTableName(space())+"\"")
        .withNamedParameter("temporary_tbl",  schema+".\""+(TransportTools.getTemporaryTableName(this))+"\"")
        .withNamedParameter("format", format.toString())
        .withQueryFragment("successQuery", successQuery.substitute().text().replaceAll("'","''"))
        .withQueryFragment("failureQuery", failureQuery.substitute().text().replaceAll("'","''"));
  }

  private SQLQuery buildImportQueryBlock() throws WebClientException {
    /**
     * TODO:
     * The idea was to uses context with asyncify. The integration in "_create_asyncify_query_block" (same
     * principal as with xzy.password) has not worked. If we find a solution with asyncify we can use the block
     * query - if not, we can simply use buildImportQuery()
    */
    return new SQLQuery("${{contextQuery}} ${{importQuery}}")
            .withAsyncProcedure(true)
            .withQueryFragment("contextQuery", buildContextQuery()) //TODO: Set the query context at the import query object
            .withQueryFragment("importQuery", buildImportQuery());
  }

  private SQLQuery buildTableCheckQuery(String schema) {
    return new SQLQuery("SELECT count(1) FROM ${schema}.${table};")
            .withVariable("schema", schema)
            .withVariable("table", TransportTools.getTemporaryTableName(this));
  }

  private SQLQuery buildFeatureWriterQuery(String featureList, long targetVersion)
          throws WebClientException {

    String superTable = space().getExtension() != null ? getRootTableName(superSpace()) : null;

    final Map<String, Object> queryContext = new HashMap<>(Map.of(
            "schema",  getSchema(db()),
            "table", getRootTableName(space()),
            "context", superTable != null ? "'DEFAULT'" : "NULL",
            "historyEnabled", (space().getVersionsToKeep() > 1)
    ));

    if (superTable != null)
      queryContext.put("extendedTable", superTable);

    SQLQuery writeFeaturesQuery = new SQLQuery("SELECT write_features(#{featureList}::TEXT, #{author}::TEXT, #{onExists}::TEXT,"
            + "#{onNotExists}::TEXT, #{onVersionConflict}::TEXT, #{onMergeConflict}::TEXT, #{isPartial}::BOOLEAN, #{version}::BIGINT);")
            .withNamedParameter("featureList", featureList)
            .withNamedParameter("author", space.getOwner())
            .withNamedParameter("onExists", updateStrategy == null || updateStrategy.onExists() == null ? null : updateStrategy.onExists().toString())
            .withNamedParameter("onNotExists",updateStrategy == null || updateStrategy.onNotExists() == null? null : updateStrategy.onNotExists().toString())
            .withNamedParameter("onVersionConflict", updateStrategy == null || updateStrategy.onVersionConflict() == null ? null : updateStrategy.onVersionConflict().toString())
            .withNamedParameter("onMergeConflict", updateStrategy == null || updateStrategy.onMergeConflict() == null ? null :  updateStrategy.onMergeConflict().toString())
            .withNamedParameter("isPartial", false)
            .withNamedParameter("version", targetVersion)
            .withContext(queryContext);

    return writeFeaturesQuery;
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
            .withVariable("table", TransportTools.getTemporaryTableName(this));
  }

  private double calculateNeededAcus(int threadCount){
    double neededACUs;

    if (fileCount == -1)
      fileCount = currentInputsCount(UploadUrl.class);

    if (fileCount == 0)
      return 0;

    neededACUs = ResourceAndTimeCalculator.getInstance().calculateNeededImportAcus(
            getUncompressedUploadBytesEstimation(), fileCount, threadCount);

    logAndSetPhase(Phase.CALCULATE_ACUS, "expectedMemoryConsumption: " + getUncompressedUploadBytesEstimation() +" => neededACUs:" + neededACUs);
    return neededACUs;
  }

  private void log(String ... messages) {
    logAndSetPhase(null, messages);
  }

  private void logAndSetPhase(Phase newPhase, String... messages) {
    if (newPhase != null)
      phase = newPhase;
    logger.info("[{}@{}] ON/INTO '{}' {}", getGlobalStepId(), getPhase(), getSpaceId(), messages.length > 0 ? messages : "");
  }
}
