/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.events.UpdateStrategy.DEFAULT_UPDATE_STRATEGY;
import static com.here.xyz.jobs.steps.Step.Visibility.USER;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.ExecutionMode.ASYNC;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.ExecutionMode.SYNC;
import static com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.EntityPerLine.Feature;
import static com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.EntityPerLine.FeatureCollection;
import static com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.Format.CSV_JSON_WKB;
import static com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.Format.GEOJSON;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.JOB_EXECUTOR;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.JOB_VALIDATE;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_EXECUTE;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_ON_ASYNC_SUCCESS;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_ON_STATE_CHECK;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.buildResetJobTableItemsForResumeStatement;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.buildTemporaryJobTableCreateStatement;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.buildTemporaryJobTableDropStatement;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.buildTemporaryJobTableInsertStatements;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.createQueryContext;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.errorLog;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.getTemporaryJobTableName;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.getTemporaryTriggerTableName;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.infoLog;
import static com.here.xyz.util.web.XyzWebClient.WebClientException;

import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.jobs.steps.S3DataFile;
import com.here.xyz.jobs.steps.execution.StepException;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.impl.tools.ResourceAndTimeCalculator;
import com.here.xyz.jobs.steps.impl.transport.tools.ImportFilesQuickValidator;
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
import com.here.xyz.util.service.Core;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;
import org.locationtech.jts.io.ParseException;


/**
 * This step imports a set of user provided inputs and imports their data into a specified space. This step produces exactly one output of
 * type {@link FeatureStatistics}.
 */
public class ImportFilesToSpace extends SpaceBasedStep<ImportFilesToSpace> {

  private static final long MAX_INPUT_BYTES_FOR_NON_EMPTY_IMPORT = 10l * 1024 * 1024 * 1024;
  private static final long MAX_INPUT_BYTES_FOR_SYNC_IMPORT = 100l * 1024 * 1024;
  private static final long MAX_INPUT_BYTES_FOR_KEEP_INDICES = 1l * 1024 * 1024 * 1024;
  private static final int MIN_FEATURE_COUNT_IN_TARGET_TABLE_FOR_KEEP_INDICES = 5_000_000;
  private static final int MAX_DB_THREAD_COUNT = 15;
  public static final String STATISTICS = "statistics";

  private Format format = GEOJSON;

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
  private UpdateStrategy updateStrategy = DEFAULT_UPDATE_STRATEGY;

  @JsonView({Internal.class, Static.class})
  private EntityPerLine entityPerLine = Feature;

  @JsonView({Internal.class, Static.class})
  private boolean retainMetadata = false;

  {
    setOutputSets(List.of(new OutputSet(STATISTICS, USER, true)));
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

  public ImportFilesToSpace withCalculatedThreadCount(int calculatedThreadCount) {
    setCalculatedThreadCount(calculatedThreadCount);
    return this;
  }

  public void setCalculatedThreadCount(int calculatedThreadCount) {
    this.calculatedThreadCount = calculatedThreadCount;
  }

  public EntityPerLine getEntityPerLine() {
    return entityPerLine;
  }

  public void setEntityPerLine(EntityPerLine entityPerLine) {
    this.entityPerLine = entityPerLine;
  }

  public ImportFilesToSpace withEntityPerLine(EntityPerLine entityPerLine) {
    setEntityPerLine(entityPerLine);
    return this;
  }

  public boolean isRetainMetadata() {
    return retainMetadata;
  }

  public void setRetainMetadata(boolean retainMetadata) {
    this.retainMetadata = retainMetadata;
  }

  public ImportFilesToSpace withRetainMetadata(boolean retainMetadata) {
    setRetainMetadata(retainMetadata);
    return this;
  }

  public boolean keepIndices() {
    /*
     * The targetSpace needs to have more than MIN_FEATURE_COUNT_IN_TARGET_TABLE_FOR_KEEP_INDICES features
     * Reason: For tables with not that many records in its always faster to remove and recreate indices
     * +
     * Incoming bytes have to be smaller as MAX_INPUT_BYTES_FOR_KEEP_INDICES
     * Reason: if we write not that much, it's also with indices fast enough
     */
    return loadTargetSpaceFeatureCount() >= MIN_FEATURE_COUNT_IN_TARGET_TABLE_FOR_KEEP_INDICES
        && getUncompressedUploadBytesEstimation() <= MAX_INPUT_BYTES_FOR_KEEP_INDICES;
  }

  private long loadTargetSpaceFeatureCount() {
    if (targetTableFeatureCount == -1 && getSpaceId() != null) {
      StatisticsResponse statistics;
      try {
        statistics = loadSpaceStatistics(getSpaceId(), EXTENSION, true);
        targetTableFeatureCount = statistics.getCount().getValue();
      }
      catch (WebClientException e) {
        throw new RuntimeException(e);
      }
    }
    return targetTableFeatureCount;
  }

  @Override
  public List<Load> getNeededResources() {
    try {
      fileCount = fileCount != -1 ? fileCount : currentInputsCount(UploadUrl.class);

      calculatedThreadCount = calculatedThreadCount != -1 ? calculatedThreadCount :
          ResourceAndTimeCalculator.getInstance().calculateNeededImportDBThreadCount(getUncompressedUploadBytesEstimation(), fileCount,
              MAX_DB_THREAD_COUNT);

      //Calculate estimation for ACUs for all parallel running threads
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
    return 3 * 24 * 3600;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    if (estimatedSeconds == -1 && getSpaceId() != null) {
      estimatedSeconds = ResourceAndTimeCalculator.getInstance()
          .calculateImportTimeInSeconds(getSpaceId(), getUncompressedUploadBytesEstimation(), getExecutionMode());
      infoLog(JOB_EXECUTOR, this, "Calculated estimatedSeconds: "+estimatedSeconds );
    }
    return estimatedSeconds;
  }

  //TODO: Cache the execution-mode once it was calculated
  @Override
  public ExecutionMode getExecutionMode() {
    //CSV is not supported in SYNC mode
//    if (format == CSV_JSON_WKB || format == CSV_GEOJSON)
//      return ASYNC;
//    return getUncompressedUploadBytesEstimation() > MAX_INPUT_BYTES_FOR_SYNC_IMPORT ? ASYNC : SYNC;
    //TODO: Fix ConnectionPool issue caused from threading in syncExecution()
    return ASYNC;
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
    try {
      infoLog(JOB_VALIDATE, this);
      //Check if the space is actually existing
      Space space = space();
      if (!space.isActive())
        throw new ValidationException("Data can not be written to target " + space.getId() + " as it is inactive.");

      if (space.isReadOnly())
        throw new ValidationException("Data can not be written to target " + space.getId() + " as it is in read-only mode.");

      if (entityPerLine == FeatureCollection && format == CSV_JSON_WKB)
        throw new ValidationException("Combination of entityPerLine 'FeatureCollection' and type 'Csv' is not supported!");

      if (loadTargetSpaceFeatureCount() > 0 && getUncompressedUploadBytesEstimation() > MAX_INPUT_BYTES_FOR_NON_EMPTY_IMPORT)
        throw new ValidationException("An import into a non empty space is not possible. "
            + "The uncompressed size of the provided files exceeds the limit of " + MAX_INPUT_BYTES_FOR_NON_EMPTY_IMPORT + " bytes.");
    }
    catch (WebClientException e) {
      throw new ValidationException("Error loading resource " + getSpaceId(), e);
    }

    if (isUserInputsExpected()) {
      if (!isUserInputsPresent(UploadUrl.class))
        return false;
      //Quick-validate the first UploadUrl that is found in the inputs
      ImportFilesQuickValidator.validate(loadInputsSample(1, UploadUrl.class).get(0), format, entityPerLine);
    }

    return true;
  }

  @Override
  public void execute(boolean resume) throws WebClientException, SQLException, TooManyResourcesClaimed, IOException, ParseException,
      InterruptedException {
    if (getExecutionMode() == SYNC)
      syncExecution();
    else {
      if (!resume) {
        infoLog(STEP_EXECUTE, this,"Retrieve new version");
        long newVersion = increaseVersionSequence();

        infoLog(STEP_EXECUTE, this,"Create TriggerTable and Trigger");
        //Create Temp-ImportTable to avoid deserialization of JSON and fix missing row count
        runBatchWriteQuerySync(buildTemporaryTriggerTableBlock(space().getOwner(), newVersion), db(), 0);
      }

      if(!createAndFillTemporaryJobTable()) {
        infoLog(STEP_EXECUTE, this,"No files available - nothing to do!");
        //Report Success with a new invocation.
        runReadQueryAsync(buildSuccessReportQuery(), db(), 0, true);
        //no Files to process simply return successfully!
        return;
      }

      calculatedThreadCount = calculatedThreadCount != -1 ? calculatedThreadCount :
          ResourceAndTimeCalculator.getInstance().calculateNeededImportDBThreadCount(getUncompressedUploadBytesEstimation(), fileCount,
              MAX_DB_THREAD_COUNT);

      double neededAcusForOneThread = calculateNeededAcus(1);

      for (int i = 1; i <= calculatedThreadCount; i++) {
        infoLog(STEP_EXECUTE, this,"Start Import Thread number " + i);
        runReadQueryAsync(buildImportQuery(), db(), neededAcusForOneThread, false);
      }
    }
  }

  private void syncExecution() throws WebClientException, SQLException, TooManyResourcesClaimed, IOException {
    //TODO: Support resume
    infoLog(STEP_EXECUTE, this,"Retrieve new version");
    long newVersion = increaseVersionSequence();

    ExecutorService exec = Executors.newFixedThreadPool(5);
    List<Future<FeatureStatistics>> resultFutures = new ArrayList<>();

    //Execute the sync for each import file in parallel
    for (Input input : loadInputs(UploadUrl.class))
      resultFutures.add(exec.submit(() -> new FeatureStatistics()
          .withFeatureCount(syncWriteFileToSpace((UploadUrl) input, newVersion))
          .withByteSize(input.getByteSize())));

    //TODO: Use CompletableFuture.allOf() instead of the following
    //Wait for the futures and aggregate the result statistics into one FeatureStatistics object
    FeatureStatistics resultOutput = new FeatureStatistics();
    for (Future<FeatureStatistics> future : resultFutures) {
      try {
        resultOutput
            .withFeatureCount(resultOutput.getFeatureCount() + future.get().getFeatureCount())
            .withByteSize(resultOutput.getByteSize() + future.get().getByteSize());
      }
      catch (InterruptedException | ExecutionException e) {
        throw new StepException("Error during sync write to target space", e);
      }
    }

    exec.shutdown();

    registerOutputs(List.of(resultOutput), STATISTICS);
    infoLog(STEP_EXECUTE, this,"Set contentUpdatedAt on target space");
    hubWebClient().patchSpace(getSpaceId(), Map.of("contentUpdatedAt", Core.currentTimeMillis()));
  }

  /**
   * Writes one input file into the target space.
   *
   * @param input The input file
   * @param newVersion The new space version being created by this import
   * @return The number of features that have been written
   */
  private int syncWriteFileToSpace(UploadUrl input, long newVersion) throws IOException, WebClientException, SQLException,
      TooManyResourcesClaimed {
    infoLog(STEP_EXECUTE, this,"Start sync write of file " + input.getS3Key() + " ...");
    final S3Client s3Client = S3Client.getInstance(input.getS3Bucket());

    InputStream inputStream = s3Client.streamObjectContent(input.getS3Key());
    if (input.isCompressed())
      inputStream = new GZIPInputStream(inputStream);

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      StringBuilder fileContent = new StringBuilder();
      fileContent.append("[");
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.replace("\\","\\\\");
        fileContent.append(line).append(",");
      }
      //cut comma if file was empty
      if (fileContent.length() > 1) {
        fileContent.setLength(fileContent.length() - 1);
      }
      fileContent.append("]");

      int writtenFeatureCount = runReadQuerySync(buildFeatureWriterQuery(fileContent.toString(), newVersion), db(),  0, rs -> {
        rs.next();
        return rs.getInt("count");
      });

      infoLog(STEP_EXECUTE, this,"Completed sync write of file " + input.getS3Key() + ". Written features: "
          + writtenFeatureCount + ", input bytes: " + input.getByteSize());

      return writtenFeatureCount;
    }
  }

  private long increaseVersionSequence() throws SQLException, TooManyResourcesClaimed, WebClientException {
    return runReadQuerySync(buildVersionSequenceIncrement(), db(), 0, rs -> {
      rs.next();
      return rs.getLong(1);
    });
  }

  private boolean createAndFillTemporaryJobTable() throws SQLException, TooManyResourcesClaimed, WebClientException {
    if (isResume()) {
      infoLog(STEP_EXECUTE, this,"Reset SuccessMarker");
      runWriteQuerySync(buildResetJobTableItemsForResumeStatement(getSchema(db()) ,this), db(), 0);
    }
    else {
      infoLog(STEP_EXECUTE, this,"Create temporary job table");
      runWriteQuerySync(buildTemporaryJobTableCreateStatement(getSchema(db()), this), db(), 0);

      infoLog(STEP_EXECUTE, this,"Fill temporary job table");

      List<?> inputs = loadInputs(UploadUrl.class);
      runBatchWriteQuerySync(SQLQuery.batchOf(buildTemporaryJobTableInsertStatements(getSchema(db()),
          (List<S3DataFile>)inputs, bucketRegion(),this)), db(), 0 );

      //If no Inputs are present return 0
      return inputs.size() != 0;
    }
    return true;
  }

  @Override
  protected void onStateCheck() {
    try {
      runReadQuerySync(buildProgressQuery(getSchema(db()), this), db(), 0,
          rs -> {
            rs.next();

            float progress = rs.getFloat("progress");
            long processedBytes = rs.getLong("processed_bytes");
            int finishedCnt = rs.getInt("finished_cnt");
            int failedCnt = rs.getInt("failed_cnt");

            getStatus().setEstimatedProgress(progress);

            infoLog(STEP_ON_STATE_CHECK,this,"Progress[" + progress + "] => " + " processedBytes:"
                    + processedBytes + " ,finishedCnt:" + finishedCnt + " ,failedCnt:" + failedCnt);
            return progress;
          });
    }
    catch (Exception e) {
      //TODO: What to do? Only log? Report Status is not that important. Further Ignore "table does not exists error" - report 0 in this case.
      errorLog(STEP_ON_STATE_CHECK, this, e);
    }
  }

  @Override
  protected void onAsyncSuccess() throws WebClientException,
      SQLException, TooManyResourcesClaimed, IOException {
    try {
      FeatureStatistics statistics = runReadQuerySync(buildStatisticDataOfTemporaryTableQuery(), db(),
          0, rs -> rs.next()
              ? new FeatureStatistics().withFeatureCount(rs.getLong("imported_rows")).withByteSize(rs.getLong("imported_bytes"))
              : new FeatureStatistics());

      infoLog(STEP_ON_ASYNC_SUCCESS, this,"Job Statistics: bytes=" + statistics.getByteSize() + " rows=" + statistics.getFeatureCount());
      registerOutputs(List.of(statistics), STATISTICS);

      cleanUpDbRelatedResources();
    }
    catch (SQLException e) {
      //relation "*_job_data" does not exist - can happen when we have received twice a SUCCESS_CALLBACK
      //TODO: Find out the cases in which that could happen and prevent it from happening
      if (e.getSQLState() != null && e.getSQLState().equals("42P01")) {
        errorLog(STEP_ON_ASYNC_SUCCESS, this, e, "_job_data table got already deleted!");
        return;
      }
      throw e;
    }
  }

  private void cleanUpDbRelatedResources() throws TooManyResourcesClaimed, SQLException, WebClientException {
    infoLog(STEP_ON_ASYNC_SUCCESS, this, "Clean up database resources");
    runBatchWriteQuerySync(SQLQuery.batchOf(
            buildTemporaryJobTableDropStatement(getSchema(db()), getTemporaryJobTableName(getId())),
            buildTemporaryJobTableDropStatement(getSchema(db()), getTemporaryTriggerTableName(getId()))
    ), db(), 0);
  }

  @Override
  protected boolean onAsyncFailure() {
    try {
      //TODO: Inspect the error provided in the status and decide whether it is retryable (return-value)
      boolean isRetryable = false;

      if (!isRetryable)
        cleanUpDbRelatedResources();

      return isRetryable;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private SQLQuery buildTemporaryTriggerTableForImportQuery() throws WebClientException {
    String tableFields =
        "jsondata TEXT, "
            + "geo geometry(GeometryZ, 4326), "
            + "count INT ";
    return new SQLQuery("CREATE TABLE IF NOT EXISTS ${schema}.${table} (${{tableFields}} )")
        .withQueryFragment("tableFields", tableFields)
        .withVariable("schema", getSchema(db()))
        .withVariable("table", TransportTools.getTemporaryTriggerTableName(getId()));
  }

  private SQLQuery buildCreateImportTrigger(String targetAuthor, long newVersion) throws WebClientException {
    if (loadTargetSpaceFeatureCount() <= 0)
      return buildCreateImportTriggerForEmptyLayer(targetAuthor, newVersion);
    return buildCreateImportTriggerForNonEmptyLayer(targetAuthor, newVersion);
  }

  private SQLQuery buildTemporaryTriggerTableBlock(String targetAuthor, long newVersion) throws WebClientException {
    return SQLQuery.batchOf(
        buildTemporaryTriggerTableForImportQuery(),
        buildCreateImportTrigger(targetAuthor, newVersion)
    );
  }

  private SQLQuery buildCreateImportTriggerForEmptyLayer(String targetAuthor, long targetSpaceVersion) throws WebClientException {
    String triggerFunction = "import_from_s3_trigger_for_empty_layer";
    triggerFunction += entityPerLine == FeatureCollection ? "_geojsonfc" : "";

    return new SQLQuery("CREATE OR REPLACE TRIGGER insertTrigger BEFORE INSERT ON ${schema}.${table} "
        + "FOR EACH ROW EXECUTE PROCEDURE ${triggerFunction}('${{author}}', ${{spaceVersion}}, '${{targetTable}}', ${{retainMetadata}});")
        .withQueryFragment("spaceVersion", "" + targetSpaceVersion)
        .withQueryFragment("author", targetAuthor)
        .withQueryFragment("targetTable", getRootTableName(space()))
        .withQueryFragment("retainMetadata", "" + isRetainMetadata())
        .withVariable("triggerFunction", triggerFunction)
        .withVariable("schema", getSchema(db()))
        .withVariable("table", TransportTools.getTemporaryTriggerTableName(getId()));
  }

  private SQLQuery buildCreateImportTriggerForNonEmptyLayer(String author, long newVersion) throws WebClientException {
    String triggerFunction = "import_from_s3_trigger_for_non_empty_layer";
    String superTable = space().getExtension() != null ? getRootTableName(superSpace()) : null;

    //TODO: Check if we can forward the whole transaction to the FeatureWriter rather than doing it for each row
    return new SQLQuery("""
        CREATE OR REPLACE TRIGGER insertTrigger BEFORE INSERT ON ${schema}.${table} 
          FOR EACH ROW EXECUTE PROCEDURE ${triggerFunction}(
             ${{author}},
             ${{spaceVersion}},
             false, --isPartial
             ${{onExists}},
             ${{onNotExists}},
             ${{onVersionConflict}},
             ${{onMergeConflict}},
             ${{historyEnabled}},
             ${{context}},
             ${{extendedTable}},
             '${{format}}',
             '${{entityPerLine}}',
             '${{targetTable}}'
             )
        """)
        .withQueryFragment("spaceVersion", Long.toString(newVersion))
        .withQueryFragment("author", "'" + author + "'")
        .withQueryFragment("onExists", updateStrategy.onExists() == null ? "NULL" : "'" + updateStrategy.onExists() + "'")
        .withQueryFragment("onNotExists", updateStrategy.onNotExists() == null ? "NULL" : "'" + updateStrategy.onNotExists() + "'")
        .withQueryFragment("onVersionConflict",
            updateStrategy.onVersionConflict() == null ? "NULL" : "'" + updateStrategy.onVersionConflict() + "'")
        .withQueryFragment("onMergeConflict",
            updateStrategy.onMergeConflict() == null ? "NULL" : "'" + updateStrategy.onMergeConflict() + "'")
        .withQueryFragment("historyEnabled", "" + (space().getVersionsToKeep() > 1))
        .withQueryFragment("context", superTable == null ? "NULL" : "'DEFAULT'")
        .withQueryFragment("extendedTable", superTable == null ? "NULL" : "'" + superTable + "'")
        .withQueryFragment("format", format.toString())
        .withQueryFragment("entityPerLine", entityPerLine.toString())
        .withQueryFragment("targetTable", getRootTableName(space()))
        .withVariable("schema", getSchema(db()))
        .withVariable("triggerFunction", triggerFunction)
        .withVariable("table", TransportTools.getTemporaryTriggerTableName(getId()));
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
              (SELECT sum(count) FROM ${schema}.${triggerTable} ) as imported_rows
                  FROM ${schema}.${tmpTable}
              WHERE POSITION('SUCCESS_MARKER' in state) = 0;
        """)
        .withVariable("schema", getSchema(db()))
        .withVariable("tmpTable", getTemporaryJobTableName(getId()))
        .withVariable("triggerTable", TransportTools.getTemporaryTriggerTableName(getId()));
  }

  private SQLQuery buildImportQuery() throws WebClientException {

    SQLQuery successQuery = buildSuccessCallbackQuery();
    SQLQuery failureQuery = buildFailureCallbackQuery();

    return new SQLQuery(
            "CALL execute_transfer(#{format}, '${{successQuery}}', '${{failureQuery}}');")
            .withContext(getQueryContext())
            .withAsyncProcedure(true)
            .withNamedParameter("format", format.toString())
            .withQueryFragment("successQuery", successQuery.substitute().text().replaceAll("'", "''"))
            .withQueryFragment("failureQuery", failureQuery.substitute().text().replaceAll("'", "''"));
  }

  private SQLQuery buildSuccessReportQuery() throws WebClientException {
    //Wait 5 seconds before report success to ensure event rule is successfully created before.
    return new SQLQuery("PERFORM pg_sleep(5)");
  }

  private Map<String, Object> getQueryContext() throws WebClientException {
    String superTable = space().getExtension() != null ? getRootTableName(superSpace()) : null;
    return createQueryContext(getId(), getSchema(db()), getRootTableName(space()), (space().getVersionsToKeep() > 1), superTable);
  }

  private SQLQuery buildFeatureWriterQuery(String featureList, long targetVersion) throws WebClientException, JsonProcessingException {
    return new SQLQuery("""
        SELECT (write_features::JSONB->>'count')::INT AS count FROM write_features(
          #{featureList},
          'Features',
          #{author},
          #{returnResult},
          #{version},
          #{onExists},
          #{onNotExists},
          #{onVersionConflict},
          #{onMergeConflict},
          #{isPartial}
        );""")
          .withNamedParameter("featureList", featureList)
          .withNamedParameter("author", space().getOwner())
          .withNamedParameter("returnResult", false)
          .withNamedParameter("version", targetVersion)
          .withNamedParameter("onExists", updateStrategy.onExists())
          .withNamedParameter("onNotExists", updateStrategy.onNotExists())
          .withNamedParameter("onVersionConflict", updateStrategy.onVersionConflict())
          .withNamedParameter("onMergeConflict", updateStrategy.onMergeConflict())
          .withNamedParameter("isPartial", false)
          .withContext(getQueryContext());
  }

  private SQLQuery buildProgressQuery(String schema, ImportFilesToSpace step) {
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
            .withVariable("table", getTemporaryJobTableName(step.getId()));
  }

  private double calculateNeededAcus(int threadCount) {
    double neededACUs;

    if (fileCount == -1)
      fileCount = currentInputsCount(UploadUrl.class);

    neededACUs = ResourceAndTimeCalculator.getInstance().calculateNeededImportAcus(
        getUncompressedUploadBytesEstimation(), fileCount, threadCount);
    neededACUs /= 4d; //TODO: Remove workaround once GraphSequentializer was implemented

    infoLog(JOB_EXECUTOR, this, "Calculated ACUS: expectedMemoryConsumption: "
            + getUncompressedUploadBytesEstimation() + " => neededACUs:" + neededACUs);
    return neededACUs;
  }

  //TODO: De-duplicate once CSV was removed (see GeoJson format class)
  public enum EntityPerLine {
    Feature,
    FeatureCollection
  }

  public enum Format {
    CSV_GEOJSON,
    CSV_JSON_WKB,
    GEOJSON;
  }
}
