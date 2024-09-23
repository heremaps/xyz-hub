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

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.jobs.steps.S3DataFile;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.impl.tools.ResourceAndTimeCalculator;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.outputs.FileStatistics;
import com.here.xyz.jobs.steps.resources.IOResource;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_EXECUTE;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.JOB_EXECUTOR;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_ON_ASYNC_SUCCESS;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_ON_STATE_CHECK;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.buildDropTemporaryTableQuery;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.buildInitialInsertsForTemporaryJobTable;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.buildProgressQuery;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.buildResetSuccessMarkerAndRunningOnes;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.buildTemporaryJobTableForImportQuery;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.createQueryContext;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.errorLog;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.getTemporaryJobTableName;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.infoLog;
import static com.here.xyz.util.web.XyzWebClient.WebClientException;

/**
 * This step imports a set of user provided inputs and imports their data into a specified space.
 * This step produces exactly one output of type {@link FeatureStatistics}.
 */
public class ExportSpaceToFiles extends SpaceBasedStep<ExportSpaceToFiles> {
    //Defines how many features a source layer need to have to start parallelization.
  public static final int PARALLELIZTATION_MIN_THRESHOLD = 10;//TODO: put back to 500k
  //Defines how many export threads are getting used
  public static final int PARALLELIZTATION_THREAD_COUNT = 8;

  @JsonView({Internal.class, Static.class})
  private int calculatedThreadCount = -1;

  @JsonView({Internal.class, Static.class})
  private double overallNeededAcus = -1;

  @JsonView({Internal.class, Static.class})
  private int estimatedSeconds = -1;

  private Format format = Format.GEOJSON;


  /**
   * TODO:
   *   Geometry-Filters
   *    private Geometry geometry;
   *    private int radius = -1;
   *    private boolean clipOnFilterGeometry;
   *
   *   Content-Filters
   *    private String propertyFilter;
   *    private SpaceContext context;
   *    private String targetVersion;
   *
   *   Version Filter:
   *    private VersionRef versionRef;
   *
   *   Partitioning - part of EMR?
   *    private String partitionKey;
   *    --Required if partitionKey=tileId
   *      private Integer targetLevel;
   *      private boolean clipOnPartitions;
   */

  public enum Format {
    CSV_JSON_WKB,
    CSV_PARTITIONED_JSON_WKB,
    GEOJSON;
  }

  @JsonView({Internal.class, Static.class})
  private StatisticsResponse statistics = null;

  @Override
  public List<Load> getNeededResources() {
    try {
      statistics = statistics != null ? statistics : loadSpaceStatistics(getSpaceId(), EXTENSION);
      overallNeededAcus = overallNeededAcus != -1 ?
              overallNeededAcus : ResourceAndTimeCalculator.getInstance().calculateNeededExportAcus(statistics.getDataSize().getValue());

      infoLog(JOB_EXECUTOR, this,"Calculated ACUS: byteSize of layer: "
              + statistics.getDataSize().getValue() + " => neededACUs:" + overallNeededAcus);

      return List.of(new Load().withResource(db()).withEstimatedVirtualUnits(overallNeededAcus),
              new Load().withResource(IOResource.getInstance()).withEstimatedVirtualUnits(getUncompressedUploadBytesEstimation()));
    }catch (Exception e){
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getTimeoutSeconds() {
    return 24 * 3600;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    if (estimatedSeconds == -1 && getSpaceId() != null) {
      estimatedSeconds = ResourceAndTimeCalculator.getInstance()
              .calculateExportTimeInSeconds(getSpaceId(), getUncompressedUploadBytesEstimation());
      infoLog(JOB_EXECUTOR, this,"Calculated estimatedSeconds: "+estimatedSeconds );
    }
    return estimatedSeconds;
  }

  @Override
  public String getDescription() {
    return "Export data from space " + getSpaceId();
  }

  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }

  @Override
  public boolean validate() throws ValidationException {
    super.validate();
    try {

      loadSpace(getSpaceId());

      /**
       * @TODO:
       * - Check if geometry is valid
       * - Check searchableProperties
       * - Check if targetVersion is valid
       * - Check if targetLevel is valid
      */
    }
    catch (WebClientException e) {
      throw new ValidationException("Error loading resource " + getSpaceId(), e);
    }
    return true;
  }

  @Override
  public void execute() throws Exception {
        
    statistics = statistics != null ? statistics : loadSpaceStatistics(getSpaceId(), EXTENSION);
    calculatedThreadCount = (statistics.getCount().getValue() > PARALLELIZTATION_MIN_THRESHOLD) ? PARALLELIZTATION_THREAD_COUNT : 1;

    List<S3DataFile> s3FileNames = generateS3FileNames(calculatedThreadCount);
    createAndFillTemporaryJobTable(s3FileNames);

    for (int i = 0; i < calculatedThreadCount; i++) {
      infoLog(STEP_EXECUTE, this,"Start export thread number: " + i );
      runReadQueryAsync(buildExportQuery(i), db(), 0,false);
    }
  }

  @Override
  public void resume() throws Exception {
    //TODO
  }

  @Override
  protected void onAsyncSuccess() throws Exception {
    //TODO
    super.onAsyncSuccess();

    FileStatistics statistics = runReadQuerySync(buildStatisticDataOfTemporaryTableQuery(), db(),
            0, rs -> rs.next()
                    ? new FileStatistics()
                        .withBytesUploaded(rs.getLong("bytes_uploaded"))
                        .withRowsUploaded(rs.getLong("rows_uploaded"))
                        .withFilesUploaded(rs.getInt("files_uploaded"))
                    : new FileStatistics());

    infoLog(STEP_ON_ASYNC_SUCCESS, this,"Job Statistics: bytes=" + statistics.getBytesUploaded() + " files=" + statistics.getFilesUploaded());
    registerOutputs(List.of(statistics), true);

    infoLog(STEP_ON_ASYNC_SUCCESS, this,"Cleanup temporary table");
    runWriteQuerySync(buildDropTemporaryTableQuery(getSchema(db()), getTemporaryJobTableName(this)), db(), 0);
  }

  @Override
  protected boolean onAsyncFailure() {
    //TODO
    return super.onAsyncFailure();
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
  
  private List<S3DataFile> generateS3FileNames(int cnt){
    List<S3DataFile> urlList = new ArrayList<>();
    
    for (int i = 1; i <= calculatedThreadCount; i++) {
      urlList.add(new DownloadUrl().withS3Key(outputS3Prefix(true,false) + "/" + i + "/" + UUID.randomUUID()));
    }

    return urlList;
  }

  private void createAndFillTemporaryJobTable(List<S3DataFile> s3FileNames) throws SQLException, TooManyResourcesClaimed, WebClientException {
    if (isResume()) {
      infoLog(STEP_EXECUTE, this,"Reset SuccessMarker");
      runWriteQuerySync(buildResetSuccessMarkerAndRunningOnes(getSchema(db()) ,this), db(), 0);
    }
    else {
      infoLog(STEP_EXECUTE, this,"Create temporary job table");
      runWriteQuerySync(buildTemporaryJobTableForImportQuery(getSchema(db()), this), db(), 0);

      infoLog(STEP_EXECUTE, this,"Fill temporary job table");
      runBatchWriteQuerySync(SQLQuery.batchOf(buildInitialInsertsForTemporaryJobTable(getSchema(db()),
              s3FileNames, bucketRegion(),this)), db(), 0 );
    }
  }

  private SQLQuery generateFilteredExportQuery(int threadNumber) throws WebClientException {
    return new SQLQuery("${{exportQuery}} ${{threadCondition}}")
            .withQueryFragment("exportQuery" ,"Select * from ${schema}.${table}")
            .withQueryFragment("threadCondition"," WHERE i % " + calculatedThreadCount + " = " + threadNumber)
            .withVariable("table", getRootTableName(space()))
            .withVariable("schema", getSchema(db()));
  }

  public SQLQuery buildExportQuery(int threadNumber) throws WebClientException {
    SQLQuery exportSelectString = generateFilteredExportQuery(threadNumber);

    SQLQuery successQuery = buildSuccessCallbackQuery();
    SQLQuery failureQuery = buildFailureCallbackQuery();

    return new SQLQuery(
                    "CALL execute_transfer(#{format}, '${{successQuery}}', '${{failureQuery}}', #{content_query} );")
                    .withContext(getQueryContext())
                    .withAsyncProcedure(true)
                    .withNamedParameter("format", format.toString())
                    .withQueryFragment("successQuery", successQuery.substitute().text().replaceAll("'", "''"))
                    .withQueryFragment("failureQuery", failureQuery.substitute().text().replaceAll("'", "''"))
                    .withNamedParameter("content_query", exportSelectString.substitute().text());
  }

  private SQLQuery buildStatisticDataOfTemporaryTableQuery() throws WebClientException {
    return new SQLQuery("""
          SELECT sum((data->'export_statistics'->'rows_uploaded')::bigint) as rows_uploaded,
                 sum((data->'export_statistics'->'files_uploaded')::bigint) as files_uploaded,
                 sum((data->'export_statistics'->'bytes_uploaded')::bigint) as bytes_uploaded
                  FROM ${schema}.${tmpTable}
              WHERE POSITION('SUCCESS_MARKER' in state) = 0;
        """)
            .withVariable("schema", getSchema(db()))
            .withVariable("tmpTable", getTemporaryJobTableName(this))
            .withVariable("triggerTable", TransportTools.getTemporaryTriggerTableName(this));
  }

  private Map<String, Object> getQueryContext() throws WebClientException {
    String superTable = space().getExtension() != null ? getRootTableName(superSpace()) : null;
    return createQueryContext(getId(), getSchema(db()), getRootTableName(space()), (space().getVersionsToKeep() > 1), superTable);
  }
}
