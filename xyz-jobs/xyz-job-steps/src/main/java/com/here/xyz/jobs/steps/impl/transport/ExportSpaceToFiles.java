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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.JOB_EXECUTOR;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_EXECUTE;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_ON_ASYNC_SUCCESS;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_ON_STATE_CHECK;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.buildProgressQuery;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.buildResetSuccessMarkerAndRunningOnesStatement;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.buildTemporaryJobTableCreateStatement;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.buildTemporaryJobTableDropStatement;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.buildTemporaryJobTableInsertStatements;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.createQueryContext;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.errorLog;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.getTemporaryJobTableName;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.infoLog;
import static com.here.xyz.util.web.XyzWebClient.WebClientException;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.jobs.steps.S3DataFile;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.impl.tools.ResourceAndTimeCalculator;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.outputs.FileStatistics;
import com.here.xyz.jobs.steps.resources.IOResource;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.psql.query.GetFeaturesByGeometryBuilder;
import com.here.xyz.psql.query.GetFeaturesByGeometryBuilder.GetFeaturesByGeometryInput;
import com.here.xyz.psql.query.QueryBuilder.QueryBuildingException;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

  @JsonView({Internal.class, Static.class})
  private boolean addStatisticsToUserOutput = true;

  private Format format = Format.GEOJSON;

  private SpatialFilter spatialFilter;
  private PropertiesQuery propertyFilter;
  private SpaceContext context;

  private Ref versionRef;

  /**
   * TODO:
   *   Spatial-Filters
   *    DONE
   *
   *   Content-Filters
   *    DONE private String propertyFilter;
   *    DONE private SpaceContext context;
   *   ? private String targetVersion;
   *
   *   Version Filter:
   *    DONE private VersionRef versionRef;
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

  public SpatialFilter getSpatialFilter() {
    return spatialFilter;
  }

  public void setSpatialFilter(SpatialFilter spatialFilter) {
    this.spatialFilter = spatialFilter;
  }

  public ExportSpaceToFiles withSpatialFilter(SpatialFilter spatialFilter) {
    setSpatialFilter(spatialFilter);
    return this;
  }

  public PropertiesQuery getPropertyFilter() {
    return propertyFilter;
  }

  public void setPropertyFilter(PropertiesQuery propertyFilter) {
    this.propertyFilter = propertyFilter;
  }

  public ExportSpaceToFiles withPropertyFilter(PropertiesQuery propertyFilter){
    setPropertyFilter(propertyFilter);
    return this;
  }

  public SpaceContext getContext() {
    return context == null ? EXTENSION :context;
  }

  public void setContext(SpaceContext context) {
    this.context = context;
  }

  public ExportSpaceToFiles withContext(SpaceContext context) {
    setContext(context);
    return this;
  }

  public Ref getVersionRef() {
    return versionRef;
  }

  public void setVersionRef(Ref versionRef) {
    this.versionRef = versionRef;
  }

  public ExportSpaceToFiles withVersionRef(Ref versionRef) {
    setVersionRef(versionRef);
    return this;
  }

  public boolean isAddStatisticsToUserOutput() {
    return addStatisticsToUserOutput;
  }

  public void setAddStatisticsToUserOutput(boolean addStatisticsToUserOutput) {
    this.addStatisticsToUserOutput = addStatisticsToUserOutput;
  }

  public ExportSpaceToFiles withAddStatisticsToUserOutput(boolean addStatisticsToUserOutput) {
    setAddStatisticsToUserOutput(addStatisticsToUserOutput);
    return this;
  }

  @JsonView({Internal.class, Static.class})
  private StatisticsResponse statistics = null;

  @Override
  public List<Load> getNeededResources() {
    try {
      statistics = statistics != null ? statistics : loadSpaceStatistics(getSpaceId(), context);
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
      statistics = statistics != null ? statistics : loadSpaceStatistics(getSpaceId(), context);

      //Validate input Geometry
      if(this.spatialFilter != null)
        this.spatialFilter.validateSpatialFilter();

      //Validate versionRef
      if(this.versionRef == null)
        return true;

      Long minSpaceVersion = statistics.getMinVersion().getValue();
      Long maxSpaceVersion = statistics.getMaxVersion().getValue();

      if(this.versionRef.isSingleVersion()){
        if(this.versionRef.getVersion() < minSpaceVersion)
          throw new ValidationException("Invalid VersionRef! Version is smaller than min available version '"+
                  minSpaceVersion+"'!");
        if(this.versionRef.getVersion() > maxSpaceVersion)
          throw new ValidationException("Invalid VersionRef! Version is higher than max available version '"+
                  maxSpaceVersion+"'!");
      }else if(this.versionRef.isRange()){
        if(this.versionRef.getStartVersion() < minSpaceVersion)
          throw new ValidationException("Invalid VersionRef! StartVersion is smaller than min available version '"+
                  minSpaceVersion+"'!");
        if(this.versionRef.getEndVersion() > maxSpaceVersion)
          throw new ValidationException("Invalid VersionRef! EndVersion is higher than max available version '"+
                  maxSpaceVersion+"'!");
      }


      //TODO: Check if property validation is needed - in sense of searchableProperties
//      if(statistics.getCount().getValue() > 1_000_000 && getPropertyFilter() != null){
//        getPropertyFilter().getQueryKeys()
//          throw new ValidationException("is not a searchable property");
//      }
    }
    catch (WebClientException e) {
      throw new ValidationException("Error loading resource " + getSpaceId(), e);
    }
    return true;
  }

  @Override
  public void execute() throws Exception {
    statistics = statistics != null ? statistics : loadSpaceStatistics(getSpaceId(), context);
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
                        .withBytesExported(rs.getLong("bytes_uploaded"))
                        .withRowsExported(rs.getLong("rows_uploaded"))
                        .withFilesCreated(rs.getInt("files_uploaded"))
                    : new FileStatistics());

    infoLog(STEP_ON_ASYNC_SUCCESS, this,"Job Statistics: bytes=" + statistics.getExportedBytes() + " files=" + statistics.getExportedFiles());
    if(addStatisticsToUserOutput)
      registerOutputs(List.of(statistics), true);

    infoLog(STEP_ON_ASYNC_SUCCESS, this,"Cleanup temporary table");
    runWriteQuerySync(buildTemporaryJobTableDropStatement(getSchema(db()), getTemporaryJobTableName(getId())), db(), 0);
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
      urlList.add(new DownloadUrl().withS3Key(outputS3Prefix(!isUseSystemOutput(),false) + "/" + i + "/" + UUID.randomUUID()));
    }

    return urlList;
  }

  private void createAndFillTemporaryJobTable(List<S3DataFile> s3FileNames) throws SQLException, TooManyResourcesClaimed, WebClientException {
    if (isResume()) {
      infoLog(STEP_EXECUTE, this,"Reset SuccessMarker");
      runWriteQuerySync(buildResetSuccessMarkerAndRunningOnesStatement(getSchema(db()) ,this), db(), 0);
    }
    else {
      infoLog(STEP_EXECUTE, this,"Create temporary job table");
      runWriteQuerySync(buildTemporaryJobTableCreateStatement(getSchema(db()), this), db(), 0);

      infoLog(STEP_EXECUTE, this,"Fill temporary job table");
      runBatchWriteQuerySync(SQLQuery.batchOf(buildTemporaryJobTableInsertStatements(getSchema(db()),
              s3FileNames, bucketRegion(),this)), db(), 0 );
    }
  }

  private String generateFilteredExportQuery(int threadNumber) throws WebClientException, TooManyResourcesClaimed, QueryBuildingException {
    GetFeaturesByGeometryBuilder queryBuilder = new GetFeaturesByGeometryBuilder()
        .withDataSourceProvider(requestResource(db(), 0));

    GetFeaturesByGeometryInput input = new GetFeaturesByGeometryInput(
        getSpaceId(),
        context == null ? EXTENSION : context,
        space().getVersionsToKeep(),
        versionRef,
        spatialFilter != null ? spatialFilter.getGeometry() : null,
        spatialFilter != null ? spatialFilter.getRadius() : 0,
        spatialFilter != null && spatialFilter.isClip(),
        propertyFilter
    );

    SQLQuery threadCondition = new SQLQuery("i % #{threadCount} = #{threadNumber}")
        .withNamedParameter("threadCount", calculatedThreadCount)
        .withNamedParameter("threadNumber", threadNumber);

    return queryBuilder
        .withAdditionalFilterFragment(threadCondition)
        .buildQuery(input)
        .toExecutableQueryString();
  }

  public SQLQuery buildExportQuery(int threadNumber) throws WebClientException, TooManyResourcesClaimed,
          QueryBuildingException {
    String exportSelectString = generateFilteredExportQuery(threadNumber);

    SQLQuery successQuery = buildSuccessCallbackQuery();
    SQLQuery failureQuery = buildFailureCallbackQuery();

    return new SQLQuery(
                    "CALL execute_transfer(#{format}, '${{successQuery}}', '${{failureQuery}}', #{contentQuery});")
                    .withContext(getQueryContext())
                    .withAsyncProcedure(true)
                    .withNamedParameter("format", format.toString())
                    .withQueryFragment("successQuery", successQuery.substitute().text().replaceAll("'", "''"))
                    .withQueryFragment("failureQuery", failureQuery.substitute().text().replaceAll("'", "''"))
                    .withNamedParameter("contentQuery", exportSelectString);
  }

  private SQLQuery buildStatisticDataOfTemporaryTableQuery() throws WebClientException {
    return new SQLQuery("""
          SELECT sum((data->'export_statistics'->'rows_uploaded')::bigint) as rows_uploaded,
                 sum(CASE
                     WHEN (data->'export_statistics'->'bytes_uploaded')::bigint > 0
                     THEN (data->'export_statistics'->'files_uploaded')::bigint
                     ELSE 0
                 END) as files_uploaded,
                 sum((data->'export_statistics'->'bytes_uploaded')::bigint) as bytes_uploaded
                  FROM ${schema}.${tmpTable}
              WHERE POSITION('SUCCESS_MARKER' in state) = 0;
        """)
            .withVariable("schema", getSchema(db()))
            .withVariable("tmpTable", getTemporaryJobTableName(getId()))
            .withVariable("triggerTable", TransportTools.getTemporaryTriggerTableName(getId()));
  }

  private Map<String, Object> getQueryContext() throws WebClientException {
    String superTable = space().getExtension() != null ? getRootTableName(superSpace()) : null;
    return createQueryContext(getId(), getSchema(db()), getRootTableName(space()), (space().getVersionsToKeep() > 1), superTable);
  }
}
