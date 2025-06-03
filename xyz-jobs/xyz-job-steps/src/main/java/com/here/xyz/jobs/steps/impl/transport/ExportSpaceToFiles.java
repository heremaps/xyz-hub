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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static com.here.xyz.jobs.steps.Step.Visibility.USER;
import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.WRITER;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.JOB_EXECUTOR;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.JOB_VALIDATE;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_EXECUTE;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_ON_ASYNC_SUCCESS;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.buildTemporaryJobTableDropStatement;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.errorLog;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.getTemporaryJobTableName;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.infoLog;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.jobs.steps.StepExecution;
import com.here.xyz.jobs.steps.execution.StepException;
import com.here.xyz.jobs.steps.execution.db.Database;
import com.here.xyz.jobs.steps.impl.tools.ResourceAndTimeCalculator;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.resources.IOResource;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.query.GetFeaturesByGeometryBuilder;
import com.here.xyz.psql.query.GetFeaturesByGeometryBuilder.GetFeaturesByGeometryInput;
import com.here.xyz.psql.query.QueryBuilder.QueryBuildingException;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.geo.GeoTools;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.xml.crypto.dsig.TransformException;
import org.geotools.api.referencing.FactoryException;
import org.locationtech.jts.geom.Geometry;


/**
 * The {@code ExportSpaceToFiles} class represents a step in a data processing workflow that exports data
 * from a specified space to files. This step supports spatial and property-based filtering of the exported data.
 *
 * <p>Key features of this class include:
 * <ul>
 *   <li>Supports spatial filtering to define export areas within allowed constraints.</li>
 *   <li>Allows property-based filtering to refine exported data.</li>
 *   <li>Ensures validation of spatial filters and version references.</li>
 *   <li>Handles exporting data using an AWS RDS export plugin.</li>
 *   <li>Computes estimated execution time based on data volume.</li>
 *   <li>Manages parallel execution for efficient data export.</li>
 * </ul>
 * </p>
 *
 * <p>This step produces outputs of type {@link FeatureStatistics} and {@link DownloadUrl}.</p>
 */
public class ExportSpaceToFiles extends TaskedSpaceBasedStep<ExportSpaceToFiles> {
  public static final String STATISTICS = "statistics";
  public static final String EXPORTED_DATA = "exportedData";
  //Defines how large the area of a defined spatialFilter can be
  //If a point is defined - the maximum radius can be 17898 meters
  private static final int MAX_ALLOWED_SPATALFILTER_AREA_IN_SQUARE_KM = 1_000;
  //Currently only used if there is no filter set
  private static final long MAX_TASK_COUNT = 1_000;
  private static final long MAX_BYTES_PER_TASK = 200L * 1024 * 1024; // 200MB in bytes
  public static final double ESTIMATED_SPATIAL_FILTERED_PEAK_ACUS = 0.05;
  public static final int ESTIMATED_SPATIAL_FILTERED_IO_BYTES = 100 * 1024 * 1024;

  @JsonView({Internal.class, Static.class})
  private int estimatedSeconds = -1;

  @JsonView({Internal.class, Static.class})
  protected SpatialFilter spatialFilter;
  @JsonView({Internal.class, Static.class})
  protected PropertiesQuery propertyFilter;

  @JsonView({Internal.class, Static.class})
  private long minI = -1;
  @JsonView({Internal.class, Static.class})
  private long maxI = -1;
  @JsonView({Internal.class, Static.class})
  protected boolean restrictExtendOfSpatialFilter = true;

  @JsonView({Internal.class, Static.class})
  protected Ref providedVersionRef;


  public void setProvidedVersionRef(Ref providedVersionRef) {
    this.providedVersionRef = providedVersionRef;
  }

  public Ref getProvidedVersionRef() {
    return this.providedVersionRef;
  }

  public ExportSpaceToFiles withProvidedVersionRef(Ref providedVersionRef) {
    setProvidedVersionRef(providedVersionRef);
    return this;
  }

  public void setRestrictExtendOfSpatialFilter(boolean restrictExtendOfSpatialFilter) {
    this.restrictExtendOfSpatialFilter = restrictExtendOfSpatialFilter;
  }

  public ExportSpaceToFiles withRestrictExtendOfSpatialFilter(boolean restrictExtendOfSpatialFilter) {
    setRestrictExtendOfSpatialFilter(restrictExtendOfSpatialFilter);
    return this;
  }

  public ExportSpaceToFiles withStepExecutionHeartBeatTimeoutOverride(int timeOutSeconds) {
    setStepExecutionHeartBeatTimeoutOverride(timeOutSeconds);
    return this;
  }

  public ExportSpaceToFiles withVersionRef(Ref versionRef) {
    setVersionRef(versionRef);
    return this;
  }

  public ExportSpaceToFiles withContext(SpaceContext context) {
    setContext(context);
    return this;
  }

  {
    setOutputSets(List.of(
        new OutputSet(STATISTICS, USER, true),
        new OutputSet(EXPORTED_DATA, USER, false)
    ));
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

  /**
   * Determines whether this {@code ExportSpaceToFiles} step execution is equivalent to another step execution.
   *
   * <p>Equivalence is defined based on the following conditions:
   * <ul>
   *   <li>The other step execution must also be an instance of {@code ExportSpaceToFiles}.</li>
   *   <li>The space associated with this step must be in read-only mode.</li>
   *   <li>The maximum version of the space's statistics must match the current maximum version.</li>
   *   <li>The following properties must be equal between the two instances:
   *     <ul>
   *       <li>Space ID</li>
   *       <li>Context</li>
   *       <li>Version reference (if not null)</li>
   *       <li>Spatial filter (if not null)</li>
   *       <li>Property filter (if not null)</li>
   *       <li>System output usage flag</li>
   *     </ul>
   *   </li>
   * </ul>
   *
   * @param other The other {@link StepExecution} to compare against.
   * @return {@code true} if the other step execution is equivalent to this one; {@code false} otherwise.
   * @throws RuntimeException If an exception occurs while comparing values.
   */
  @Override
  public boolean isEquivalentTo(StepExecution other) {
    if (!(other instanceof ExportSpaceToFiles otherExport))
      return super.isEquivalentTo(other);

    try {
      return Objects.equals(otherExport.getSpaceId(), getSpaceId())
          && Objects.equals(otherExport.versionRef, versionRef)
          && (otherExport.context == context || (space().getExtension() == null && otherExport.context == null && context == SUPER))
          && Objects.equals(otherExport.spatialFilter, spatialFilter)
          && Objects.equals(otherExport.propertyFilter, propertyFilter)
          && Objects.equals(otherExport.spaceCreatedAt, spaceCreatedAt);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<Load> getNeededResources() {
    try {
      if (spatialFilter != null)
        return List.of(
            new Load().withResource(dbReader()).withEstimatedVirtualUnits(ESTIMATED_SPATIAL_FILTERED_PEAK_ACUS),
            new Load().withResource(IOResource.getInstance()).withEstimatedVirtualUnits(ESTIMATED_SPATIAL_FILTERED_IO_BYTES)
        );

      StatisticsResponse statistics = spaceStatistics(context, true);
      overallNeededAcus = overallNeededAcus != -1 ? overallNeededAcus : calculateNeededExportAcus(statistics.getDataSize().getValue());

      infoLog(JOB_EXECUTOR, this, "Calculated ACUS: byteSize of layer: " + statistics.getDataSize().getValue()
          + " => neededACUs:" + overallNeededAcus);

      return List.of(
          new Load().withResource(dbReader()).withEstimatedVirtualUnits(overallNeededAcus),
          new Load().withResource(IOResource.getInstance()).withEstimatedVirtualUnits(getUncompressedUploadBytesEstimation()));
    }
    catch (WebClientException e) {
      throw new StepException("Error calculating the necessary resources for the step.", e);
    }
  }

  private double calculateNeededExportAcus(long bytesSizeEstimation) {
    //maximum auf Acus - to prevent that job never gets executed. @TODO: check how to deal is maxUnits of DB
    final double maxAcus = 70;
    //exports are not as heavy as imports
    //TODO: Implement more specific load calculation that matches the actual use case of exporting data
    final double exportDivisor = 3;

    //Calculate the needed ACUs
    double neededAcus = ResourceAndTimeCalculator.getInstance().calculateNeededAcusFromByteSize(bytesSizeEstimation) / exportDivisor;
    return Math.min(neededAcus, maxAcus);
  }

  @Override
  public int getTimeoutSeconds() {
    return 24 * 3600;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    //TODO: Fix estimation. Calculate estimatedSeconds on expected the number of features and the size of the data.
    if (estimatedSeconds == -1 && getSpaceId() != null) {
      estimatedSeconds = calculateExportTimeInSeconds(getSpaceId(), getUncompressedUploadBytesEstimation());
      infoLog(JOB_EXECUTOR, this,"Calculated estimatedSeconds: "+estimatedSeconds );
    }
    return estimatedSeconds;
  }

  private int calculateExportTimeInSeconds(String spaceId, long byteSize){
    int warmUpTime = 10;
    int bytesPerSecond = 57 * 1024 * 1024;
    return (int) (warmUpTime + ((double) byteSize / bytesPerSecond));
  }

  @Override
  public String getDescription() {
    return "Export data from space " + getSpaceId();
  }

  @Override
  public boolean validate() throws ValidationException {
    super.validate();

    if (versionRef.isAllVersions())
      throw new ValidationException("It is not supported to export all versions at once.");

    //Validate input Geometry
    if (this.spatialFilter != null) {
      if(spatialFilter.getGeometry() == null)
        throw new ValidationException("Invalid arguments! Geometry cant be null!");

      Geometry jtsGeometry = spatialFilter.getGeometry().getJTSGeometry();
      spatialFilter.validateSpatialFilter();

      //Enhanced Check validation check of Geometry with JTS
      if(jtsGeometry != null && !jtsGeometry.isValid())
        throw new ValidationException("Invalid geometry in spatialFilter!");

      if(restrictExtendOfSpatialFilter) {
        try {
          Geometry bufferedGeo = GeoTools.applyBufferInMetersToGeometry(jtsGeometry, spatialFilter.getRadius());
          double areaInSquareKilometersFromGeometry = GeoTools.getAreaInSquareKilometersFromGeometry(bufferedGeo);
          if ( areaInSquareKilometersFromGeometry > MAX_ALLOWED_SPATALFILTER_AREA_IN_SQUARE_KM) {
            throw new ValidationException( String.format("Invalid SpatialFilter! Provided area of filter geometry is to large! [%.2f km² > %d km²]",areaInSquareKilometersFromGeometry,MAX_ALLOWED_SPATALFILTER_AREA_IN_SQUARE_KM));
          }
        } catch (FactoryException | org.geotools.api.referencing.operation.TransformException | TransformException |
                 NullPointerException e) {
          errorLog(JOB_VALIDATE, this, e, "Invalid SpatialFilter provided! ", spatialFilter.getGeometry().serialize());
          throw new ValidationException("Invalid SpatialFilter!");
        }
      }
    }

      //TODO: Check if property validation is needed - in sense of searchableProperties
//      if(statistics.getCount().getValue() > 1_000_000 && getPropertyFilter() != null){
//        getPropertyFilter().getQueryKeys()
//          throw new ValidationException("is not a searchable property");
//      }

    return true;
  }

  @Override
  protected void onAsyncSuccess() throws Exception {
    String schema = getSchema(db());

    TransportStatistics stepStatistics = runReadQuerySync(retrieveStatisticFromTaskAndStatisticTable(schema), db(WRITER),
            0, rs -> rs.next()
                    ? new TransportStatistics(rs.getLong("rows_uploaded"), rs.getLong("bytes_uploaded"), rs.getInt("files_uploaded"))
                    : new TransportStatistics(0, 0, 0));

    infoLog(STEP_ON_ASYNC_SUCCESS, this, "Job Statistics: bytes=" + stepStatistics.byteSize + " files=" + stepStatistics.fileCount);

    registerOutputs(List.of(
            new FeatureStatistics()
                    .withFeatureCount(stepStatistics.rowCount)
                    .withByteSize(stepStatistics.byteSize)
                    .withVersionRef(providedVersionRef)
            ), STATISTICS);

    infoLog(STEP_ON_ASYNC_SUCCESS, this, "Cleanup temporary table");
    runWriteQuerySync(buildTemporaryJobTableDropStatement(schema, getTemporaryJobTableName(getId())), db(WRITER), 0);
  }

  @Override
  protected boolean onAsyncFailure() {
    //TODO
    return super.onAsyncFailure();
  }

  @Override
  protected int setInitialThreadCount(String schema) throws WebClientException {
    StatisticsResponse statistics = loadSpaceStatistics(getSpaceId(), context, true);
    return statistics.getCount().getValue() > PARALLELIZTATION_MIN_THRESHOLD ? PARALLELIZTATION_THREAD_COUNT : 1;
  }

  /**
   * Creates generic task items in the task and statistic table for each thread.
   * {@code generateTaskDataObject} is used to generate the task data for each thread.
   * This method can get overridden easily from other ExportProcesses.
   *
   * @param schema The database schema to use for the task items.
   * @return The number of task items created, which is equal to the calculated thread count.
   * @throws WebClientException If an error occurs while interacting with the web client.
   * @throws SQLException If an error occurs while executing SQL queries.
   * @throws TooManyResourcesClaimed If too many resources are claimed during the process.
   * @throws QueryBuildingException If an error occurs while building the SQL query.
   */
  protected List<TaskData> createTaskItems(String schema)
          throws WebClientException, SQLException, TooManyResourcesClaimed, QueryBuildingException{
    int taskListCount = calculatedThreadCount;

    //Todo: find a possibility to add more tasks if filters are set
    if(spatialFilter == null && propertyFilter == null) {
      //The dataSize includes indexes and other overhead so the resulting files will be smaller than the
      //defined MAX_BYTES_PER_TASK.
      Long estByteCount = spaceStatistics(context, true).getDataSize().getValue();
      infoLog(STEP_EXECUTE, this,"Retrieved estByteCount: " + estByteCount);
      long calculatedTaskCount = (estByteCount + MAX_BYTES_PER_TASK - 1) / MAX_BYTES_PER_TASK;
      // Ensure taskListCount does not exceed the maximum allowed limit
      taskListCount = (int) Math.min(calculatedTaskCount, MAX_TASK_COUNT);
    }

    List<TaskData> taskDataList = new ArrayList<>();
    for (int i = 0; i < taskListCount; i++) {
      taskDataList.add(new TaskData(i));
    }
    return taskDataList;
  }

  /**
   * Builds an SQL query for exporting data to S3 on the provided schema,
   * task ID, and task data. The AWS RDS EXPORT plugin is getting used.
   *
   * @param schema The database schema to use for the query.
   * @param taskId The ID of the task for which the query is being built.
   * @param taskData The data associated with the task.
   * @return The constructed SQL query.
   * @throws QueryBuildingException If an error occurs while building the SQL query.
   * @throws TooManyResourcesClaimed If too many resources are claimed during the process.
   * @throws WebClientException If an error occurs while interacting with the web client.
   */
  @Override
  protected SQLQuery buildTaskQuery(String schema, Integer taskId, TaskData taskData)
          throws QueryBuildingException, TooManyResourcesClaimed, WebClientException, InvalidGeometryException {
    return buildExportToS3PluginQuery(schema, taskId, generateContentQueryForExportPlugin(taskData));
  }

  protected GetFeaturesByGeometryInput createGetFeaturesByGeometryInput(SpaceContext context, SpatialFilter spatialFilter, Ref versionRef)
      throws WebClientException {
    Space space = context == SUPER ? superSpace() : space();

    return new GetFeaturesByGeometryInput(space.getId(), hubWebClient().loadConnector(space.getStorage().getId()).params,
        space().getExtension() != null ? space().resolveCompositeParams(superSpace()) : null, context, space.getVersionsToKeep(),
        versionRef, spatialFilter != null ? spatialFilter.getGeometry() : null, spatialFilter != null ? spatialFilter.getRadius() : 0,
        spatialFilter != null && spatialFilter.isClip(), propertyFilter);
  }

  private SQLQuery buildExportToS3PluginQuery(String schema, int taskId, String contentQuery) throws WebClientException {
    //TODO Group by threadId
    DownloadUrl downloadUrl = new DownloadUrl().withS3Key(toS3Path(getOutputSet(EXPORTED_DATA)) + "/" + taskId + "/" + UUID.randomUUID() + ".json");
    return new SQLQuery(
            "SELECT export_to_s3_perform(#{taskId},  #{s3_bucket}, #{s3_path}, #{s3_region}, #{step_payload}::JSON->'step', " +
                    "#{lambda_function_arn}, #{lambda_region}, #{contentQuery}, '${{failureCallback}}');")
            .withContext(getQueryContext(schema))
            .withAsyncProcedure(false)
            .withNamedParameter("taskId", taskId)
            .withNamedParameter("s3_bucket", downloadUrl.getS3Bucket())
            .withNamedParameter("s3_path", downloadUrl.getS3Key())
            .withNamedParameter("s3_region", bucketRegion())
            .withNamedParameter("step_payload", new LambdaStepRequest().withStep(this).serialize())
            .withNamedParameter("lambda_function_arn", getwOwnLambdaArn().toString())
            .withNamedParameter("lambda_region", getwOwnLambdaArn().getRegion())
            .withNamedParameter("contentQuery", contentQuery)
            .withQueryFragment("failureCallback",  buildFailureCallbackQuery().substitute().text().replaceAll("'", "''"));
  }

  private void loadIRange() {
    try {
      String table = getRootTableName(context == SUPER ? superSpace() : space());
      IRange iRange = loadIRange(table);
      if (space().getExtension() != null && (context == DEFAULT || context == null)) {
        IRange superIRange = loadIRange(getRootTableName(superSpace()));
        iRange = new IRange(Math.min(iRange.minI, superIRange.minI), Math.max(iRange.maxI, superIRange.maxI));
      }
      this.minI = iRange.minI;
      this.maxI = iRange.maxI;
    }
    catch (Exception e) {
      throw new StepException(e.getMessage(), e);
    }
  }

  private IRange loadIRange(String table) throws WebClientException, SQLException, TooManyResourcesClaimed {
    Database dbReader = dbReader();
    return runReadQuerySync(new SQLQuery("SELECT min(i) AS min_i, max(i) AS max_i FROM ${schema}.${table}")
        .withVariable("schema", getSchema(dbReader))
        .withVariable("table", table), dbReader, 0d, rs -> {
      if (!rs.next())
        throw new StepException("Error while loading min / max i values.");
      return new IRange(rs.getLong("min_i"), rs.getLong("max_i"));
    });
  }

  private record IRange(long minI, long maxI) {};

  private long loadMinI() {
    if (minI == -1)
      loadIRange();
    return minI;
  }

  private long loadMaxI() {
    if (maxI == -1)
      loadIRange();
    return maxI;
  }


  /**
   * Generates a content query for the export plugin based on the task data and context. This method
   * can get overridden easily from other ExportProcesses.
   *
   * We will build here a GetFeaturesByGeometry query and add a custom thread condition to it
   *
   * @param taskData The task data containing generic task data.
   * @return The generated content query as a string.
   * @throws WebClientException If an error occurs while interacting with the web client.
   * @throws TooManyResourcesClaimed If too many resources are claimed during the process.
   * @throws QueryBuildingException If an error occurs while building the SQL query.
   */
  protected String generateContentQueryForExportPlugin(TaskData taskData) throws WebClientException, TooManyResourcesClaimed,
      QueryBuildingException, InvalidGeometryException {

    //We use the thread number as a condition for the query
    int taskNumber = (int) taskData.taskInput();

    GetFeaturesByGeometryBuilder queryBuilder = new GetFeaturesByGeometryBuilder()
        .withDataSourceProvider(requestResource(dbReader(), 0));

    GetFeaturesByGeometryInput input = createGetFeaturesByGeometryInput(context == null ? DEFAULT : context, spatialFilter, versionRef);

    long minI = loadMinI();
    long maxI = loadMaxI();
    long iRangeSize = (long) Math.ceil((double) (maxI - minI + 1) / (double) taskItemCount);

    SQLQuery threadCondition = new SQLQuery("i >= #{minI} + #{taskNumber} * #{iRangeSize} AND i < #{minI} + (#{taskNumber} + 1) * #{iRangeSize}")
        .withNamedParameter("minI", minI)
        .withNamedParameter("taskNumber", taskNumber)
        .withNamedParameter("iRangeSize", iRangeSize);

    return queryBuilder
        .withAdditionalFilterFragment(threadCondition)
        .buildQuery(input)
        .toExecutableQueryString();
  }

  private record TransportStatistics(long rowCount, long byteSize, int fileCount) {}
}
