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
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static com.here.xyz.jobs.steps.Step.Visibility.SYSTEM;
import static com.here.xyz.jobs.steps.Step.Visibility.USER;
import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.WRITER;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.JOB_EXECUTOR;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.JOB_VALIDATE;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_EXECUTE;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_ON_ASYNC_SUCCESS;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_RESUME;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.buildTemporaryJobTableDropStatement;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.createQueryContext;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.errorLog;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.getTemporaryJobTableName;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.infoLog;
import static com.here.xyz.util.web.XyzWebClient.WebClientException;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.jobs.JobClientInfo;
import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.StepExecution;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.ProcessUpdate;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.impl.tools.ResourceAndTimeCalculator;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.resources.IOResource;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.psql.query.GetFeaturesByGeometryBuilder;
import com.here.xyz.psql.query.GetFeaturesByGeometryBuilder.GetFeaturesByGeometryInput;
import com.here.xyz.psql.query.QueryBuilder.QueryBuildingException;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.geo.GeoTools;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import javax.xml.crypto.dsig.TransformException;
import org.geotools.api.referencing.FactoryException;
import org.locationtech.jts.geom.Geometry;


/**
 * This step imports a set of user provided inputs and imports their data into a specified space.
 * This step produces exactly one output of type {@link FeatureStatistics}.
 */
public class ExportSpaceToFiles extends SpaceBasedStep<ExportSpaceToFiles> {
  //Defines how many features a source layer need to have to start parallelization.
  public static final int PARALLELIZTATION_MIN_THRESHOLD = 200_000;
  //Defines how many export threads are getting used
  public static final int PARALLELIZTATION_THREAD_COUNT = 8;
  public static final String STATISTICS = "statistics";
  public static final String INTERNAL_STATISTICS = "internalStatistics";
  public static final String EXPORTED_DATA = "exportedData";
  //Defines how large the area of a defined spatialFilter can be
  //If a point is defined - the maximum radius can be 17898 meters
  private static final int MAX_ALLOWED_SPATALFILTER_AREA_IN_SQUARE_KM = 1_000;
  private static final int FEATURES_PER_CHAIN_LINK = 100_000;

  @JsonView({Internal.class, Static.class})
  private int calculatedThreadCount = -1;

  @JsonView({Internal.class, Static.class})
  private double overallNeededAcus = -1;

  @JsonView({Internal.class, Static.class})
  private int estimatedSeconds = -1;

  @JsonView({Internal.class, Static.class})
  private SpatialFilter spatialFilter;
  @JsonView({Internal.class, Static.class})
  private PropertiesQuery propertyFilter;
  @JsonView({Internal.class, Static.class})
  private SpaceContext context;
  @JsonView({Internal.class, Static.class})
  private Ref versionRef;

  {
    setOutputSets(List.of(
        new OutputSet(STATISTICS, USER, true),
        new OutputSet(INTERNAL_STATISTICS, SYSTEM, true),
        new OutputSet(EXPORTED_DATA, USER, false)
    ));
  }

  /**
   * TODO:
   *   Partitioning - part of EMR?
   *    private String partitionKey;
   *    --Required if partitionKey=tileId
   *      private Integer targetLevel;
   *      private boolean clipOnPartitions;
   */

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
    return this.context;
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

  @Override
  public List<Load> getNeededResources() {
    try {
      StatisticsResponse statistics = spaceStatistics(context, true);
      overallNeededAcus = overallNeededAcus != -1 ?
              overallNeededAcus : ResourceAndTimeCalculator.getInstance().calculateNeededExportAcus(statistics.getDataSize().getValue());

      infoLog(JOB_EXECUTOR, this,"Calculated ACUS: byteSize of layer: "
              + statistics.getDataSize().getValue() + " => neededACUs:" + overallNeededAcus);

      return List.of(
              new Load().withResource(dbReader()).withEstimatedVirtualUnits(overallNeededAcus),
              new Load().withResource(IOResource.getInstance()).withEstimatedVirtualUnits(getUncompressedUploadBytesEstimation()));
    }catch (Exception e){
      throw new RuntimeException(e);
    }
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
          && Objects.equals(otherExport.propertyFilter, propertyFilter);
    }
    catch (Exception e) {
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
  public void prepare(String owner, JobClientInfo ownerAuth) throws ValidationException {
    if(versionRef == null)
      throw new ValidationException("Version ref is required.");

    validateSpaceExists();
    //Resolve the ref to an actual version
    if (versionRef.isTag()) {
      try {
        setVersionRef(new Ref(hubWebClient().loadTag(getSpaceId(), versionRef.getTag()).getVersion()));
      }
      catch (WebClientException e) {
        throw new ValidationException("Unable to resolve tag \"" + versionRef.getTag() + "\" of " + getSpaceId(), e);
      }
    }
    else if (versionRef.isHead()) {
      try {
        setVersionRef(new Ref(spaceStatistics(context, true).getMaxVersion().getValue()));
      }
      catch (WebClientException e) {
        throw new ValidationException("Unable to resolve HEAD version of " + getSpaceId(), e);
      }
    }
  }

  @Override
  public boolean validate() throws ValidationException {
    super.validate();

    if (versionRef.isAllVersions())
      throw new ValidationException("It is not supported to export all versions at once.");

    try {
      StatisticsResponse statistics = loadSpaceStatistics(getSpaceId(), context, true);

      //Validate input Geometry
      if (this.spatialFilter != null) {
        spatialFilter.validateSpatialFilter();
        try {
          Geometry bufferedGeo = GeoTools.applyBufferInMetersToGeometry((spatialFilter.getGeometry().getJTSGeometry()),
                  spatialFilter.getRadius());
          int areaInSquareKilometersFromGeometry = (int) GeoTools.getAreaInSquareKilometersFromGeometry(bufferedGeo);
          if(GeoTools.getAreaInSquareKilometersFromGeometry(bufferedGeo) > MAX_ALLOWED_SPATALFILTER_AREA_IN_SQUARE_KM) {
            throw new ValidationException("Invalid SpatialFilter! Provided area of filter geometry is to large! ["
              + areaInSquareKilometersFromGeometry + " km² > " + MAX_ALLOWED_SPATALFILTER_AREA_IN_SQUARE_KM + " km²]");
          }
        } catch (FactoryException | org.geotools.api.referencing.operation.TransformException | TransformException e) {
          errorLog(JOB_VALIDATE, this, e, "Invalid Filter provided!");
          throw new ValidationException("Invalid SpatialFilter!");
        }
      }

      //Validate versionRef
      if (this.versionRef == null)
        return true;

      Long minSpaceVersion = statistics.getMinVersion().getValue();
      Long maxSpaceVersion = statistics.getMaxVersion().getValue();

      if (this.versionRef.isSingleVersion()) {
        if (this.versionRef.getVersion() < minSpaceVersion)
          throw new ValidationException("Invalid VersionRef! Version is smaller than min available version '" +
              minSpaceVersion + "'!");
        if (this.versionRef.getVersion() > maxSpaceVersion)
          throw new ValidationException("Invalid VersionRef! Version is higher than max available version '" +
              maxSpaceVersion + "'!");
      }
      else if (this.versionRef.isRange()) {
        if (this.versionRef.getStartVersion() < minSpaceVersion)
          throw new ValidationException("Invalid VersionRef! StartVersion is smaller than min available version '" +
              minSpaceVersion + "'!");
        if (this.versionRef.getEndVersion() > maxSpaceVersion)
          throw new ValidationException("Invalid VersionRef! EndVersion is higher than max available version '" +
              maxSpaceVersion + "'!");
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
  public void execute(boolean resume) throws Exception {
    if (resume) {
      resume();
      return;
    }
    String schema = getSchema(db());
    StatisticsResponse statistics = loadSpaceStatistics(getSpaceId(), context, true);
    calculatedThreadCount = statistics.getCount().getValue() > PARALLELIZTATION_MIN_THRESHOLD ? PARALLELIZTATION_THREAD_COUNT : 1;

    // create progress update table
    runWriteQuerySync(buildProcessUpdateTableStatement(schema, this), db(WRITER), 0);

    for (int i = 0; i < calculatedThreadCount; i++) {
      infoLog(STEP_EXECUTE, this,"Add initial entry in process_table for thread number: " + i );
      runWriteQuerySync(upsertProcessUpdateForThreadQuery(schema, this, i, 0, 0 ,0 , false), db(WRITER), 0);

      infoLog(STEP_EXECUTE, this,"Start export thread number: " + i );
      runReadQueryAsync(buildExportQuery(schema, i), dbReader(),
              overallNeededAcus/calculatedThreadCount,false);
    }
  }

  //TODO: Remove Code-duplication (integrate into #execute())
  public void resume() throws Exception {
    String schema = getSchema(db());

    //TODO: add featureCount if chunking is possible
    List<Integer> threadList = Arrays.stream((int[]) runReadQuerySync(retrieveProcessItemsForResumeQuery(schema, this), db(WRITER), 0,
            rs -> rs.next() ? rs.getArray("threads") : rs.getArray("threads")
    ).getArray()).boxed().toList();

    infoLog(STEP_RESUME, this,"Resume with "+threadList.size()+" threads!");

    //TODO: check exception Type
    if(threadList.size() == 0)
      throw new ValidationException("Resume is not possible!");

    for (int i = 0; i < calculatedThreadCount; i++) {
      if(threadList.contains(Integer.valueOf(i))) {
        infoLog(STEP_EXECUTE, this, "Start export for thread number: " + i);
        runReadQueryAsync(buildExportQuery(schema, i), dbReader(), overallNeededAcus/threadList.size(), false);
      }
    }
  }

  @Override
  protected void onAsyncSuccess() throws Exception {
    String schema = getSchema(db());

    Statistics statistics = runReadQuerySync(buildStatisticDataOfTemporaryTableQuery(schema), db(WRITER),
        0, rs -> rs.next()
            ? createStatistics(rs.getLong("rows_uploaded"), rs.getLong("bytes_uploaded"),
            rs.getInt("files_uploaded"))
            : createStatistics(0, 0, 0));

    infoLog(STEP_ON_ASYNC_SUCCESS, this, "Job Statistics: bytes=" + statistics.published().getByteSize()
        + " files=" + statistics.internal().getFileCount());

    registerOutputs(List.of(statistics.published()), STATISTICS);
    registerOutputs(List.of(statistics.internal()), INTERNAL_STATISTICS);

    infoLog(STEP_ON_ASYNC_SUCCESS, this, "Cleanup temporary table");
    runWriteQuerySync(buildTemporaryJobTableDropStatement(schema, getTemporaryJobTableName(getId())), db(WRITER), 0);
  }

  @Override
  protected boolean onAsyncUpdate(ProcessUpdate processUpdate){
    try {
      FeaturesExportedUpdate update = (FeaturesExportedUpdate) processUpdate;
      updateStatisticsTable(update);

      int completedThreads = countCompletedThreads();
      if (completedThreads == calculatedThreadCount)
        return true;
      else
        //Calculate progress and set it on the step's status
        getStatus().setEstimatedProgress((float) completedThreads / (float) calculatedThreadCount); //TODO: Can be calculated in higher detail once chain-links were implemented

      return false;
    }catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void updateStatisticsTable(FeaturesExportedUpdate update) throws WebClientException, SQLException, TooManyResourcesClaimed {
    /** TODO: Implement Chunking */
    boolean threadComplete = true;//update.featureCount < FEATURES_PER_CHAIN_LINK;

    /** create update process table */
    runWriteQuerySync(
            upsertProcessUpdateForThreadQuery(getSchema(db(WRITER)), this, update.threadId,
              update.byteCount, update.featureCount, update.fileCount, threadComplete
    ), db(WRITER), 0);
  }

  private int countCompletedThreads() throws WebClientException, SQLException, TooManyResourcesClaimed {
    return runReadQuerySync(retrieveFinalizedProcessItems(getSchema(db(WRITER)), this), db(WRITER), 0,
      rs -> rs.next() ? rs.getInt("count") : -1);
  }

  public static class FeaturesExportedUpdate extends ProcessUpdate<FeaturesExportedUpdate> {
    public int threadId;
    public long byteCount;
    public long featureCount;
    public int fileCount;
  }

  private static Statistics createStatistics(long featureCount, long byteSize, int fileCount) {
    return new Statistics(
        //NOTE: Do not publish the file count for the user facing statistics, as it could be confusing when it comes to invisible intermediate outputs
        new FeatureStatistics().withByteSize(byteSize).withFeatureCount(featureCount),
        new FeatureStatistics().withFileCount(fileCount)
    );
  }

  private record Statistics(FeatureStatistics published, FeatureStatistics internal) {}

  @Override
  protected boolean onAsyncFailure() {
    //TODO
    return super.onAsyncFailure();
  }

  private String generateFilteredExportQuery(int threadNumber) throws WebClientException, TooManyResourcesClaimed, QueryBuildingException {
    GetFeaturesByGeometryBuilder queryBuilder = new GetFeaturesByGeometryBuilder()
        .withDataSourceProvider(requestResource(dbReader(), 0));
    if(context == SUPER)
      space().switchToSuper(superSpace().getId());

    GetFeaturesByGeometryInput input = new GetFeaturesByGeometryInput(
        space().getId(),
        hubWebClient().loadConnector(space().getStorage().getId()).params,
        space().getExtension() != null ? space().resolveCompositeParams(superSpace()) : null,
        context == null ? DEFAULT : context,
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

  public SQLQuery buildExportQuery(String schema, int threadNumber) throws WebClientException, TooManyResourcesClaimed,
          QueryBuildingException {
    DownloadUrl downloadUrl = new DownloadUrl().withS3Key(toS3Path(getOutputSet(EXPORTED_DATA)) + "/" + threadNumber + "/" + UUID.randomUUID() + ".json");
    return new SQLQuery(
            "SELECT export_to_s3_perform(#{thread_id},  #{s3_bucket}, #{s3_path}, #{s3_region}, #{step_payload}::JSON->'step', " +
                    "#{lambda_function_arn}, #{lambda_region}, #{contentQuery}, '${{failureCallback}}');")
            .withContext(getQueryContext(schema))
            .withAsyncProcedure(false)
            .withNamedParameter("thread_id", threadNumber)
            .withNamedParameter("s3_bucket", downloadUrl.getS3Bucket())
            .withNamedParameter("s3_path", downloadUrl.getS3Key())
            .withNamedParameter("s3_region", bucketRegion())
            .withNamedParameter("step_payload", new LambdaStepRequest().withStep(this).serialize())
            .withNamedParameter("lambda_function_arn", getwOwnLambdaArn().toString())
            .withNamedParameter("lambda_region", getwOwnLambdaArn().getRegion())
            .withNamedParameter("contentQuery", generateFilteredExportQuery(threadNumber))
            .withQueryFragment("failureCallback",  buildFailureCallbackQuery().substitute().text().replaceAll("'", "''"));
  }

  private SQLQuery buildStatisticDataOfTemporaryTableQuery(String schema) {
    return new SQLQuery("""
          SELECT sum(rows_uploaded) as rows_uploaded,
                 sum(CASE
                     WHEN (bytes_uploaded)::bigint > 0
                     THEN (files_uploaded)::bigint
                     ELSE 0
                 END) as files_uploaded,
                 sum(bytes_uploaded)::bigint as bytes_uploaded
                  FROM ${schema}.${tmpTable};
        """)
            .withVariable("schema", schema)
            .withVariable("tmpTable", getTemporaryJobTableName(getId()));
  }

  protected static SQLQuery buildProcessUpdateTableStatement(String schema, Step step) {
    return new SQLQuery("""          
            CREATE TABLE IF NOT EXISTS ${schema}.${table}
            (
            	thread_id INT,
            	bytes_uploaded BIGINT DEFAULT 0,
            	rows_uploaded BIGINT DEFAULT 0,
            	files_uploaded INT DEFAULT 0,
            	finalized BOOLEAN DEFAULT false,
            	CONSTRAINT ${primaryKey} PRIMARY KEY (thread_id)
            );
        """)
            .withVariable("table", getTemporaryJobTableName(step.getId()))
            .withVariable("schema", schema)
            .withVariable("primaryKey", getTemporaryJobTableName(step.getId()) + "_primKey");
  }

  protected static SQLQuery upsertProcessUpdateForThreadQuery(String schema, Step step,
                                                              int threadId, long bytesUploaded, long rowsUploaded, int filesUploaded,
                                                              boolean finalized) {
    /** TODO: switch to complete upsert */
    return new SQLQuery("""             
            INSERT INTO  ${schema}.${table} AS t (thread_id, bytes_uploaded, rows_uploaded, files_uploaded, finalized)
                VALUES (#{threadId}, #{bytesUploaded}, #{rowsUploaded}, #{filesUploaded}, #{finalized})
                ON CONFLICT (thread_id) DO UPDATE
                  SET thread_id = #{threadId},
                    bytes_uploaded = t.bytes_uploaded + EXCLUDED.bytes_uploaded,
                    rows_uploaded = t.rows_uploaded + EXCLUDED.rows_uploaded,
                    files_uploaded = t.files_uploaded + EXCLUDED.files_uploaded,
                    finalized = EXCLUDED.finalized;
        """)
            .withVariable("schema", schema)
            .withVariable("table", getTemporaryJobTableName(step.getId()))
            .withNamedParameter("threadId", threadId)
            .withNamedParameter("bytesUploaded", bytesUploaded)
            .withNamedParameter("rowsUploaded", rowsUploaded)
            .withNamedParameter("filesUploaded", filesUploaded)
            .withNamedParameter("finalized", finalized); //future prove
  }

  protected static SQLQuery retrieveFinalizedProcessItems(String schema, Step step){
    return new SQLQuery("SELECT count(1) FROM ${schema}.${table} WHERE finalized = true;")
            .withVariable("schema", schema)
            .withVariable("table", getTemporaryJobTableName(step.getId()));
  }

  protected static SQLQuery retrieveProcessItemsForResumeQuery(String schema, Step step){
    return new SQLQuery("select array_agg(thread_id) as threads FROM ${schema}.${table} where finalized != true")
            .withVariable("schema", schema)
            .withVariable("table", getTemporaryJobTableName(step.getId()));
  }

  private Map<String, Object> getQueryContext(String schema) throws WebClientException {
    String superTable = space().getExtension() != null ? getRootTableName(superSpace()) : null;
    return createQueryContext(getId(), schema, getRootTableName(space()), (space().getVersionsToKeep() > 1), superTable);
  }
}
