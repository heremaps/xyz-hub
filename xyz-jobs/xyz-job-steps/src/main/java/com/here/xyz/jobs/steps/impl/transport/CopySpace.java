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
import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.WRITER;
import static com.here.xyz.jobs.steps.execution.db.Database.loadDatabase;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_EXECUTE;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_RESUME;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.createQueryContext;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.infoLog;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.jobs.steps.execution.StepException;
import com.here.xyz.jobs.steps.execution.db.Database;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.inputs.InputFromOutput;
import com.here.xyz.jobs.steps.outputs.CreatedVersion;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.query.GetFeaturesByGeometryBuilder;
import com.here.xyz.psql.query.GetFeaturesByGeometryBuilder.GetFeaturesByGeometryInput;
import com.here.xyz.psql.query.QueryBuilder.QueryBuildingException;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This step copies the content of a source space into a target space. It is possible that the target space
 * already contains data. Spaces where history is enabled are also supported. With filters, it is possible to
 * only copy a dataset subset from the source space.
 */
//TODO: Merge version creation step into this step again (basically both: pre- & post-step)
public class CopySpace extends SpaceBasedStep<CopySpace> {
  private static final Logger logger = LogManager.getLogger();

  //Input settings for the compiler:
  @JsonView({Internal.class, Static.class})
  private String targetSpaceId;
  @JsonView({Internal.class, Static.class})
  private SpatialFilter spatialFilter;
  @JsonView({Internal.class, Static.class})
  private PropertiesQuery propertyFilter;
  @JsonView({Internal.class, Static.class})
  private Ref sourceVersionRef;
  @JsonView({Internal.class, Static.class})
  private int[] threadInfo = {0,1};  // [threadId, threadCount]

  //Some internal caching fields:
  @JsonView({Internal.class, Static.class}) //TODO: Remove static
  private double overallNeededAcus = -1;
  @JsonView({Internal.class, Static.class}) //TODO: Remove static
  private long estimatedSourceFeatureCount = -1;
  @JsonView({Internal.class, Static.class}) //TODO: Remove static
  private long estimatedTargetFeatureCount = -1;
  @JsonView({Internal.class, Static.class}) //TODO: Remove static
  private int estimatedSeconds = -1;
  @JsonView({Internal.class, Static.class}) //TODO: Remove static
  private long targetVersion = 0;

  public static double calculateNeededCopyAcus(long nrFeatureSource) {
    final double maxAcus = 5;
    int ftBlock = 20000;

    double neededAcus = (nrFeatureSource <= ftBlock ? 1.0 : (nrFeatureSource / ftBlock) * 0.5 + 0.5);

    return Math.min(neededAcus, maxAcus);
  }

  public static int calculateCopyTimeInSeconds(long nrFeatureSource, long featureCountInTarget) {
    // ~1min per 20T feature,
    int oneMinute = 60, featuresPerBlock = 20000, startUp = oneMinute,
        calcSecs = (nrFeatureSource <= featuresPerBlock ? oneMinute : ((int) (nrFeatureSource / featuresPerBlock)) * oneMinute) + startUp;

    if (featureCountInTarget > featuresPerBlock) // ~ copy into nonempty space => add additional 1/2 amount of calculated
      calcSecs *= 1.5;

    return calcSecs;
  }


  public SpatialFilter getSpatialFilter() {
    return spatialFilter;
  }

  public void setSpatialFilter(SpatialFilter spatialFilter) {
    this.spatialFilter = spatialFilter;
  }

  public CopySpace withSpatialFilter(SpatialFilter spatialFilter) {
    setSpatialFilter(spatialFilter);
    return this;
  }

  public PropertiesQuery getPropertyFilter() {
    return propertyFilter;
  }

  public void setPropertyFilter(PropertiesQuery propertyFilter) {
    this.propertyFilter = propertyFilter;
  }

  public CopySpace withPropertyFilter(PropertiesQuery propertyFilter){
    setPropertyFilter(propertyFilter);
    return this;
  }

  public Ref getSourceVersionRef() {
    return sourceVersionRef;
  }

  public void setSourceVersionRef(Ref sourceVersionRef) {
    this.sourceVersionRef = sourceVersionRef;
  }

  public CopySpace withSourceVersionRef(Ref sourceVersionRef) {
    setSourceVersionRef(sourceVersionRef);
    return this;
  }

  public String getTargetSpaceId() {
    return targetSpaceId;
  }

  public void setTargetSpaceId(String targetSpaceId) {
    this.targetSpaceId = targetSpaceId;
  }

  public CopySpace withTargetSpaceId(String targetSpaceId) {
    setTargetSpaceId(targetSpaceId);
    return this;
  }

  public int[] getThreadInfo() {
    return threadInfo;
  }

  public void setThreadInfo(int[] threadInfo) {
    this.threadInfo = threadInfo;
  }

  public CopySpace withThreadInfo(int[] threadInfo) {
    setThreadInfo( threadInfo );
    return this;
  }

  private long getTargetVersion() {
    return targetVersion;
  }

  private void setTargetVersion(long targetVersion) {
    this.targetVersion = targetVersion;
  }

  //TODO: Remove testing code. Provide mocked version (model-based)input during tests instead
  public CopySpace withTargetVersion(long version) {
    setTargetVersion(version);
    return this;
  }

  @Override
  public List<Load> getNeededResources() {
    try {
      List<Load> expectedLoads = new ArrayList<>();
      Space sourceSpace = space();
      Space targetSpace = targetSpace();

      expectedLoads.add(new Load().withResource(loadDatabase(targetSpace.getStorage().getId(), WRITER))
          .withEstimatedVirtualUnits(calculateNeededAcus()));

      boolean isRemoteCopy = isRemoteCopy();

      if (isRemoteCopy)
        expectedLoads.add(new Load().withResource(dbReader()).withEstimatedVirtualUnits(calculateNeededAcus()));

      logger.info("[{}] #getNeededResources() isRemoteCopy={} {} -> {}", getGlobalStepId(), isRemoteCopy,
          sourceSpace.getStorage().getId(), targetSpace.getStorage().getId());

      return expectedLoads;
    }
    catch (WebClientException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getTimeoutSeconds() {
    return 2 * 3600; //TODO: Calculate using #getEstimatedExecutionSeconds()
  }

  private int getThreadPartitions() {
    return getThreadInfo() != null ? getThreadInfo()[1] : 1;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    if (estimatedSeconds == -1 && getSpaceId() != null) {
      estimatedSeconds = calculateCopyTimeInSeconds(loadSourceFeatureCount() / getThreadPartitions(), loadTargetFeatureCount());
      logger.info("[{}] Copy estimatedSeconds {}", getGlobalStepId(), estimatedSeconds);
    }
    return estimatedSeconds;
  }

  private double calculateNeededAcus() {
    overallNeededAcus =  overallNeededAcus != -1
        ? overallNeededAcus
        : calculateNeededCopyAcus(loadSourceFeatureCount() / getThreadPartitions());
    return overallNeededAcus;
  }

  @Override
  public String getDescription() {
    return "Copy space " + getSpaceId() + " to space " + getTargetSpaceId();
  }

  @Override
  public boolean validate() throws ValidationException {
    super.validate();

    if (getTargetSpaceId() == null)
      throw new ValidationException("Target Id is missing!");
    if (getSpaceId().equalsIgnoreCase(getTargetSpaceId()))
      throw new ValidationException("Source = Target!");
    //TODO: Validate that versionRef is set

    //Validate source space is active / readable
    try {
      if (!space().isActive())
        throw new ValidationException("Source is not active! It is currently not possible to read from it.");
    }
    catch (WebClientException e) {
      throw new ValidationException("Error loading source resource " + getSpaceId(), e);
    }

    //Validate target space exists and is writable
    try {
      Space targetSpace = targetSpace();
      if (!targetSpace.isActive())
        throw new ValidationException("Target resource is not active! It is currently not possible to write into it.");
      if (targetSpace.isReadOnly())
        throw new ValidationException("Target resource is read-only!");
    }
    catch (WebClientException e) {
      throw new ValidationException("Error loading target resource " + getTargetSpaceId(), e);
    }

    //FIXME: Enforce sourceVersionRef to be always set by the compiler
    sourceVersionRef = sourceVersionRef == null ? new Ref("HEAD") : sourceVersionRef;

    return true;
  }

  private long loadSourceFeatureCount() {
    if (estimatedSourceFeatureCount < 0) {
      try {
        estimatedSourceFeatureCount = loadSpaceStatistics(getSpaceId(), space().getExtension() != null ? EXTENSION : null)
            .getCount().getValue();
      }
      catch (WebClientException e) {
        throw new StepException("Error loading the source feature count", e).withRetryable(true);
      }
    }
    return estimatedSourceFeatureCount;
  }

  private long loadTargetFeatureCount() {
    if (estimatedTargetFeatureCount < 0) {
      try {
        estimatedTargetFeatureCount = loadSpaceStatistics(getSpaceId(), space().getExtension() != null ? EXTENSION : null)
            .getCount().getValue();
      }
      catch (WebClientException e) {
        throw new StepException("Error loading the target feature count", e).withRetryable(true);
      }
    }
    return estimatedTargetFeatureCount;
  }

  private Space targetSpace() throws WebClientException {
    return space(getTargetSpaceId());
  }

  long _getCreatedVersion() {
    for (InputFromOutput input : (List<InputFromOutput>)(List<?>) loadInputs(InputFromOutput.class))
      if (input.getDelegate() instanceof CreatedVersion f)
        return f.getVersion();

    return getTargetVersion(); // in case not version found return provided version from caller, used for mocking test cases
  }

  @Override
  public void execute(boolean resume) throws Exception {
    try {
      setTargetVersion(_getCreatedVersion());

      int threadId = getThreadInfo()[0],
          threadCount = getThreadInfo()[1];

      infoLog(resume ? STEP_RESUME : STEP_EXECUTE, this, "Start ImlCopy thread number: " + threadId + " / " + threadCount
          + "; target version: " + getTargetVersion());

      Space targetSpace = targetSpace();
      Database targetDb = loadDatabase(targetSpace.getStorage().getId(), WRITER);
      runReadQueryAsync(buildCopySpaceQuery(threadCount, threadId), targetDb,
          calculateNeededAcus() / threadCount, true);
    }
    catch (Exception e) {
      throw new StepException("Error iml-copy chunk-id " + getThreadInfo()[0] + "/" + getThreadInfo()[1], e)
          .withRetryable(true); //TODO: always retryable for now (later: handle errors properly here!)
    }
  }

  @Override
  protected void onAsyncSuccess() throws WebClientException, SQLException, TooManyResourcesClaimed, IOException {
    logger.info("[{}] AsyncSuccess Copy {} -> {}", getGlobalStepId(), getSpaceId() , getTargetSpaceId());
  }

  @Override
  protected void onStateCheck() {
    //@TODO: Implement
    logger.info("Copy - onStateCheck");
    getStatus().setEstimatedProgress(0.2f);
  }

  private boolean isRemoteCopy() throws WebClientException {
    return !space().getStorage().getId().equals(targetSpace().getStorage().getId());
  }

  private SQLQuery buildCopySpaceQuery(int threadCount, int threadId) throws WebClientException, QueryBuildingException,
      TooManyResourcesClaimed {
    String targetStorageId = targetSpace().getStorage().getId(),
        targetSchema = getSchema(loadDatabase(targetStorageId, WRITER)),
        targetTable = getRootTableName(targetSpace());

    final Map<String, Object> queryContext = createQueryContext(getId(), targetSchema, targetTable, targetSpace().getVersionsToKeep() > 1,
        null);

    SQLQuery contentQuery = buildCopyContentQuery(threadCount, threadId);

    if (isRemoteCopy())
      contentQuery = buildCopyQueryRemoteSpace(dbReader(), contentQuery);

    int maxBatchSize = 1000;
    //TODO: Do not use slow JSONB functions in the following query!
    //TODO: Simplify / deduplicate the following query
    return new SQLQuery("""
          WITH ins_data as
          (
            select
              write_features(
               jsonb_build_array(
                case deleted_flag when false then
                  jsonb_build_object('updateStrategy','{"onExists":null,"onNotExists":null,"onVersionConflict":null,"onMergeConflict":null}'::jsonb,
                                      'partialUpdates',false,
                                      'featureData', jsonb_build_object( 'type', 'FeatureCollection', 'features', jsonb_agg( iidata.feature ) ))
                else
                    jsonb_build_object('updateStrategy','{"onExists":"DELETE","onNotExists":"RETAIN"}'::jsonb,
                                      'partialUpdates',false,
                                      'featureIds', jsonb_agg( iidata.feature->'id' ) )
                end
              )::text,
                 'Modifications', iidata.author,false,${{versionToBeUsed}}
              ) as wfresult
            from
            (
             select ((row_number() over ())-1)/${{maxblksize}} as rn, 
                    idata.jsondata#>>'{properties,@ns:com:here:xyz,author}' as author, 
                    idata.jsondata || jsonb_build_object('geometry', (idata.geo)::json) as feature,
                    ((idata.jsondata#>>'{properties,@ns:com:here:xyz,deleted}') is not null) as deleted_flag
             from
             ( ${{contentQuery}} ) idata
            ) iidata
            group by rn, author, deleted_flag
          )
          select sum((wfresult::json->>'count')::bigint)::bigint into dummy_output from ins_data
        """).withContext(queryContext)
        .withQueryFragment("maxblksize", "" + maxBatchSize)
        .withQueryFragment("versionToBeUsed", "" + getTargetVersion())
        .withQueryFragment("contentQuery", contentQuery);
  }

  private SQLQuery buildCopyContentQuery(int threadCount, int threadId) throws WebClientException, QueryBuildingException,
      TooManyResourcesClaimed {

    Database db = !isRemoteCopy()
        ? loadDatabase(space().getStorage().getId(), WRITER)
        : dbReader();

    GetFeaturesByGeometryBuilder queryBuilder = new GetFeaturesByGeometryBuilder()
        .withDataSourceProvider(requestResource(db, 0));

    GetFeaturesByGeometryInput input = new GetFeaturesByGeometryInput(
        space().getId(),
        hubWebClient().loadConnector(space().getStorage().getId()).params,
        space().getExtension() != null ? space().resolveCompositeParams(superSpace()) : null,
        EXTENSION,
        space().getVersionsToKeep(),
        sourceVersionRef,
        spatialFilter != null ? spatialFilter.getGeometry() : null,
        spatialFilter != null ? spatialFilter.getRadius() : 0,
        spatialFilter != null && spatialFilter.isClip(),
        propertyFilter
    );

    final long VIZ_ID_COUNT = 0xfffffL + 1,
               blockRange = (long) Math.ceil((double) VIZ_ID_COUNT / (double) threadCount);

    final String VIZ_IDX_FKT   = "left(md5((''::text || i)), 5)";

    SQLQuery lowerBoundCondition = null,
             upperBoundCondition = null;

    if( threadId > 0)
     lowerBoundCondition =
      new SQLQuery("${{vizIdxFkt1}} >= #{vizLowerBound}")
          .withQueryFragment("vizIdxFkt1", VIZ_IDX_FKT)
          .withNamedParameter("vizLowerBound", String.format("%05x", threadId * blockRange));

    if( threadId < threadCount - 1)
     upperBoundCondition =
       new SQLQuery("${{vizIdxFkt2}} < #{vizUpperBound}")
           .withQueryFragment("vizIdxFkt2", VIZ_IDX_FKT)
           .withNamedParameter("vizUpperBound", String.format("%05x", (threadId + 1) * blockRange));

    if(lowerBoundCondition != null || upperBoundCondition != null)
    {
     SQLQuery additionalFilterFragment = new SQLQuery( (lowerBoundCondition != null && upperBoundCondition != null) ? "${{Bound1}} AND ${{Bound2}}" : "${{Bound1}}" );

     if(lowerBoundCondition != null)
      additionalFilterFragment.withQueryFragment("Bound1", lowerBoundCondition);

     if(upperBoundCondition != null)
      additionalFilterFragment.withQueryFragment(lowerBoundCondition != null ? "Bound2" : "Bound1", upperBoundCondition);

     queryBuilder.withAdditionalFilterFragment(additionalFilterFragment);
    }

    return queryBuilder
        //.withSelectionOverride(new SQLQuery("jsondata, author, operation"))
        //TODO: with author, operation provided in selection the parsing of those values in buildCopySpaceQuery would be obsolete
        .buildQuery(input);
  }
}
