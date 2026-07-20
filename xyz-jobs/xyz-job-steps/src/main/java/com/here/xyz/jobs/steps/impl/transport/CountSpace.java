/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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
import static com.here.xyz.jobs.steps.Step.Visibility.USER;
import static com.here.xyz.jobs.steps.impl.SpaceBasedStep.LogPhase.STEP_ON_ASYNC_SUCCESS;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.models.filters.SpatialFilter;
import com.here.xyz.jobs.steps.execution.db.Database;
import com.here.xyz.jobs.steps.impl.transport.tasks.inputs.CountInput;
import com.here.xyz.jobs.steps.impl.transport.tasks.outputs.ExportOutput;
import com.here.xyz.jobs.steps.impl.transport.tools.ContentPartitioning;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This step counts the features of a source space
 * With filters, it is possible to only count a subset from the source space.
 */

public class CountSpace extends TaskedSpaceBasedStep<CountSpace, CountInput, ExportOutput> {
  private static final Logger logger = LogManager.getLogger();

  public static final String FEATURECOUNT = "featurecount";
  public static final int MAX_THREAD_COUNT = 8;
  {
    threadCount = 1;
    addOutputSets(List.of(new OutputSet(FEATURECOUNT, USER, true)));
  }

  @JsonView({Internal.class, Static.class})
  private SpatialFilter spatialFilter;
  @JsonView({Internal.class, Static.class})
  private PropertiesQuery propertyFilter;

  public SpatialFilter getSpatialFilter() {
    return spatialFilter;
  }

  public void setSpatialFilter(SpatialFilter spatialFilter) {
    this.spatialFilter = spatialFilter;
  }

  public CountSpace withSpatialFilter(SpatialFilter spatialFilter) {
    setSpatialFilter(spatialFilter);
    return this;
  }

  public PropertiesQuery getPropertyFilter() {
    return propertyFilter;
  }

  public void setPropertyFilter(PropertiesQuery propertyFilter) {
    this.propertyFilter = propertyFilter;
  }

  public CountSpace withPropertyFilter(PropertiesQuery propertyFilter){
    setPropertyFilter(propertyFilter);
    return this;
  }

  /**
   * The number of parallel partitions the count is split into.
   * Configurable in the range [1, {@value #MAX_THREAD_COUNT}]; values outside are clamped.
   */
  @Override
  public void setThreadCount(int threadCount) {
    super.setThreadCount(Math.max(1, Math.min(MAX_THREAD_COUNT, threadCount)));
  }

  @Override
  protected boolean queryRunsOnWriter() throws WebClientException, SQLException, TooManyResourcesClaimed {
    return false;
  }

  @Override
  public String getDescription() {
    return "Count on " + getSpaceId();
  }

  @Override
  public boolean validate() throws ValidationException {
    super.validate();

    //TODO: Validate that versionRef is set

    //Validate source space is active / readable
    try {
      if (!space().isActive())
        throw new ValidationException("Source is not active! It is currently not possible to read from it.");
    }
    catch (WebClientException e) {
      throw new ValidationException("Error loading source resource " + getSpaceId(), e);
    }

    return true;
  }

  @Override
  protected boolean onAsyncFailure() {
    //TODO
    return super.onAsyncFailure();
  }

/*
  @Override
  protected void onStateCheck() {
    //@TODO: Implement
    logger.info("Count - onStateCheck");
    getStatus().setEstimatedProgress(0.2f);
  }
*/

  private SQLQuery buildCountSpaceQuery(Integer taskId, CountInput taskData) throws WebClientException, QueryBuildingException, TooManyResourcesClaimed {

    //The content query performs the exact COUNT for this partition and yields a single "nr_features" row.
    SQLQuery contentQuery = buildCountContentQuery(taskData.threadCount(), taskData.threadId());

    return new SQLQuery(
        """
          with idata as ( ${{contentQuery}} )
          select report_task_progress(
             #{lambda_function_arn},
             #{lambda_region},
             #{step_payload}::JSON->'step',
             #{taskId},
             jsonb_build_object(
                 'bytes', 0,
                 'rows', c.nr_features,
                 'files', 0,
                 'type', 'ExportOutput'
             )) from idata c
        """
        )
        //???.withContext(getQueryContext(schema))
        //???.withAsyncProcedure(false)
        .withNamedParameter("taskId", taskId)
        .withNamedParameter("step_payload", new LambdaStepRequest().withStep(this).serialize())
        .withNamedParameter("lambda_function_arn", getwOwnLambdaArn().toString())
        .withNamedParameter("lambda_region", getwOwnLambdaArn().getRegion())
        .withQueryFragment("contentQuery", contentQuery)
        //???.withQueryFragment("failureCallback",  buildFailureCallbackQuery().substitute().text().replaceAll("'", "''"))
        ;
  }

  private SQLQuery buildCountContentQuery(int threadCount, int threadId) throws WebClientException, QueryBuildingException, TooManyResourcesClaimed {

    Database db = dbReader();

    GetFeaturesByGeometryBuilder queryBuilder = new GetFeaturesByGeometryBuilder()
        .withDataSourceProvider(requestResource(db, 0));

    GetFeaturesByGeometryInput input = new GetFeaturesByGeometryInput(
        space().getId(),
        hubWebClient().loadConnector(space().getStorage().getId()).params,
        space().getExtension() != null ? space().resolveCompositeParams(superSpace()) : null,
        context,
        space().getVersionsToKeep(),
        minSpaceVersion,
        getVersionRef(),
        spatialFilter != null ? spatialFilter.getGeometry() : null,
        spatialFilter != null ? spatialFilter.getRadius() : 0,
        spatialFilter != null && spatialFilter.isClip(),
        propertyFilter
    );

    //Restrict this partition to its disjoint slice of the feature id space, so all partitions
    //together cover every feature exactly once and their partial counts can simply be summed.
    SQLQuery threadIdFilter = ContentPartitioning.buildThreadIdFilter(threadCount, threadId);
    if (threadIdFilter != null)
      queryBuilder.withAdditionalFilterFragment(threadIdFilter);

    String selectClause = space().getExtension() != null && context != EXTENSION ? "id" : "1::integer as id";

    //Only a minimal per-row projection (id / constant) is selected, so the exact COUNT does not
    //materialize full features. The COUNT is folded directly into the content query.
    SQLQuery featureQuery = queryBuilder.withSelectClauseOverride(new SQLQuery(selectClause)).buildQuery(input);

    return new SQLQuery("select count(1)::bigint as nr_features from ( ${{featureQuery}} ) ctable")
        .withQueryFragment("featureQuery", featureQuery);
  }

  @Override
  protected List<CountInput> createTaskItems(){
    //Split the count into #threadCount disjoint partitions that are processed in parallel.
    List<CountInput> tasks = new ArrayList<>();
    for (int threadId = 0; threadId < threadCount; threadId++)
      tasks.add(new CountInput("CountSpace", threadId, threadCount));
    return tasks;
  }

  @Override
  protected SQLQuery buildTaskQuery(Integer taskId, CountInput taskData, String failureCallback)
      throws QueryBuildingException, TooManyResourcesClaimed, WebClientException {
    return buildCountSpaceQuery( taskId, taskData );
  }

  @Override
  protected void processFinalizedTasks(List<FinalizedTaskItem<CountInput, ExportOutput>> finalizedTaskItems) throws IOException{
  long count = 0L;
    if(!finalizedTaskItems.isEmpty())
      count = finalizedTaskItems.stream().mapToLong(item -> item.output().rows()).sum();

    infoLog(STEP_ON_ASYNC_SUCCESS,  "Job Featurecount: count=" + count);

    FeatureStatistics featureStatistics = new FeatureStatistics()
            .withFeatureCount(count)
            .withByteSize(0);

    registerOutputs(List.of( featureStatistics ), FEATURECOUNT);
  }

  @Override
  public int getTimeoutSeconds() {
    return getEstimatedExecutionSeconds() * 5;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    return 60 * 15;
  }

  @Override
  protected double calculateOverallNeededACUs(){
    return 0.0;
  }

  @Override
  public List<Load> getNeededResources() {
    try {
      Load expectedLoad = new Load()
          .withResource(db())
          .withEstimatedVirtualUnits(calculateOverallNeededACUs());

      logger.info("[{}] getNeededResources {}", getGlobalStepId(), getSpaceId());

      return List.of(expectedLoad);
    }
    catch (WebClientException e) {
      //TODO: log error
      //TODO: is the step failed? Retry later? It could be a retryable error as the prior validation succeeded, depending on the type of HubWebClientException
      throw new RuntimeException(e);
    }
  }

}
