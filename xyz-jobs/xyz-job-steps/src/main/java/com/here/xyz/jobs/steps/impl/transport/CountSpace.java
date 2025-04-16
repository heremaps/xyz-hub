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

import static com.here.xyz.jobs.steps.Step.Visibility.USER;
import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.WRITER;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_ON_ASYNC_SUCCESS;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.buildTemporaryJobTableDropStatement;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.getTemporaryJobTableName;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.infoLog;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.jobs.steps.execution.db.Database;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.psql.query.GetFeaturesByGeometryBuilder;
import com.here.xyz.psql.query.GetFeaturesByGeometryBuilder.GetFeaturesByGeometryInput;
import com.here.xyz.psql.query.QueryBuilder.QueryBuildingException;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.sql.SQLException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This step counts the features of a source space
 * With filters, it is possible to only count a subset from the source space.
 */

public class CountSpace extends TaskedSpaceBasedStep<CountSpace> {
  private static final Logger logger = LogManager.getLogger();

  public static final String FEATURECOUNT = "featurecount";
  {
    setOutputSets(List.of(new OutputSet(FEATURECOUNT, USER, true)));
  }

  @JsonView({Internal.class, Static.class})
  private SpatialFilter spatialFilter;
  @JsonView({Internal.class, Static.class})
  private PropertiesQuery propertyFilter;

  private boolean realCount = true;

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
  protected void onAsyncSuccess() throws Exception {
    //Thread.sleep(1000 * 5);

    logger.info("[{}] AsyncSuccess *** Count {} ", getGlobalStepId(), getSpaceId());

    String schema = getSchema(db());

    Long count = runReadQuerySync(retrieveStatisticFromTaskAndStatisticTable(schema), db(WRITER),
            0, rs -> rs.next() ? rs.getLong("rows_uploaded") : 0L );

    infoLog(STEP_ON_ASYNC_SUCCESS, this, "Job Featurecount: count=" + count);

    FeatureStatistics featureStatistics = new FeatureStatistics()
        .withFeatureCount(count)
        .withByteSize(0);

    registerOutputs(List.of( featureStatistics ), FEATURECOUNT);

    infoLog(STEP_ON_ASYNC_SUCCESS, this, "Cleanup temporary table");
    runWriteQuerySync(buildTemporaryJobTableDropStatement(schema, getTemporaryJobTableName(getId())), db(WRITER), 0);

    //super.onAsyncSuccess();
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

  private SQLQuery buildCountSpaceQuery(Integer taskId) throws WebClientException, QueryBuildingException, TooManyResourcesClaimed {

    SQLQuery contentQuery = buildCountContentQuery();

    //TODO: contentQuery only needs to return ids, not the full feature

    return new SQLQuery(
        """
          with idata as ( select count(1) as nr_features from ( ${{contentQuery}} ) ctable )
          select report_task_progress(
             #{lambda_function_arn},
             #{lambda_region},
             #{step_payload}::JSON->'step',
             #{taskId},
             0,
             c.nr_features,
             0 ) from idata c 
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

  private SQLQuery buildCountContentQuery() throws WebClientException, QueryBuildingException, TooManyResourcesClaimed {

    Database db = dbReader();

    GetFeaturesByGeometryBuilder queryBuilder = new GetFeaturesByGeometryBuilder()
        .withDataSourceProvider(requestResource(db, 0));

    GetFeaturesByGeometryInput input = new GetFeaturesByGeometryInput(
        space().getId(),
        hubWebClient().loadConnector(space().getStorage().getId()).params,
        space().getExtension() != null ? space().resolveCompositeParams(superSpace()) : null,
        context,
        space().getVersionsToKeep(),
        getVersionRef(),
        spatialFilter != null ? spatialFilter.getGeometry() : null,
        spatialFilter != null ? spatialFilter.getRadius() : 0,
        spatialFilter != null && spatialFilter.isClip(),
        propertyFilter
    );

    return queryBuilder.buildQuery(input);
  }

  @Override
  protected int setInitialThreadCount(String schema) throws WebClientException, SQLException, TooManyResourcesClaimed {
    return 1;
  }

  @Override
  protected List<TaskData> createTaskItems(String schema){
    return List.of(new TaskData("CountSpace"));
  }

  @Override
  protected SQLQuery buildTaskQuery(String schema, Integer taskId, TaskData taskData)
      throws QueryBuildingException, TooManyResourcesClaimed, WebClientException {
    return buildCountSpaceQuery( taskId );
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
  public List<Load> getNeededResources() {
    try {
      Load expectedLoad = new Load()
          .withResource(db())
          .withEstimatedVirtualUnits(0.0);

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
