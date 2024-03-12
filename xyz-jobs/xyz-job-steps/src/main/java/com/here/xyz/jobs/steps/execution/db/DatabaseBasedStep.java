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

package com.here.xyz.jobs.steps.execution.db;

import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.FAILURE_CALLBACK;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.SUCCESS_CALLBACK;
import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.READER;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.resources.ExecutionResource;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.models.hub.Space.Internal;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@JsonSubTypes({
    @JsonSubTypes.Type(value = SpaceBasedStep.class)
})
public abstract class DatabaseBasedStep<T extends DatabaseBasedStep> extends LambdaBasedStep<T> {
  private static final Logger logger = LogManager.getLogger();
  private double claimedAcuLoad;
  @JsonView(Internal.class)
  private List<RunningQuery> runningQueries = new ArrayList<>();
  private Map<Database, DataSourceProvider> usedDataSourceProviders;

  @Override
  public abstract void execute() throws Exception;

  @Override
  protected final void onRuntimeShutdown() {
    super.onRuntimeShutdown();
    if (usedDataSourceProviders != null)
      usedDataSourceProviders.forEach((db, dataSourceProvider) -> {
        try {
          dataSourceProvider.close();
        }
        catch (Exception e) {
          logger.error("Error closing connections for database " + db.getName(), e);
        }
      });
  }

  protected final void runReadQuery(SQLQuery query, Database db, double estimatedMaxAcuLoad) throws TooManyResourcesClaimed, SQLException {
    runReadQuery(query, db, estimatedMaxAcuLoad, rs -> null);
  }

  protected final <R> R runReadQuery(SQLQuery query, Database db, double estimatedMaxAcuLoad, ResultSetHandler<R> resultSetHandler)
      throws TooManyResourcesClaimed, SQLException {
    return (R) executeQuery(query, db, estimatedMaxAcuLoad, resultSetHandler, false, true);
  }

  protected final <R> R runReadQuerySync(SQLQuery query, Database db, double estimatedMaxAcuLoad, ResultSetHandler<R> resultSetHandler)
      throws SQLException, TooManyResourcesClaimed {
    return (R) executeQuery(query, db, estimatedMaxAcuLoad, resultSetHandler, false, false);
  }

  protected final int runWriteQuery(SQLQuery query, Database db, double estimatedMaxAcuLoad) throws TooManyResourcesClaimed, SQLException {
    return (int) executeQuery(query, db, estimatedMaxAcuLoad, null, true, true);
  }

  protected final int runWriteQuerySync(SQLQuery query, Database db, double estimatedMaxAcuLoad) throws TooManyResourcesClaimed,
      SQLException {
    return (int) executeQuery(query, db, estimatedMaxAcuLoad, null, true, false);
  }

  protected final int[] runBatchWriteQuerySync(SQLQuery query, Database db, double estimatedMaxAcuLoad) throws TooManyResourcesClaimed,
          SQLException {
    return (int[]) executeQuery(query, db, estimatedMaxAcuLoad, null, true, false);
  }

  private Object executeQuery(SQLQuery query, Database db, double estimatedMaxAcuLoad, ResultSetHandler<?> resultSetHandler,
      boolean isWriteQuery, boolean async) throws TooManyResourcesClaimed, SQLException {
    if (async)
      query = wrapQuery(query).withAsync(true);
    runningQueries.add(new RunningQuery(query.getQueryId(), db.getName(), db.getRole()));

    if (query.isBatch() && isWriteQuery)
      return query.writeBatch(requestResource(db, estimatedMaxAcuLoad));

    return isWriteQuery
            ? query.write(requestResource(db, estimatedMaxAcuLoad))
            : query.run(requestResource(db, estimatedMaxAcuLoad), resultSetHandler);
  }

  /**
   * Wraps the provided SQL query provided by the step implementation into an outer query which takes care about
   * success- / error-handling.
   * That is reporting success on the completion of the query to the asynchronous step implementation or reporting
   * the failure on exception by sending a {@link LambdaStepRequest} of according type back to the Job framework's Step Lambda Function.
   *
   * Invoking the Lambda Function from within the database is done using the <i>aws_lambda</i> plugin.
   * See: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL-Lambda.html
   *
   * @see LambdaBasedStep
   * @param stepQuery The query that was provided by the step implementation of the subclass
   * @return The wrapped query. A query that takes care of reporting the state back to this implementation asynchronously.
   */
  private SQLQuery wrapQuery(SQLQuery stepQuery) {
    return new SQLQuery("""
        DO $$
        BEGIN
          ${{stepQuery}};
          ${{successCallback}}
          EXCEPTION WHEN OTHERS THEN
                ${{failureCallback}}
        END$$;
        """)
        .withQueryFragment("stepQuery", stepQuery)
        .withQueryFragment("successCallback", buildSuccessCallbackQuery())
        .withQueryFragment("failureCallback", buildFailureCallbackQuery());
  }

  protected SQLQuery buildSuccessCallbackQuery() {
                              //TODO: De-activated when running locally for now
    return new SQLQuery("--PERFORM aws_lambda.invoke(aws_commons.create_lambda_function_arn('${{lambdaArn}}'), '${{successRequestBody}}'::json, 'Event');")
        .withQueryFragment("lambdaArn", getwOwnLambdaArn())
        .withQueryFragment("successRequestBody", new LambdaStepRequest().withType(SUCCESS_CALLBACK).withStep(this).serialize());
    //TODO: Re-use the request body for success / failure cases and simply inject the request type in the query
  }

  protected SQLQuery buildFailureCallbackQuery() {
    return new SQLQuery("""
        RAISE WARNING 'Step %.% failed with SQL state % and message %', '${{jobId}}', '${{stepId}}', SQLSTATE, SQLERRM;
        --PERFORM aws_lambda.invoke(aws_commons.create_lambda_function_arn('${{lambdaArn}}'), '${{failureRequestBody}}'::json, 'Event');
        """) //TODO: De-activated when running locally for now
        .withQueryFragment("jobId", getJobId())
        .withQueryFragment("stepId", getId())
        .withQueryFragment("lambdaArn", getwOwnLambdaArn())
        .withQueryFragment("failureRequestBody", new LambdaStepRequest().withType(FAILURE_CALLBACK).withStep(this).serialize());
    //TODO: Inject error message into failureRequestBody using SQLSTATE & SQLERRM
    //TODO: Re-use the request body for success / failure cases and simply inject the request type in the query
  }

  protected final String getSchema(Database db) {
    return db.getDatabaseSettings().getSchema();
  }

  @Override
  public void cancel() throws Exception {
    //Cancel all running queries
    runningQueries.stream().forEach(runningQuery -> {
      try {
        SQLQuery.killByQueryId(runningQuery.queryId, requestResource(Database.loadDatabase(runningQuery.dbName, runningQuery.dbRole),
            0), runningQuery.dbRole == READER);
      }
      catch (SQLException | TooManyResourcesClaimed e) {
        logger.error("Error cancelling running queries of step {}.{}. Following queries are probably still running: {}",
            getJobId(), getId(), runningQueries);
        //TODO: report failure?
      }
    });
  }

  @Override
  public AsyncExecutionState getExecutionState() throws UnknownStateException {
    //TODO: Check running state of all queries
    //TODO: If the hearbeat is called, but the query is not running anymore, it might be a failure => throw UnknownStateException
    return null;
  }

  @Override
  public final ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }

  protected final DataSourceProvider requestResource(Database db, double estimatedMaxAcuLoad) throws TooManyResourcesClaimed {
    Map<ExecutionResource, Double> neededResources = getAggregatedNeededResources();
    if (!neededResources.containsKey(db) || claimedAcuLoad + estimatedMaxAcuLoad > neededResources.get(db))
      throw new TooManyResourcesClaimed("Step " + getId() + " tried to claim further " + estimatedMaxAcuLoad + " ACUs, "
          + claimedAcuLoad + "/" + neededResources.get(db) + " have been claimed before.");

    claimedAcuLoad += estimatedMaxAcuLoad;

    if (usedDataSourceProviders == null)
      usedDataSourceProviders = new HashMap<>();
    DataSourceProvider dsp = usedDataSourceProviders.get(db);
    if (dsp == null)
      usedDataSourceProviders.put(db, dsp = db.getDataSources());
    return dsp;
  }

  private record RunningQuery(String queryId, String dbName, DatabaseRole dbRole) implements XyzSerializable {}
}
