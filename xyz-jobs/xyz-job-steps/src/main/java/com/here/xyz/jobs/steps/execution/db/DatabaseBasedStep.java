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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.resources.ExecutionResource;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
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

  protected final void runReadQueryAsync(SQLQuery query, Database db, double estimatedMaxAcuLoad) throws TooManyResourcesClaimed, SQLException {
    runReadQueryAsync(query, db, estimatedMaxAcuLoad, true);
  }

  protected final void runReadQueryAsync(SQLQuery query, Database db, double estimatedMaxAcuLoad, boolean withCallbacks)
      throws TooManyResourcesClaimed, SQLException {
    executeQuery(query, db, estimatedMaxAcuLoad, rs -> null, false, true, withCallbacks);
  }

  protected final <R> R runReadQuerySync(SQLQuery query, Database db, double estimatedMaxAcuLoad, ResultSetHandler<R> resultSetHandler)
      throws SQLException, TooManyResourcesClaimed {
    return (R) executeQuery(query, db, estimatedMaxAcuLoad, resultSetHandler, false, false, false);
  }

  protected final int runWriteQueryAsync(SQLQuery query, Database db, double estimatedMaxAcuLoad) throws TooManyResourcesClaimed, SQLException {
    return runWriteQueryAsync(query, db, estimatedMaxAcuLoad, true);
  }

  protected final int runWriteQueryAsync(SQLQuery query, Database db, double estimatedMaxAcuLoad, boolean withCallbacks)
      throws TooManyResourcesClaimed, SQLException {
    return (int) executeQuery(query, db, estimatedMaxAcuLoad, null, true, true, withCallbacks);
  }

  protected final int runWriteQuerySync(SQLQuery query, Database db, double estimatedMaxAcuLoad) throws TooManyResourcesClaimed,
      SQLException {
    return (int) executeQuery(query, db, estimatedMaxAcuLoad, null, true, false, false);
  }

  protected final int[] runBatchWriteQuerySync(SQLQuery query, Database db, double estimatedMaxAcuLoad) throws TooManyResourcesClaimed,
      SQLException {
    return (int[]) executeQuery(query, db, estimatedMaxAcuLoad, null, true, false, false);
  }

  private Object executeQuery(SQLQuery query, Database db, double estimatedMaxAcuLoad, ResultSetHandler<?> resultSetHandler,
      boolean isWriteQuery, boolean async, boolean withCallbacks) throws TooManyResourcesClaimed, SQLException {
    query
        .withLabel("jobId", getJobId())
        .withLabel("stepId", getId());

    if (async) {
      query = (withCallbacks ? wrapQuery(query) : query)
          .withAsync(true)
          .withTimeout(10);
    }
    else if (query.getTimeout() == Integer.MAX_VALUE)
      query.setTimeout(300);

    Object result;
    if (query.isBatch() && isWriteQuery)
      result = query.writeBatch(requestResource(db, estimatedMaxAcuLoad));
    else
      result = isWriteQuery
          ? query.write(requestResource(db, estimatedMaxAcuLoad))
          : query.run(requestResource(db, estimatedMaxAcuLoad), resultSetHandler);

    if (async)
      runningQueries.add(new RunningQuery(query.getQueryId(), db.getName(), db.getId()));

    return result;
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
        DO
        $wrapped$
        BEGIN
          ${{stepQuery}};
          ${{successCallback}}
          EXCEPTION WHEN OTHERS THEN
            ${{failureCallback}}
        END
        $wrapped$;
        """) //TODO: Move RAISE WARNING expression back to generic #buildFailureCallbackQuery() to ensure consistent error reporting & logging across all step implementations
        .withQueryFragment("jobId", getJobId())
        .withQueryFragment("stepId", getId())
        .withQueryFragment("stepQuery", stepQuery)
        .withQueryFragment("successCallback", buildSuccessCallbackQuery())
        .withQueryFragment("failureCallback", buildFailureCallbackQuery());
  }

  protected final SQLQuery buildSuccessCallbackQuery() {
    SQLQuery lambdaSuccessInvoke = isSimulation
        ? new SQLQuery("PERFORM 'Success callback will be simulated';")
        : new SQLQuery("PERFORM aws_lambda.invoke(aws_commons.create_lambda_function_arn('${{lambdaArn}}', '${{lambdaRegion}}'), '${{successRequestBody}}'::JSON, 'Event');")
            .withQueryFragment("lambdaArn", getwOwnLambdaArn().toString()) //TODO: Use named params instead of query fragments
            .withQueryFragment("lambdaRegion", getwOwnLambdaArn().getRegion())
            //TODO: Re-use the request body for success / failure cases and simply inject the request type in the query
            .withQueryFragment("successRequestBody", new LambdaStepRequest().withType(SUCCESS_CALLBACK).withStep(this).serialize());

    return new SQLQuery("""
        DO
        $success$
        BEGIN
          ${{performLambdaSuccessInvoke}}
        END
        $success$;
        """)
        .withQueryFragment("performLambdaSuccessInvoke", lambdaSuccessInvoke);
  }

  protected final SQLQuery buildFailureCallbackQuery() {
    //TODO: Handle ErrorCause
    SQLQuery lambdaFailureInvoke = isSimulation
        ? new SQLQuery("PERFORM 'Failure callback will be simulated';")
        : new SQLQuery("""             
             PERFORM aws_lambda.invoke(aws_commons.create_lambda_function_arn('${{lambdaArn}}', '${{lambdaRegion}}'),
                  jsonb_set(
                    '${{failureRequestBody}}'::JSONB,
                    '{step, status}',
                    ('${{failureRequestBody}}'::JSONB->'step'->'status' || format('{"errorCode": "%1$s", "errorMessage": "%2$s"}', SQLSTATE, SQLERRM)::JSONB),
                    true
                  )::JSON, 'Event');
            """) //TODO: Inject fields directly in memory rather than writing and deserializing new JSON object
            .withQueryFragment("lambdaArn", getwOwnLambdaArn().toString())  //TODO: Use named params instead of query fragments
            .withQueryFragment("lambdaRegion", getwOwnLambdaArn().getRegion())
            //TODO: Re-use the request body for success / failure cases and simply inject the request type in the query
            .withQueryFragment("failureRequestBody", new LambdaStepRequest().withType(FAILURE_CALLBACK).withStep(this).serialize());

    //TODO: Re-add the following when the issue with raising notices / warnings is fixed
    //RAISE WARNING 'Step %.% failed with SQL state % and message %', '${{jobId}}', '${{stepId}}', SQLSTATE, SQLERRM;
    return new SQLQuery("""
            ${{performLambdaFailureInvoke}}
        """)
        .withQueryFragment("performLambdaFailureInvoke", lambdaFailureInvoke);
  }

  protected final String getSchema(Database db) {
    return db.getDatabaseSettings().getSchema();
  }

  @Override
  public void cancel() throws Exception {
    //Cancel all running queries
    List<RunningQuery> failedCancellations = new LinkedList<>();
    Exception lastException = null;
    for (RunningQuery runningQuery : runningQueries) {
      try {
        Database db = Database.loadDatabase(runningQuery.dbName, runningQuery.dbId);
        SQLQuery.killByQueryId(runningQuery.queryId, requestResource(db, 0), db.getRole() == READER);
      }
      catch (SQLException e) {
        logger.error("Error cancelling query {} of step {}.", runningQuery.queryId, getGlobalStepId(), e);
        failedCancellations.add(runningQuery);
        lastException = e;
        //Continue trying to cancel the remaining queries ...
      }
    }

    if (failedCancellations.size() > 0) {
      runningQueries = failedCancellations;
      logger.error("Error cancelling running queries of step {}. The following queries are probably still running: {}",
          getGlobalStepId(), runningQueries);
      throw new RuntimeException("Error cancelling running queries.", lastException);
    }
    else
      runningQueries = List.of();
  }

  @Override
  public AsyncExecutionState getExecutionState() throws UnknownStateException {
    logger.info("Checking execution state of step {} ...", getGlobalStepId());
    boolean someQueryIsRunning = runningQueries
        .stream()
        .anyMatch(runningQuery -> {
          try {
            return SQLQuery.isRunning(requestResource(Database.loadDatabase(runningQuery.dbName, runningQuery.dbId),0), false,
                runningQuery.queryId);
          }
          catch (SQLException | TooManyResourcesClaimed e) {
            /*
            Ignore it if we cannot check the state for (one of) the queries (for now).
            In the worst case, this will lead to an UnknownStateException.
             */
            return false;
          }
        });

    //If the heartbeat is called, but the query is not running anymore, it might be a failure => throw UnknownStateException
    if (!someQueryIsRunning)
      throw new UnknownStateException("No query is running anymore for step " + getGlobalStepId() + ". "
          + "Either the step is completed or failed.");

    //If there was at least one running query, the state should be still RUNNING
    return AsyncExecutionState.RUNNING;
  }

  @Override
  public ExecutionMode getExecutionMode() {
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

  private record RunningQuery(@JsonProperty("queryId") String queryId, @JsonProperty("dbName") String dbName,
                              @JsonProperty("dbId") String dbId) implements XyzSerializable {}
}
