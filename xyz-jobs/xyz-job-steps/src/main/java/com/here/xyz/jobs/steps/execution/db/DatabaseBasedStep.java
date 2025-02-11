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

import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.ExecutionMode.ASYNC;
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
import com.here.xyz.util.db.datasource.DatabaseSettings;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@JsonSubTypes({
    @JsonSubTypes.Type(value = SpaceBasedStep.class)
})
public abstract class DatabaseBasedStep<T extends DatabaseBasedStep> extends LambdaBasedStep<T> {
  private static final Logger logger = LogManager.getLogger();
  private String ASYNC_STEP_ID = "asyncStepId";
  private double claimedAcuLoad;
  @JsonView(Internal.class)
  private List<RunningQuery> runningQueries = new ArrayList<>();

  @Override
  protected final void onRuntimeShutdown() {
    super.onRuntimeShutdown();
    Database.closeAll();
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
          .withTimeout(10)
          .withLabel(ASYNC_STEP_ID, getId());
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
   * "dummy_output" can by used for ignoring outputs. This is for example needed in CTE queries.
   *
   * @see LambdaBasedStep
   * @param stepQuery The query that was provided by the step implementation of the subclass
   * @return The wrapped query. A query that takes care of reporting the state back to this implementation asynchronously.
   */
  private SQLQuery wrapQuery(SQLQuery stepQuery) {

    SQLQuery wrappedQuery = new SQLQuery("""
        DO
        $wrapped$
        DECLARE
           dummy_output INTEGER;
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

    return stepQuery.getContext() == null ?  wrappedQuery : wrappedQuery.withContext(stepQuery.getContext());
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
             //TODO: Find a solution to retrieve also a given HINT
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

  protected SQLQuery buildCopyQueryRemoteSpace( Database remoteDb, SQLQuery contentQuery) {

      DatabaseSettings dbSettings = remoteDb.getDatabaseSettings();

      contentQuery =
       new SQLQuery(
          """
            select t.* 
            from 
            dblink( $icnt$ host=${{rmtHost}} dbname=${{rmtDb}} user=${{rmtUsr}} password=${{rmtPwd}} $icnt$, 
                    $iqry$ select jsondata, jsondata#>>'{properties,@ns:com:here:xyz,author}' as author, geo from ( ${{innerContentQuery}} ) rcopy $iqry$
                  ) 
            as t( jsondata jsonb, author text, geo text )
           """
       )
       .withQueryFragment("innerContentQuery", contentQuery)
       .withQueryFragment("rmtHost", dbSettings.getHost())
       .withQueryFragment("rmtDb", dbSettings.getDb())
       .withQueryFragment("rmtUsr", dbSettings.getUser())
       .withQueryFragment("rmtPwd", dbSettings.getPassword());

     return contentQuery;
  }

  @Override
  public void cancel() throws Exception {
    //Cancel all running queries
    List<String> failedCancellations = new LinkedList<>();
    Exception lastException = null;

    Set<Map.Entry<String, String>> dbIdentifiers = runningQueries.stream()
        .map(query -> Map.entry(query.dbName(), query.dbId()))
        .collect(Collectors.toSet());

    for (Map.Entry<String, String> entry : dbIdentifiers) {
      String dbName = entry.getKey();
      String dbId = entry.getValue();

      try {
        Database db = Database.loadDatabase(dbName, dbId);
        SQLQuery.killByLabel(ASYNC_STEP_ID, getId(), db.getDataSources(), db.getRole() == READER);
        logger.info("[{}] Asynchronous queries of step were successfully cancelled on {}:{}.", getGlobalStepId(), dbName, db.getRole().name());
      }
      catch (SQLException e) {
        logger.error("[{}] Error cancelling asynchronous queries of the step on {}.", getGlobalStepId(), dbName, e);
        failedCancellations.add(dbName);
        lastException = e;
        //Continue trying to cancel the remaining queries ...
      }
    }

    if (!failedCancellations.isEmpty()) {
      //TODO: check if we want to back report like in the uncommented code
      //runningQueries = failedCancellations;
      logger.error("[{}] Error cancelling running queries of the step. Some queries on db(s) {} are probably still running!",
          getGlobalStepId(), failedCancellations);
      throw new RuntimeException("Error cancelling running queries.", lastException);
    }
    else
      runningQueries = List.of();
  }

  @Override
  public AsyncExecutionState getExecutionState() throws UnknownStateException {
    logger.info("[{}] Checking execution state of the step ...", getGlobalStepId());
    Set<Map.Entry<String, String>> dbList = runningQueries.stream()
        .map(rq -> Map.entry(rq.dbName(), rq.dbId()))
        .collect(Collectors.toSet());
    boolean someQueryIsRunning = dbList
        .stream()
        .anyMatch(db -> {
          try {
            Database database = Database.loadDatabase(db.getKey(), db.getValue());
            return SQLQuery.isRunning(database.getDataSources(), database.getRole() == READER,
                ASYNC_STEP_ID, getId());
          }
          catch (SQLException e) {
            logger.error("[{}] Error while trying to check running queries of the step on {}.", getGlobalStepId(), db.getKey(), e);
            /*
            Ignore it if we cannot check the state for (one of) the queries (for now).
            In the worst case, this will lead to an UnknownStateException.
             */
            return false;
          }
        });

    //If the heartbeat is called, but no query is running anymore, it might be a failure, but it could also be a success => throw UnknownStateException
    if (!someQueryIsRunning)
      throw new UnknownStateException("No query is running anymore for step " + getGlobalStepId() + ". "
          + "Either the step is completed or failed.");

    //If there was at least one running query, the state should be still RUNNING
    return AsyncExecutionState.RUNNING;
  }

  @Override
  public ExecutionMode getExecutionMode() {
    return ASYNC;
  }

  protected final DataSourceProvider requestResource(Database db, double estimatedMaxAcuLoad) throws TooManyResourcesClaimed {
    Map<ExecutionResource, Double> neededResources = getAggregatedNeededResources();

    if (estimatedMaxAcuLoad != 0d &&
            (!neededResources.containsKey(db) || claimedAcuLoad + estimatedMaxAcuLoad > neededResources.get(db)))
      throw new TooManyResourcesClaimed("Step " + getId() + " tried to claim further " + estimatedMaxAcuLoad + " ACUs, "
          + claimedAcuLoad + "/" + neededResources.get(db) + " have been claimed before on [" + db.getName() +
              ":" + db.getRole() + "]" );

    claimedAcuLoad += estimatedMaxAcuLoad;
    return db.getDataSources();
  }

  private record RunningQuery(@JsonProperty("queryId") String queryId, @JsonProperty("dbName") String dbName,
                              @JsonProperty("dbId") String dbId) implements XyzSerializable {}
}
