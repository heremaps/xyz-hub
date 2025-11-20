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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.jobs.JobClientInfo;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.ProcessUpdate;
import com.here.xyz.jobs.steps.execution.StepException;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.impl.transport.tasks.TaskPayload;
import com.here.xyz.jobs.steps.impl.transport.tasks.TaskProgress;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.query.QueryBuilder.QueryBuildingException;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.pg.FeatureWriterQueryBuilder.FeatureWriterQueryContextBuilder;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.web.XyzWebClient;
import com.here.xyz.util.web.XyzWebClient.ErrorResponseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.WRITER;
import static com.here.xyz.jobs.steps.impl.SpaceBasedStep.LogPhase.STEP_EXECUTE;
import static com.here.xyz.jobs.steps.impl.SpaceBasedStep.LogPhase.STEP_ON_ASYNC_SUCCESS;
import static com.here.xyz.jobs.steps.impl.SpaceBasedStep.LogPhase.STEP_ON_ASYNC_UPDATE;
import static com.here.xyz.jobs.steps.impl.SpaceBasedStep.LogPhase.UNKNOWN;
import static com.here.xyz.util.web.XyzWebClient.WebClientException;

/**
 * Abstract base class for space-based job steps that execute tasks in parallel.
 * <p>
 * Responsibilities:
 * <ul>
 *   <li>Manages task creation, execution, and progress tracking for distributed processing.</li>
 *   <li>Handles resource allocation and compute unit estimation.</li>
 *   <li>Provides hooks for subclasses to implement task-specific logic, SQL query building, and output processing.</li>
 *   <li>Supports asynchronous updates and finalization of tasks.</li>
 * </ul>
 *
 * @param <T> The concrete subclass type.
 * @param <I> The input payload type for each task.fail
 * @param <O> The output payload type for each task.
 */
public abstract class TaskedSpaceBasedStep<T extends TaskedSpaceBasedStep, I extends TaskPayload, O extends TaskPayload>
        extends SpaceBasedStep<T> {
  private static final String JOB_DATA_PREFIX = "job_data_";
  private static final Logger logger = LogManager.getLogger();

  @JsonView({Internal.class, Static.class})
  protected int threadCount = 8;

  @JsonView({Internal.class, Static.class})
  private boolean noTasksCreated = false;

  @JsonView({Internal.class, Static.class})
  protected int taskItemCount = -1;
  @JsonView({Internal.class, Static.class})
  protected SpaceContext context;
  @JsonView({Internal.class, Static.class})
  protected Ref versionRef;
  @JsonView({Internal.class, Static.class})
  protected long spaceCreatedAt;

  public Ref getVersionRef() {
    return versionRef;
  }

  public void setVersionRef(Ref versionRef) {
    this.versionRef = versionRef;
  }

  public T withVersionRef(Ref versionRef) {
    setVersionRef(versionRef);
    return (T) this;
  }

  public long getSpaceCreatedAt() {
    return spaceCreatedAt;
  }

  public void setSpaceCreatedAt(long spaceCreatedAt) {
    this.spaceCreatedAt = spaceCreatedAt;
  }

  public T withSpaceCreatedAt(long spaceCreatedAt) {
    setSpaceCreatedAt(spaceCreatedAt);
    return (T) this;
  }

  public SpaceContext getContext() {
    return this.context;
  }

  public void setContext(SpaceContext context) {
    this.context = context;
  }

  public T withContext(SpaceContext context) {
    setContext(context);
    return (T) this;
  }

  public int getThreadCount() {
    return threadCount;
  }

  public void setThreadCount(int threadCount) {
    this.threadCount = threadCount;
  }

  public T withThreadCount(int threadCount) {
    setThreadCount(threadCount);
    return (T) this;
  }

  /**
   * Determines whether the SQL query for a task should run on the writer database.
   *
   * Implementations should return {@code true} if the query requires write access (e.g., modifies data),
   * or {@code false} if it can run on a read-only replica.
   *
   * @return {@code true} if the query must run on the writer DB, {@code false} otherwise.
   * @throws WebClientException If an error occurs while interacting with the web client.
   * @throws SQLException If a database access error occurs.
   * @throws TooManyResourcesClaimed If too many resources are claimed during the process.
   */
  protected abstract boolean queryRunsOnWriter()
          throws WebClientException, SQLException, TooManyResourcesClaimed;

  /**
   * Creates generic task items in the taskAndStatistic table.
   * {@code createTaskItems} is used to generate the task data for each thread.
   *
   * @return The number of task items created.
   * @throws WebClientException If an error occurs while interacting with the web client.
   * @throws SQLException If an error occurs while executing SQL queries.
   * @throws TooManyResourcesClaimed If too many resources are claimed during the process.
   * @throws QueryBuildingException If an error occurs while building the SQL query.
   */
  protected abstract List<I> createTaskItems()
          throws WebClientException, SQLException, TooManyResourcesClaimed, QueryBuildingException;

  /**
   * Builds the SQL query for processing a specific task.
   * <p>
   * Implementations should construct and return the SQLQuery required to execute the task logic
   * for the given schema, task ID, and input payload.
   * Please review the reference SQL function implementation {@code perform_example_task(..)} in transport.sql.
   * </p>
   *
   * @param taskId The unique identifier of the task.
   * @param taskInput The input payload for the task.
   * @return The SQLQuery to execute for the task.
   * @throws QueryBuildingException If an error occurs while building the SQL query.
   * @throws TooManyResourcesClaimed If too many resources are claimed during query construction.
   * @throws WebClientException If an error occurs while interacting with the web client.
   * @throws InvalidGeometryException If the input contains invalid geometry data.
   */
  protected abstract SQLQuery buildTaskQuery(Integer taskId, I taskInput, String failureCallback)
          throws QueryBuildingException, TooManyResourcesClaimed, WebClientException, InvalidGeometryException;

  /**
   * Processes the collected task outputs after all parallel tasks have finalized.
   * <p>
   * This hook is invoked during asynchronous success handling to allow subclasses
   * to aggregate, transform, persist, or derive final statistics from the per-task
   * output payloads.
   * </p>
   *
   * <ul>
   *   <li>Called exactly once after all task rows are finalized.</li>
   *   <li>Implementations may perform I/O (e.g. serialization, storage, enrichment).</li>
   *   <li>May produce aggregated results for outputs.</li>
   * </ul>
   *
   * @param taskOutputs The list of per-task output payloads of type {@code O}.
   * @throws IOException If an I/O error occurs while processing or persisting outputs.
   */
  protected abstract void processOutputs(List<O> taskOutputs)
          throws IOException, WebClientException;

  /**
   * Calculates the overall needed ACUs (Amazon Compute Units) for the job step based on the provided data.
   *
   * Implementations should estimate the total compute resources required to process all tasks,
   * using the input data (e.g. statistics, item count, or other relevant metrics).
   * The implementations should use this method in their getNeededResources() methods.
   *
   * @return The estimated total ACUs required for the job step.
   */
  protected abstract double calculateOverallNeededACUs();

  /**
   * Performs initial setup before starting the job step execution.
   * <p>
   * This method can be overridden by subclasses to implement any required initialization logic,
   * such as preparing resources, validating prerequisites, or configuring the environment.
   * </p>
   *
   * @throws SQLException If a database access error occurs.
   * @throws TooManyResourcesClaimed If too many resources are claimed during setup.
   * @throws WebClientException If an error occurs while interacting with the web client.
   */
  protected void initialSetup() throws SQLException, TooManyResourcesClaimed, WebClientException {};

  /**
   * Performs final cleanup after all tasks have completed.
   * <p>
   * This method can be overridden by subclasses to implement resource deallocation,
   * temporary data removal, or any other necessary cleanup logic at the end of the job step.
   * </p>
   *
   * @throws WebClientException If an error occurs while interacting with the web client.
   * @throws SQLException If a database access error occurs.
   * @throws TooManyResourcesClaimed If too many resources are claimed during cleanup.
   */
  protected void finalCleanUp() throws WebClientException, SQLException, TooManyResourcesClaimed {};

  /**
   * Prepares the process by resolving the version reference to an actual version.
   *
   * @param owner The owner of the job.
   * @param ownerAuth The authentication information of the job owner.
   * @throws ValidationException If the version reference is null or cannot be resolved.
   */
  @Override
  public void prepare(String owner, JobClientInfo ownerAuth) throws ValidationException {
    logger.info("[{}] Preparing step ...", getGlobalStepId());

    if (versionRef == null)
      throw new ValidationException("Version ref is required.");

    try {
      try {
        setVersionRef(hubWebClient().resolveRef(getSpaceId(), context, versionRef));
        setSpaceCreatedAt(space(true).getCreatedAt());
      }
      catch (ErrorResponseException e) {
        handleErrorResponse(e);
      }
    }
    catch (WebClientException e) {
      throw handleWebClientException("Unable to resolve the provided version ref \"" + versionRef + "\" of " + getSpaceId() + ": "
          + e.getMessage(), e);
    }
    catch (StepException e) {
      throw new ValidationException("Unable to resolve the provided version ref \"" + versionRef + "\" of " + getSpaceId() + ": "
          + e.getMessage(), e);
    }

    logger.info("[{}] Completed preparation of step.", getGlobalStepId());
  }

  @Override
  public boolean validate() throws ValidationException {
    super.validate();
    //Validate versionRef
    if (this.versionRef == null)
      return true;

    try{
      //TODO: Discuss if we want move this to SpaceBaseStep impl
      // Can be moved to SpaceBasedStep#validateRef
      StatisticsResponse statistics = loadSpaceStatistics(getSpaceId(), context, true);

      Long minSpaceVersion = statistics.getMinVersion().getValue();
      Long maxSpaceVersion = statistics.getMaxVersion().getValue();

      if (this.versionRef.isSingleVersion()) {
        if (this.versionRef.getVersion() < minSpaceVersion)
          throw new ValidationException("Invalid VersionRef! Version is smaller than min available version '" +
                  minSpaceVersion + "'!");
        if (this.versionRef.getVersion() > maxSpaceVersion)
          throw new ValidationException("Invalid VersionRef! Version is higher than max available version '" +
                  maxSpaceVersion + "'!");
      } else if (this.versionRef.isRange()) {
        if (this.versionRef.getStart().getVersion() < minSpaceVersion)
          throw new ValidationException("Invalid VersionRef! StartVersion is smaller than min available version '" +
                  minSpaceVersion + "'!");
        if (this.versionRef.getEnd().getVersion() > maxSpaceVersion)
          throw new ValidationException("Invalid VersionRef! EndVersion is higher than max available version '" +
                  maxSpaceVersion + "'!");
      }
    }catch (WebClientException e) {
      throw handleWebClientException("Error loading resource " + getSpaceId(), e);
    }
    return true;
  }

  private long resolveTag(String tag) throws ValidationException {
    try {
      return loadTag(getSpaceId(), tag).getVersion();
    } catch (WebClientException e) {
      throw handleWebClientException("Unable to resolve tag \"" + tag + "\" of " + getSpaceId(), e);
    }
  }

  private long resolveHead() throws ValidationException {
    try {
      return spaceStatistics(context, true).getMaxVersion().getValue();
    } catch (WebClientException e) {
      throw handleWebClientException("Unable to resolve HEAD version of " + getSpaceId(), e);
    }
  }

  private ValidationException handleWebClientException(String message, WebClientException e) throws ValidationException {
    if (e instanceof XyzWebClient.ErrorResponseException err && err.getStatusCode() == 428)
      throw new ValidationException(getSpaceId() + " is deactivated!", e);
    throw new ValidationException(message, e);
  }

  /**
   * Starts the initial tasks for the process based on the calculated thread count.
   *
   * @throws TooManyResourcesClaimed If too many resources are claimed during the process.
   * @throws QueryBuildingException If an error occurs while building the SQL query.
   * @throws WebClientException If an error occurs while interacting with the web client.
   * @throws SQLException If an error occurs while executing SQL queries.
   */
  private void startInitialTasks() throws TooManyResourcesClaimed,
          QueryBuildingException, WebClientException, SQLException, InvalidGeometryException {

    for (int i = 0; i < threadCount; i++) {
      TaskProgress taskProgressAndTaskItem = getTaskProgressAndTaskItem();
      if(taskProgressAndTaskItem.getTaskId() == -1)
        break;
      startTask(taskProgressAndTaskItem);
    }
  }

  /**
   * Starts a task for the given task progress and item.
   * <p>
   * If not all tasks are started (taskId != -1), this method calculates the required compute units (ACUs)
   * for the task, logs the start, and executes the task asynchronously.
   * </p>
   *
   * @param taskProgressAndItem The task progress and item details.
   * @throws TooManyResourcesClaimed If too many resources are claimed during the process.
   * @throws QueryBuildingException If an error occurs while building the SQL query.
   * @throws WebClientException If an error occurs while interacting with the web client.
   * @throws SQLException If an error occurs while executing SQL queries.
   * @throws InvalidGeometryException If the input contains invalid geometry data.
   */
  private void startTask(TaskProgress<I> taskProgressAndItem) throws TooManyResourcesClaimed,
          QueryBuildingException, WebClientException, SQLException, InvalidGeometryException {

    //if taskId is -1 all tasks are already started
    if (taskProgressAndItem.getTaskId() != -1) {
      BigDecimal overallAcusBD = BigDecimal.valueOf(calculateOverallNeededACUs());
      BigDecimal itemCountBD = BigDecimal.valueOf(taskItemCount);

      // Perform precise division
      BigDecimal perItemAcus = overallAcusBD.divide(itemCountBD, 30, RoundingMode.HALF_UP);

      infoLog(STEP_EXECUTE,  "Start task[" +taskProgressAndItem.getTaskId() +  "] with input: " + taskProgressAndItem.getTaskInput()
              + " " + perItemAcus.stripTrailingZeros().toPlainString()
              + "/" + overallAcusBD.stripTrailingZeros().toPlainString());

      //We can`t use the default callback here because we are reporting success during onAsyncUpdate in Java.
      //The failureCallback is still needed to report failures from the DB-side
      String failureCallback = buildFailureCallbackQuery().substitute().text().replaceAll("'", "''");
      runReadQueryAsync(buildTaskQuery(taskProgressAndItem.getTaskId(), (I) taskProgressAndItem.getTaskInput(), failureCallback),
                queryRunsOnWriter() ? dbWriter() : dbReader(), 0d/*perItemAcus.doubleValue()*/,  false);
    }
  }

  @Override
  public void execute(boolean resume) throws Exception {
    //The following code is running synchronously till the first task is getting started.
    String schema = getSchema(db());
    if (!resume) {
      initialSetup();
      List<I> taskDataList = createTaskItems();
      taskItemCount = taskDataList.size();
      insertTaskItemsInTaskTable(schema, this, taskDataList);
    }
    startInitialTasks();
    noTasksCreated = taskItemCount == 0;
  }

  /**
   * Handles asynchronous updates during the process. The invocation happens from the
   * database if a task is finished. If unstarted task are present the next one gets started.
   *
   * @param processUpdate The process update containing information about the progress.
   * @return {@code true} if all tasks are complete, {@code false} otherwise.
   * @throws RuntimeException If an unexpected error occurs during the update process.
   */
  @Override
  protected boolean onAsyncUpdate(ProcessUpdate processUpdate){
    try {
      //Update the task table and mark item as finalized
      SpaceBasedTaskUpdate update = (SpaceBasedTaskUpdate) processUpdate;
      updateTaskItemInTaskTable(update);

      infoLog(STEP_ON_ASYNC_UPDATE, "received progress update: "
              + processUpdate.serialize());
      TaskProgress taskProgressAndItem = getTaskProgressAndTaskItem();
      if (taskProgressAndItem.isComplete()) {
        infoLog(STEP_ON_ASYNC_UPDATE, "All tasks are finished." + taskProgressAndItem);

        //Collect outputs and process them
        processOutputs(collectOutputs());

        //Clean up temporary resources
        cleanUpDbResources();

        return true;
      }else {
        infoLog(STEP_ON_ASYNC_UPDATE, "Found existing tasks. Start new item:" + taskProgressAndItem);
        //If we are not finished, start the next task
        startTask(taskProgressAndItem);
        //Calculate progress and set it on the step's status
        getStatus().setEstimatedProgress((float) taskProgressAndItem.getFinalizedTasks() / (float) taskProgressAndItem.getTotalTasks());
      }

      return false;
    }catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected boolean onAsyncFailure() {
    try {
      //TODO: Inspect the error provided in the status and decide whether it is retryable (return-value)
      boolean isRetryable = false;

      if (!isRetryable)
        cleanUpDbResources();

      return isRetryable;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void cleanUpDbResources() throws WebClientException, SQLException, TooManyResourcesClaimed {
    infoLog(STEP_ON_ASYNC_SUCCESS,  "Cleanup temporary table");
    runWriteQuerySync(buildTemporaryJobTableDropStatement(getSchema(db()), getTemporaryJobTableName(getId())), db(WRITER), 0);

    infoLog(STEP_ON_ASYNC_SUCCESS,  "Executing cleanUp Hook");
    finalCleanUp();
  }

  private List<O> collectOutputs() throws SQLException, TooManyResourcesClaimed, WebClientException {
    return runReadQuerySync(retrieveTaskOutputsQuery(), db(WRITER), 0, rs -> {
      try {
        List<O> outputs = new ArrayList<>();
        while (rs.next()){
          String taskOutput = rs.getString("task_output");
          if(taskOutput != null){
            SpaceBasedTaskUpdate<O> update =
                    XyzSerializable.deserialize(taskOutput, SpaceBasedTaskUpdate.class);
            outputs.add(update.taskOutput);
          }else{
            infoLog(STEP_ON_ASYNC_SUCCESS,  "Empty task output found - skip.");
          }

        }
        return outputs;
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public AsyncExecutionState getExecutionState() throws UnknownStateException {
    if(noTasksCreated)
      return AsyncExecutionState.SUCCEEDED;
    return super.getExecutionState();
  }

  /**
   * Retrieves the task progress and an unstarted task item (if present) from the database.
   *
   * @return The task progress and task item details.
   * @throws WebClientException If an error occurs while interacting with the web client.
   * @throws SQLException If an error occurs while executing SQL queries.
   * @throws TooManyResourcesClaimed If too many resources are claimed during the process.
   */
  private TaskProgress getTaskProgressAndTaskItem() throws WebClientException, SQLException, TooManyResourcesClaimed {
    TaskProgress taskProgress;
    try {
      taskProgress = runReadQuerySync(retrieveTaskItemAndStatisticsQuery(getSchema(db(WRITER))), db(WRITER), 0,
              rs -> {
                if (!rs.next())
                  return null;
                try {
                  return new TaskProgress(rs.getInt("total"), rs.getInt("started"), rs.getInt("finalized"),
                          rs.getInt("task_id"), XyzSerializable.deserialize(rs.getString("task_input"), new TypeReference<I>() {
                  }));
                } catch (JsonProcessingException e) {
                  throw new RuntimeException("Can not deserialize task_input!", e);
                }
              });
    }catch (SQLException e){
      if(e.getSQLState().equalsIgnoreCase("42P01")) {
        //If we are here a failure already happened and the task table does not exist anymore
        //To avoid overriding the original failure we just log this and return a completed TaskProgress
        infoLog(UNKNOWN,  "Task table does not exist anymore. Ignore.");
        return new TaskProgress<>(-1);
      }
      throw e;
    }
    return taskProgress;
  }

  private static SQLQuery buildTaskTableStatement(String schema, Step step) {
    return new SQLQuery("""          
            CREATE TABLE IF NOT EXISTS ${schema}.${table}
            (
            	task_id SERIAL,
            	task_input JSONB,
            	task_output JSONB,
            	started BOOLEAN DEFAULT false,
            	finalized BOOLEAN DEFAULT false,
            	CONSTRAINT ${primaryKey} PRIMARY KEY (task_id)
            );
        """)
            //TODO: CHECK CONSTRAINT!!
            .withVariable("table", getTemporaryJobTableName(step.getId()))
            .withVariable("schema", schema)
            .withVariable("primaryKey", getTemporaryJobTableName(step.getId()) + "_primKey");
  }

  private void updateTaskItemInTaskTable(SpaceBasedTaskUpdate update) throws WebClientException, SQLException, TooManyResourcesClaimed {
    infoLog(STEP_ON_ASYNC_UPDATE,  "Update process table with: " + update.serialize());
    /** create update process table */
    runWriteQuerySync(
            buildUpdateTaskItemStatement(getSchema(db(WRITER)), this, update.taskId,
                    update,  true
            ), db(WRITER), 0);
  }

  private static SQLQuery buildUpdateTaskItemStatement(String schema, Step step, int taskId,
                                                       SpaceBasedTaskUpdate update, boolean finalized) {
    return new SQLQuery("""             
            UPDATE ${schema}.${table} t
                SET task_output = #{taskOutput}::JSONB,
                    finalized = #{finalized}
                WHERE task_id = #{taskId};
        """)
            .withVariable("schema", schema)
            .withVariable("table", getTemporaryJobTableName(step.getId()))
            .withNamedParameter("taskId", taskId)
            .withNamedParameter("taskOutput", XyzSerializable.serialize(update))
            .withNamedParameter("finalized", finalized); //future prove
  }

  private SQLQuery retrieveTaskItemAndStatisticsQuery(String schema) throws WebClientException {
    return new SQLQuery("SELECT total, started, finalized, task_id, task_input from get_task_item_and_statistics();")
            .withContext(getQueryContext(schema));
  }

  private SQLQuery retrieveTaskOutputsQuery() throws WebClientException {
    return new SQLQuery("SELECT task_output FROM ${schema}.${tmpTable};")
            .withVariable("schema", getSchema(db()))
            .withVariable("tmpTable", getTemporaryJobTableName(getId()));
  }

  private void insertTaskItemsInTaskTable(String schema, Step step, List<I> taskInputs)
          throws WebClientException, SQLException, TooManyResourcesClaimed {
    List<SQLQuery> insertQueries = new ArrayList<>();

    //Create process table
    runWriteQuerySync(buildTaskTableStatement(schema, this), db(WRITER), 0);

    for (I taskInput : taskInputs) {
      String taskItem = taskInput.serialize();

      infoLog(STEP_EXECUTE, "Add initial entry in process_table: " + taskItem );
      insertQueries.add(new SQLQuery("""             
            INSERT INTO  ${schema}.${table} AS t (task_input)
                VALUES (#{taskItem}::JSONB);
        """)
              .withVariable("schema", schema)
              .withVariable("table", getTemporaryJobTableName(step.getId()))
              .withNamedParameter("taskItem", taskItem));
    }
    if(!insertQueries.isEmpty()) {
      //Insert TaskItem into process table
      runBatchWriteQuerySync(SQLQuery.batchOf(insertQueries), db(WRITER), 0);
    }
  }

  public static String getTemporaryJobTableName(String stepId) {
    return JOB_DATA_PREFIX + stepId;
  }

  //TODO: if ImportSpaceToFilesStep is removed make it private
  protected static SQLQuery buildTemporaryJobTableDropStatement(String schema, String tableName) {
    return new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table};")
            .withVariable("table", tableName)
            .withVariable("schema", schema);
  }

  @JsonIgnore
  protected Map<String, Object> getQueryContext(String schema) throws WebClientException {
    Space superSpace = superSpace();
    List<String> tables = new ArrayList<>();
    if (superSpace != null)
      tables.add(getRootTableName(superSpace));
    tables.add(getRootTableName(space()));

    return new FeatureWriterQueryContextBuilder()
        .withSchema(schema)
        .withTables(tables)
        .withSpaceContext(DEFAULT)
        .withHistoryEnabled(space().getVersionsToKeep() > 1)
        .withBatchMode(true)
        .with("stepId", getId())
        .build();
  }

  /**
   * Represents an update for a space-based task, containing the task ID and its output payload.
   *
   * @param <O> The type of the output payload, extending TaskPayload.
   */
  public static class SpaceBasedTaskUpdate<O extends TaskPayload> extends ProcessUpdate<SpaceBasedTaskUpdate<O>> {
    public int taskId;
    public O taskOutput;
  }
}
