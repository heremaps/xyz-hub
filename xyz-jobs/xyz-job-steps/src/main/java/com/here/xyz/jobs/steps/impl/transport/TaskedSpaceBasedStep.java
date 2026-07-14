/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

import static com.here.xyz.jobs.steps.Step.Visibility.SYSTEM;
import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.WRITER;
import static com.here.xyz.jobs.steps.impl.SpaceBasedStep.LogPhase.STEP_EXECUTE;
import static com.here.xyz.jobs.steps.impl.SpaceBasedStep.LogPhase.STEP_ON_ASYNC_FAILURE;
import static com.here.xyz.jobs.steps.impl.SpaceBasedStep.LogPhase.STEP_ON_ASYNC_SUCCESS;
import static com.here.xyz.jobs.steps.impl.SpaceBasedStep.LogPhase.STEP_ON_ASYNC_UPDATE;
import static com.here.xyz.jobs.steps.impl.SpaceBasedStep.LogPhase.STEP_ON_STATE_CHECK;
import static com.here.xyz.jobs.steps.impl.SpaceBasedStep.LogPhase.UNKNOWN;
import static com.here.xyz.util.web.XyzWebClient.WebClientException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.jobs.JobClientInfo;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.ProcessUpdate;
import com.here.xyz.jobs.steps.execution.StepException;
import com.here.xyz.jobs.steps.execution.db.Database;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.impl.transport.tasks.TaskPayload;
import com.here.xyz.jobs.steps.impl.transport.tasks.TaskProgress;
import com.here.xyz.jobs.steps.impl.transport.tools.DatabaseStepQueryBuilder;
import com.here.xyz.jobs.steps.impl.transport.tools.TaskedSpaceBasedQueryBuilder;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.jobs.steps.outputs.S3Marker;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.query.QueryBuilder.QueryBuildingException;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.web.XyzWebClient;
import com.here.xyz.util.web.XyzWebClient.ErrorResponseException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
 * @param <I> The input payload type for each task.
 * @param <O> The output payload type for each task.
 */
public abstract class TaskedSpaceBasedStep<T extends TaskedSpaceBasedStep, I extends TaskPayload, O extends TaskPayload>
        extends SpaceBasedStep<T> {
  private static final Logger logger = LogManager.getLogger();
  public static final String FINALIZATION_MARKER = "finalized_marker";
  public static final Integer MAX_UNKNOWN_TASK_QUERY_CHECKS = 3;
  public static final Integer MAX_TASK_RETRY_ATTEMPTS = 1;
  private TaskedSpaceBasedQueryBuilder taskedSpaceBasedQueryBuilder;

  {
    setOutputSets(List.of(new OutputSet(FINALIZATION_MARKER, SYSTEM, true)));
  }

  @JsonView({Internal.class, Static.class})
  protected int threadCount = 8;

  @JsonView({Internal.class, Static.class})
  private boolean noTasksCreated = false;

  @JsonIgnore
  private boolean finalizedOnResume = false;

  @JsonView({Internal.class, Static.class})
  protected int taskItemCount = -1;
  @JsonView({Internal.class, Static.class})
  protected SpaceContext context;
  @JsonView({Internal.class, Static.class})
  protected Ref versionRef;
  @JsonView({Internal.class, Static.class})
  protected long spaceCreatedAt;
  @JsonView({Internal.class, Static.class})
  protected long minSpaceVersion = -1;
  @JsonView({Internal.class, Static.class})
  protected long maxSpaceVersion = -1;

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

  public long getMinSpaceVersion() {
    return minSpaceVersion;
  }

  public void setMinSpaceVersion(long minSpaceVersion) {
    this.minSpaceVersion = minSpaceVersion;
  }

  public T withMinSpaceVersion(long minSpaceVersion) {
    setMinSpaceVersion(minSpaceVersion);
    return (T) this;
  }

  public long getMaxSpaceVersion() {
    return maxSpaceVersion;
  }

  public void setMaxSpaceVersion(long maxSpaceVersion) {
    this.maxSpaceVersion = maxSpaceVersion;
  }

  public T withMaxSpaceVersion(long maxSpaceVersion) {
    setMaxSpaceVersion(maxSpaceVersion);
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
   * Processes the finalized task items after all parallel tasks have finalized.
   * Task outputs, inputs and ids are available.
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
   * @param finalizedTaskItems The list of finalized taskItems<taskId,{@code I},{@code O}> .
   * @throws IOException If an I/O error occurs while processing or persisting outputs.
   */
  protected abstract void processFinalizedTasks(List<FinalizedTaskItem<I,O>> finalizedTaskItems)
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
   * The hook is invoked for both fresh executions and resumed executions.
   * </p>
   *
   * @param resume {@code true} when the step is resumed after a previous attempt and should restore
   *               or rebuild any required state accordingly; {@code false} for a fresh execution.
   * @throws SQLException If a database access error occurs.
   * @throws TooManyResourcesClaimed If too many resources are claimed during setup.
   * @throws WebClientException If an error occurs while interacting with the web client.
   */
  protected void initialSetup(boolean resume) throws SQLException, TooManyResourcesClaimed, WebClientException {};

  /**
   * Performs final cleanup after all tasks have completed.
   * <p>
   * This method can be overridden by subclasses to implement resource deallocation,
   * temporary data removal, or any other necessary cleanup logic at the end of the job step.
   * </p>
   *
   * @param noTasksCreated Information if tasks got created.
   *
   * @throws WebClientException If an error occurs while interacting with the web client.
   * @throws SQLException If a database access error occurs.
   * @throws TooManyResourcesClaimed If too many resources are claimed during cleanup.
   */
  protected void finalCleanUp(boolean noTasksCreated) throws WebClientException, SQLException, TooManyResourcesClaimed, IOException {};

  /**
   * Hook invoked immediately before starting an individual task query.
   * <p>
   * Use this to prepare task-scoped resources (for example creating temporary helper tables)
   * that are required by the task query execution.
   * </p>
   *
   * @param taskId The id of the task about to be started.
   * @throws WebClientException If external calls fail.
   * @throws SQLException If DB preparation fails.
   * @throws TooManyResourcesClaimed If resource claiming exceeds configured limits.
   */
  protected void prepareTaskQuery(int taskId) throws WebClientException, SQLException, TooManyResourcesClaimed {}

  /**
   * Hook invoked when a non-final progress update for a running task is received.
   * If a async query is needed - always use callback=false!
   * <p>
   * This is the place to trigger follow-up async work (for example the next chunk query).
   * If a new async query is started here, it should use callback=false to avoid duplicate callback chains.
   * </p>
   *
   * @param taskId The id of the task that reported progress.
   * @param output The current task output payload from the callback.
   * @throws WebClientException If external calls fail.
   * @throws SQLException If DB interaction fails.
   * @throws TooManyResourcesClaimed If resource claiming exceeds configured limits.
   */
  protected void onTaskProgress(int taskId, O output) throws WebClientException, SQLException, TooManyResourcesClaimed {}

  /**
   * Determines whether the received task update represents the final state of the task.
   * <p>
   * Returning {@code false} keeps the task in running state and routes the update through the
   * progress hook. Returning {@code true} triggers finalization flow.
   * </p>
   *
   * @param taskId The id of the task being evaluated.
   * @param output The task output payload from the callback.
   * @return {@code true} if the task is final and can be finalized; {@code false} otherwise.
   * @throws WebClientException If external calls fail during decision making.
   */
  protected boolean isTaskFinalized(int taskId, O output) throws WebClientException { return true; }

  /**
   * Hook invoked after a task is considered finalized and before the framework marks it finalized
   * in the process table and claims the next task.
   * <p>
   * Use this for task-level finalization work that must succeed before finalization is persisted
   * (for example deleting task-specific temporary artifacts).
   * </p>
   *
   * @param taskId The id of the task being finalized.
   * @throws WebClientException If external calls fail.
   * @throws SQLException If DB finalization work fails.
   * @throws TooManyResourcesClaimed If resource claiming exceeds configured limits.
   */
  protected void finalizeTaskQuery(int taskId) throws WebClientException, SQLException, TooManyResourcesClaimed {}

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
      StatisticsResponse statistics = loadSpaceStatistics(getSpaceId(), context, true);
      setMinSpaceVersion(statistics.getMinVersion().getValue());
      setMaxSpaceVersion(statistics.getMaxVersion().getValue());

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
    if (versionRef == null)
      return true;

    if (versionRef.isSingleVersion()) {
      if (versionRef.getVersion() < minSpaceVersion)
        throw new ValidationException("Invalid VersionRef (" + versionRef + ")! Version is smaller than min available version '" +
            minSpaceVersion + "'!");
      if (versionRef.getVersion() > maxSpaceVersion)
        throw new ValidationException("Invalid VersionRef (" + versionRef + ")! Version is higher than max available version '" +
            maxSpaceVersion + "'!");
    }
    else if (versionRef.isRange()) {
      if (versionRef.getStart().getVersion() < minSpaceVersion - 1)
        throw new ValidationException("Invalid VersionRef! The first referenced version (" + (versionRef.getStart().getVersion() + 1)
            + ") by version range (" + versionRef + ") is smaller than min available version '" + minSpaceVersion
            + "'! [NOTE: The start version of a version range is exclusive, the end version however is inclusive]");
      if (versionRef.getEnd().getVersion() > maxSpaceVersion)
        throw new ValidationException("Invalid VersionRef (" + versionRef + ")! EndVersion is higher than max available version '"
            + maxSpaceVersion + "'!");
    }

    return true;
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
    List<TaskProgress<I>> claimedInitialTasks = new ArrayList<>();

    //Claim the initial task set
    for (int i = 0; i < threadCount; i++) {
      TaskProgress<I> taskProgressAndTaskItem = getTaskProgressAndNextTaskItem();
      if (taskProgressAndTaskItem.getTaskId() == -1)
        break;
      claimedInitialTasks.add(taskProgressAndTaskItem);
    }

    //Start initial tasks
    for (TaskProgress<I> claimedTask : claimedInitialTasks)
      startTask(claimedTask);
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

      prepareTaskQuery(taskProgressAndItem.getTaskId());

      runReadQueryAsync(buildTaskQuery(taskProgressAndItem.getTaskId(), (I) taskProgressAndItem.getTaskInput(), failureCallback),
                queryRunsOnWriter() ? dbWriter() : dbReader(), 0d/*perItemAcus.doubleValue()*/,  false);
    }
  }

  @Override
  public void execute(boolean resume) throws Exception {
    //The following code is running synchronously till the first task is getting started.
    if (!resume) {
      initialSetup(false);
      createAndInsertTaskItems();
    }else{
      try {
        //we need to do the initialSetup here to be able to cover the case that the table does not already exists.
        initialSetup(true);
        if (resetTaskItems()) return;
      }catch (SQLException e){
        if (e.getSQLState() != null && e.getSQLState().toUpperCase().equals("42P01")) {
          Optional<Output> marker = loadStepOutputs(getOutputSet(FINALIZATION_MARKER)).stream().findFirst();

          if(marker.isPresent()){
            //if the job_data table is not present anymore during a resume and the finalizationMarker is present
            //the step succeeded after der StateMaschnine was already canceled. In this case we only have to report success.
            infoLog(STEP_EXECUTE, "Outputs are already present -> finalize");
            finalizedOnResume = true;
            reportAsyncSuccess();
            return;
          }
          //Can not retrieve output - start from scratch
          infoLog(STEP_EXECUTE, "Reset of taskItems failed cause job_data table is missing! Recreating it!");
          initialSetup(false);
          createAndInsertTaskItems();
        }else
          throw e;
      }
    }
    startInitialTasks();
    noTasksCreated = taskItemCount == 0;
  }

  private boolean resetTaskItems() throws TooManyResourcesClaimed, SQLException, WebClientException {
    //Reset all items which are not finalized to be able to restart them
    int resetItemCount = runWriteQuerySyncUnkillable(resetTaskItemWhichAreNotFinalized(), db(WRITER), 0);

    //If no item got reset, there is no restartable work left. If additionally all task items are already
    //finalized, nothing needs to be (re)started - just collect the produced outputs and succeed directly.
    if (resetItemCount == 0 && getTaskProgress().isComplete()) {
      infoLog(STEP_EXECUTE, "All task items are already finalized on resume -> collect outputs and finalize.");
      reportAsyncSuccess();
      return true;
    }
    return false;
  }

  private void createAndInsertTaskItems() throws SQLException, TooManyResourcesClaimed, QueryBuildingException, WebClientException {
    List<I> taskDataList = createTaskItems();
    taskItemCount = taskDataList.size();
    insertTaskItemsInTaskTable(taskDataList);
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
  protected boolean onAsyncUpdate(ProcessUpdate processUpdate) {
    try {
      //Update the task table and mark item as finalized
      SpaceBasedTaskUpdate update = (SpaceBasedTaskUpdate) processUpdate;
      infoLog(STEP_ON_ASYNC_UPDATE, "Received progress update: " + processUpdate.serialize());

      if (!isTaskFinalized(update.taskId, (O) update.taskOutput)){
        updateQueryTaskItemOutput(update); //update taskItem output
        onTaskProgress(update.taskId, (O) update.taskOutput);  //hook method
      }else{
        finalizeTaskQuery(update.taskId); //hook method

        TaskProgress taskProgressAndItem = finalizeCurrentTaskAndGetTaskProgressAndNextTaskItem(update);
        //Calculate progress and set it on the step's status
        getStatus().setEstimatedProgress((float) taskProgressAndItem.getFinalizedTasks() / (float) taskProgressAndItem.getTotalTasks());

        if (taskProgressAndItem.isComplete()) {
          //All tasks are finalized - succeed!
          return true;
        }

        if (!taskProgressAndItem.hasTaskItem()) {
          infoLog(STEP_ON_ASYNC_UPDATE,"No claimable task. Waiting for running tasks to finish: " + taskProgressAndItem);
          return false;
        }

        infoLog(STEP_ON_ASYNC_UPDATE,"Found existing tasks. Start new item: " + taskProgressAndItem);
        startTask(taskProgressAndItem);
        return false;
      }
      return false;
    }catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void onAsyncSuccess() throws Exception {
    if (finalizedOnResume) {
      infoLog(STEP_ON_ASYNC_SUCCESS, "Step was already finalized during resume. Skip output collection.");
      cleanUpDbResources(STEP_ON_ASYNC_SUCCESS);
      return;
    }

    infoLog(STEP_ON_ASYNC_SUCCESS, "Reached onAsyncSuccess! Start collecting task outputs!");

    //Collect outputs and process them
    processFinalizedTasks(collectOutputs());

    infoLog(STEP_ON_ASYNC_SUCCESS, "End collecting task outputs!");
    //Clean up temporary resources
    cleanUpDbResources(STEP_ON_ASYNC_SUCCESS);
  }

  @Override
  protected boolean onAsyncFailure() {
    try {
      //TODO: Inspect the error provided in the status and decide whether it is retryable (return-value)
      //For now we set all failures to be retryable
      boolean isRetryable = true;

      if (!isRetryable)
        cleanUpDbResources(STEP_ON_ASYNC_FAILURE);

      return isRetryable;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void cleanUpDbResources(LogPhase logPhase) throws WebClientException, SQLException, TooManyResourcesClaimed, IOException {
    try {
      infoLog(logPhase, "Executing cleanUp Hook");
      finalCleanUp(noTasksCreated);

      infoLog(logPhase, "Cleanup temporary table");
      runWriteQuerySyncUnkillable(getQueryBuilder().buildTemporaryJobTableDropStatement(), db(WRITER), 0);

      registerOutputs(List.of(new S3Marker()
              .withFinalized(true)
              .withFileName(FINALIZATION_MARKER + ".json")), FINALIZATION_MARKER);
    }catch (SQLException e){
      if (e.getSQLState() != null) {
        switch (e.getSQLState().toUpperCase()) {
          case "42P01":
            // relation does not exist
            warnLog(UNKNOWN, "Resource does not exist anymore. Ignore ");
            break;
          case "40P01":
            // deadlock detected
            warnLog(UNKNOWN, "Deadlock detected! Ignore ");
            break;
          default:
            errorLog(UNKNOWN, e);
            throw e;
        }
      } else {
        errorLog(UNKNOWN, e);
        throw e;
      }
    }
  }

  private List<FinalizedTaskItem<I,O>> collectOutputs() throws SQLException, TooManyResourcesClaimed, WebClientException {
    return runReadQuerySync(getQueryBuilder().buildRetrieveTaskOutputsQuery(), db(WRITER), 0, rs -> {
      try {
        List<FinalizedTaskItem<I,O>> finalizedTaskItems = new ArrayList<>();

        while (rs.next()){
          int taskId = rs.getInt("task_id");
          String taskInput = rs.getString("task_input");
          String taskOutput = rs.getString("task_output");

          if(taskOutput != null){
            I input = XyzSerializable.deserialize(taskInput, new TypeReference<I>() {});
            O output = XyzSerializable.deserialize(taskOutput, new TypeReference<O>() {});

            finalizedTaskItems.add(new FinalizedTaskItem<>(taskId, input, output));
          }else{
            infoLog(STEP_ON_ASYNC_SUCCESS,  "Empty task output found - skip.");
          }
        }
        return finalizedTaskItems;
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public AsyncExecutionState getExecutionState() throws UnknownStateException {
    if (noTasksCreated)
      return AsyncExecutionState.SUCCEEDED;

    try {
      TaskProgress<?> taskProgress = getTaskProgress();
      return evaluateExecutionState(taskProgress);
    }
    catch (SQLException e) {
      throw mapSqlException(e);
    }catch (WebClientException e) {
      throw new UnknownStateException("WebClientException occurred during execution state check for step: " + getGlobalStepId() + " !");
    }
    catch (TooManyResourcesClaimed e) {
      throw new StepException("Unexpected error occurred during execution state check for step: " + getGlobalStepId() + " !", e);
    }
  }

  private AsyncExecutionState evaluateExecutionState(TaskProgress<?> taskProgress)
      throws UnknownStateException, SQLException, WebClientException, TooManyResourcesClaimed {
    if (taskProgress.isComplete()) {
      //Only log and return RUNNING to avoid double success handling;
      infoLog(STEP_ON_STATE_CHECK, "All tasks finalized!");
    }
    if (taskProgress.hasRunningTasks()) {
      Set<Integer> expectedTaskIds = taskProgress.getStartedNotFinalizedTaskIds();
      Set<String> expectedTaskIdStrings = expectedTaskIds.stream()
          .map(String::valueOf)
          .collect(Collectors.toSet());

      if (expectedTaskIdStrings.isEmpty()) {
        infoLog(STEP_ON_STATE_CHECK, "Started tasks are reported but started_not_finalized_task_ids is empty.");
        throw new UnknownStateException("No started-not-finalized task ids available for step: " + getGlobalStepId() + " !");
      }

      boolean runsOnWriter = queryRunsOnWriter();
      Database database = runsOnWriter ? dbWriter() : dbReader();
      //Check if all expected TaskQueries areRunning. Collect all taskIds where we are not able
      //to find a running query.
      Set<String> runningTaskIds = SQLQuery.areRunning(
          requestResource(database, 0d), database.getRole() != WRITER,
              getId() + "#taskId", expectedTaskIdStrings
      );

      Set<Integer> unknownStateTaskIds = expectedTaskIds.stream()
          .filter(taskId -> !runningTaskIds.contains(String.valueOf(taskId)))
          .collect(Collectors.toSet());

      if (unknownStateTaskIds.isEmpty())
        return AsyncExecutionState.RUNNING;

      Set<Integer> exceededTaskIds = incrementUnknownQueryStateForTasks(unknownStateTaskIds);
      for(int exceededTaskId : exceededTaskIds){
        infoLog(STEP_ON_STATE_CHECK, "Unknown queryState of taskId " + exceededTaskId + " has exceeded unknown query state threshold. Retry the task now!");
        try {
          TaskProgress<I> retryTaskProgress = resetTaskForRetry(exceededTaskId);
          if (retryTaskProgress == null) {
            throw new StepException("Retry threshold exceeded for taskId " + exceededTaskId + ".");
          }
          startTask(retryTaskProgress);
        } catch (StepException e1){
          throw e1;
        } catch (Exception e2){
          throw new StepException("Not able to retry taskId " + exceededTaskId, e2);
        }
      }

      return AsyncExecutionState.RUNNING;
    }
    if (taskProgress.hasNoRunningTasks()) {
      infoLog(STEP_ON_STATE_CHECK, "No running tasks detected. StartedTasks: " + taskProgress.getStartedTasks() + ","
          + " FinalizedTasks: " + taskProgress.getFinalizedTasks() + " !");
      throw new UnknownStateException("No running Tasks detected for step: " + getGlobalStepId() + " !");
    }
    return AsyncExecutionState.RUNNING;
  }

  private UnknownStateException mapSqlException(SQLException e) {
    if (e.getSQLState() != null && e.getSQLState().equalsIgnoreCase("42P01")) {
      // If we are here task table does not exist anymore. Could happen via getTaskProgress() or during check if queries are running.
      infoLog(STEP_ON_STATE_CHECK, "Task table does not exist anymore. Ignore.");
      return new UnknownStateException("Task table does not exist anymore " + getGlobalStepId() + " !");
    }
    errorLog(STEP_ON_STATE_CHECK, e);
    return new UnknownStateException("SQLException occurred " + getGlobalStepId() + " !");
  }

  /**
   * Retrieves the task progress and an unstarted task item (if present) from the database.
   *
   * @return The task progress and task item details.
   * @throws WebClientException If an error occurs while interacting with the web client.
   * @throws SQLException If an error occurs while executing SQL queries.
   * @throws TooManyResourcesClaimed If too many resources are claimed during the process.
   */
  private TaskProgress getTaskProgressAndNextTaskItem() throws WebClientException, SQLException, TooManyResourcesClaimed {
    return executeTaskItemQuery(getQueryBuilder().retrieveTaskItemAndStatisticsQuery());
  }

  /**
   * Finalizes the current task item and atomically retrieves updated progress plus the next task item.
   * <p>
   * This method is used when an async task completion callback arrives. It persists the provided
   * task update (including task output and finalized flag), updates task statistics, and returns
   * the next unstarted task (if any) together with the current progress counters.
   * </p>
   *
   * @param update The callback payload containing the completed task id and output.
   * @return A {@link TaskProgress} instance containing total/started/finalized counters and the
   *         next task input; {@code taskId = -1} indicates that no further task is available.
   * @throws WebClientException If context-dependent metadata retrieval fails.
   * @throws SQLException If the underlying SQL function execution fails.
   * @throws TooManyResourcesClaimed If DB resource claiming exceeds allowed limits.
   */
  private TaskProgress finalizeCurrentTaskAndGetTaskProgressAndNextTaskItem(SpaceBasedTaskUpdate update)
          throws WebClientException, SQLException, TooManyResourcesClaimed {
    infoLog(STEP_ON_ASYNC_UPDATE, "Update process table and claim next task with: " + update.serialize());
    return  executeTaskItemQuery(getQueryBuilder().buildRetrieveTaskItemAndStatisticsAfterUpdateQuery(update));
  }

  /**
   * Merges the task update into the task's {@code task_output} column.
   * <p>
   * The update is merged with the existing output to preserve any previously stored fields.
   * For example, if the existing output contains {@code fileBytes} and {@code importStatistics},
   * and the update contains a {@code progress} field, the result will contain all three fields.
   * Fields in the update override fields with the same name in the existing output.
   * </p>
   *
   * @param update The in-progress task update carrying the complete payload.
   */
  private void updateQueryTaskItemOutput(SpaceBasedTaskUpdate update) throws WebClientException, SQLException, TooManyResourcesClaimed {
    runWriteQuerySyncUnkillable(getQueryBuilder().buildUpdateTaskItemOutputStatement(update)
            , db(WRITER), 0);
  }

  private Set<Integer> incrementUnknownQueryStateForTasks(Set<Integer> unknownStateTaskIds)
      throws WebClientException, SQLException, TooManyResourcesClaimed {
    if (unknownStateTaskIds == null || unknownStateTaskIds.isEmpty())
      return Set.of();

    SQLQuery query = getQueryBuilder().buildIncrementUnknownQueryStateStatement(unknownStateTaskIds, MAX_UNKNOWN_TASK_QUERY_CHECKS);

    return runReadQuerySync(query, db(WRITER), 0, rs -> {
      Set<Integer> exceededTaskIds = new java.util.LinkedHashSet<>();
      while (rs.next())
        exceededTaskIds.add(rs.getInt("task_id"));
      return exceededTaskIds;
    });
  }

  private TaskProgress<I> resetTaskForRetry(int taskId) throws WebClientException, SQLException, TooManyResourcesClaimed {
    return runReadQuerySync(getQueryBuilder().buildResetTaskForRetryStatement(taskId), db(WRITER), 0, rs -> {
      if (!rs.next())
        throw new SQLException("Task for retry not found: taskId=" + taskId);

      int retryAttempt = rs.getInt("retry_attempts");
      if (retryAttempt > MAX_TASK_RETRY_ATTEMPTS)
        return null;

      try {
        return new TaskProgress<>(rs.getInt("task_id"),
                XyzSerializable.deserialize(rs.getString("task_input"), new TypeReference<I>() {}));
      }
      catch (JsonProcessingException e) {
        throw new StepException("Can not deserialize task_input for retry of taskId=" + taskId + "!", e);
      }
    });
  }

  private TaskProgress executeTaskItemQuery(SQLQuery query) throws WebClientException, SQLException, TooManyResourcesClaimed {
    TaskProgress taskProgress;
    try {
      taskProgress = runReadQuerySync(query, db(WRITER), 0,
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
    } catch (SQLException e) {
      if (e.getSQLState() != null && e.getSQLState().equalsIgnoreCase("42P01")) {
        //If we are here a failure already happened and the task table does not exist anymore
        //To avoid overriding the original failure we just log this and return a completed TaskProgress
        infoLog(UNKNOWN, "Task table does not exist anymore. Ignore.");
        return new TaskProgress<>(-1);
      }
      errorLog(UNKNOWN, e);
      throw e;
    }
    return taskProgress;
  }

  private TaskProgress getTaskProgress() throws WebClientException, SQLException, TooManyResourcesClaimed {
    return runReadQuerySync(getQueryBuilder().retrieveTaskStatisticsQuery(), db(WRITER), 0,
              rs -> {
                if (!rs.next())
                  return null;
                return new TaskProgress(rs.getInt("total"), rs.getInt("started"), rs.getInt("finalized"),getIntegerList(rs, "started_not_finalized_task_ids"));
              });
  }

  private Set<Integer> getIntegerList(ResultSet rs, String columnName) throws SQLException {
    Array sqlArray = rs.getArray(columnName);

    return sqlArray == null
            ? Set.of()
            : Arrays.stream((Object[]) sqlArray.getArray())
            .map(value -> ((Number) value).intValue())
            .collect(Collectors.toSet());
  }

  private SQLQuery resetTaskItemWhichAreNotFinalized() {
    infoLog(STEP_EXECUTE, "Reset task items for restart.");
    return getQueryBuilder().buildResetTaskItemWhichAreNotFinalizedStatement();
  }

  /**
   * Loads the previously stored {@code task_output.taskOutput} payload of a single task item, if available.
   * <p>
   * Returns {@code null} when no row matches the given {@code taskId} or when no output has been stored yet.
   * This helper is intended to support resume scenarios where a subclass needs to inspect the last
   * persisted state of a task before deciding which query to start next.
   * </p>
   *
   * @param taskId The id of the task whose output should be loaded.
   * @return The deserialized output payload of type {@code O}, or {@code null} if not available.
   */
  protected O loadTaskOutput(int taskId) throws WebClientException, SQLException, TooManyResourcesClaimed {
    return runReadQuerySync(getQueryBuilder().buildLoadOutputsQuery(taskId), db(WRITER), 0, rs -> {
      try {
        if (!rs.next()) return null;
        String taskOutput = rs.getString("task_output");
        if (taskOutput == null) return null;
        return XyzSerializable.deserialize(taskOutput, new TypeReference<O>() {});
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Can not deserialize task_output for taskId=" + taskId, e);
      }
    });
  }

  private boolean insertTaskItemsInTaskTable(List<I> taskInputs)
          throws WebClientException, SQLException, TooManyResourcesClaimed {
    List<SQLQuery> insertQueries = new ArrayList<>();

    //TODO: this check can be removed after DS-936 is fixed.
    boolean tableAlreadyExists = false;
    try{
      //Create process table
      runWriteQuerySyncUnkillable(getQueryBuilder().buildTaskTableStatement(), db(WRITER), 0);
    }catch (SQLException e){
      if(e.getSQLState() != null && e.getSQLState().equalsIgnoreCase("42P07")) {
        infoLog(UNKNOWN,  "Task table already exists. Assume it was created in a failed attempt and continue.");
        tableAlreadyExists = true;
      }else {
        throw e;
      }
    }

    if(tableAlreadyExists) {
      //Reset all items which are not finalized to be able to restart them
      resetTaskItems();
    }else{
      infoLog(STEP_EXECUTE, "Add initial entries in process_table for " + taskInputs.size() + " tasks.");
      for (I taskInput : taskInputs)
        insertQueries.add(getQueryBuilder().buildInsertTaskItemStatement(taskInput.serialize()));

      if(!insertQueries.isEmpty()) {
        //Insert TaskItem into process table
        runBatchWriteQuerySyncUnkillable(SQLQuery.batchOf(insertQueries), db(WRITER), 0);
      }
    }
    return tableAlreadyExists;
  }

  private TaskedSpaceBasedQueryBuilder getQueryBuilder() {
    if (taskedSpaceBasedQueryBuilder == null)
      taskedSpaceBasedQueryBuilder = initQueryBuilder(TaskedSpaceBasedQueryBuilder::new);
    return taskedSpaceBasedQueryBuilder;
  }

  /**
   * Creates a {@link DatabaseStepQueryBuilder} using the resources of this step. It centralizes the resolution of the
   * step's schema, root table and (optional) super root table so that the concrete step implementations only have to
   * provide the actual builder instantiation.
   *
   * @param factory the factory instantiating the concrete query builder
   * @param <B> the concrete query builder type
   * @return the newly created query builder
   */
  protected <B extends DatabaseStepQueryBuilder> B initQueryBuilder(QueryBuilderFactory<B> factory) {
    try {
      Space superSpace = superSpace();
      return factory.create(space(), context, getId(), getSchema(db()), getRootTableName(space()),
              superSpace == null ? null : getRootTableName(superSpace));
    }
    catch (WebClientException e) {
      throw new StepException("Unable to load resource.", e.getCause());
    }
  }

  @FunctionalInterface
  protected interface QueryBuilderFactory<B extends DatabaseStepQueryBuilder> {
    B create(Space space, SpaceContext context, String stepId, String schema, String rootTable, String superRootTable)
            throws WebClientException;
  }

  public record FinalizedTaskItem<In extends TaskPayload, Out extends TaskPayload>(int taskId, In input, Out output) {}

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
