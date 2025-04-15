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

package com.here.xyz.jobs;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT;
import static com.here.xyz.jobs.RuntimeInfo.State.CANCELLED;
import static com.here.xyz.jobs.RuntimeInfo.State.CANCELLING;
import static com.here.xyz.jobs.RuntimeInfo.State.FAILED;
import static com.here.xyz.jobs.RuntimeInfo.State.NOT_READY;
import static com.here.xyz.jobs.RuntimeInfo.State.PENDING;
import static com.here.xyz.jobs.RuntimeInfo.State.RESUMING;
import static com.here.xyz.jobs.RuntimeInfo.State.RUNNING;
import static com.here.xyz.jobs.RuntimeInfo.State.SUBMITTED;
import static com.here.xyz.jobs.RuntimeInfo.State.SUCCEEDED;
import static com.here.xyz.jobs.steps.Step.InputSet.DEFAULT_INPUT_SET_NAME;
import static com.here.xyz.jobs.steps.inputs.Input.inputS3Prefix;
import static com.here.xyz.jobs.steps.resources.Load.addLoads;
import static com.here.xyz.util.Random.randomAlpha;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.config.JobConfigClient;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.datasets.streams.DynamicStream;
import com.here.xyz.jobs.processes.ProcessDescription;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.JobCompiler;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.execution.JobExecutor;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.inputs.ModelBasedInput;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.jobs.steps.resources.ExecutionResource;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.models.hub.Space.Extension;
import com.here.xyz.util.Async;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.ErrorResponseException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@JsonInclude(NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Job implements XyzSerializable {
  //Framework defined properties:
  @JsonView({Public.class, Static.class})
  private String id;
  @JsonView(Static.class)
  private RuntimeStatus status;
  @JsonView({Public.class, Static.class})
  private long createdAt;
  @JsonView({Public.class, Static.class})
  private long updatedAt;
  @JsonView({Public.class, Static.class})
  private long keepUntil;
  @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
  private Map<String, Input> inputs;
  //Caller defined properties:
  @JsonView(Static.class)
  private String owner;
  @JsonView({Public.class, Static.class})
  private String description;
  @JsonView({Public.class, Static.class})
  private ProcessDescription process;
  @JsonView({Public.class, Static.class})
  private DatasetDescription source;
  @JsonView({Public.class, Static.class})
  private DatasetDescription target;
  //Framework derived properties:
  @JsonView(Static.class)
  private StepGraph steps;
  @JsonView(Static.class)
  private String executionId;
  @JsonView({Public.class, Static.class})
  private JobClientInfo clientInfo;
  @JsonView(Static.class)
  private String secondaryResourceKey;

  private static final Async ASYNC = new Async(100, Job.class);
  private static final Logger logger = LogManager.getLogger();
  private static final long DEFAULT_JOB_TTL = TimeUnit.DAYS.toMillis(2 * 7); //4 weeks

  /**
   * Creates a new Job.
   * The new job will have the following properties being filled by the framework:
   *  - ID (auto-generated)
   *  - Initial status (with state NOT_READY)
   *  - Creation time
   *  - Initial updated time
   *
   * The following properties need to be defined by the user:
   *  - owner (Some ID identifying the user who owns & controls the job)
   *  - description (Some human-readable description for the purpose of the job)
   *  - source (A description of the source from which to "read" during the job execution)
   *  - target (A description of the target into which to "write" during the job execution)
   *
   * As the job is not complete after instantiation, it's not persisted yet.
   * Once all necessary properties have been defined, it can be submitted to the execution system by calling {@link Job#submit()}.
   * On success, that will also persist the job.
   * If the job should be persisted before submission, the {@link Job#store()} method can be used.
   */
  public Job() {}

  /**
   * Has to be called at the initial creation of a new job to initialize its ID, status and timestamps.
   *
   * @return This job instance for chaining
   */
  public Job create() {
    //Define the framework standard properties
    if (getId() == null)
      setId(randomAlpha());
    return withStatus(new RuntimeStatus().withState(NOT_READY))
        .withCreatedAt(Core.currentTimeMillis())
        .withUpdatedAt(getCreatedAt())
        .withKeepUntil(getKeepUntil() <= 0 ? getCreatedAt() + DEFAULT_JOB_TTL : getKeepUntil());
  }

  //TODO: Make sure also the step states are always set accordingly (prior to execution)

  /**
   * Used to submit this job to the job execution system in order to get it prepared & validated prior to its execution.
   * This method will do the following:
   *  - Compile the job into an executable flow
   *  - Validate if all preconditions are met (e.g. necessary user inputs are provided, all steps are configured properly, ...)
   *  - If all pre-conditions are met, transition the job to SUBMITTED state & store it, return false otherwise
   *  - Start the job and return true
   * @return Whether submission was done. If submission was not done, the Job remains in state NOT_READY.
   */
  public Future<Boolean> submit() {
    logger.info("[{}] Submitting job ...", getId());
    return JobCompiler.getInstance().compile(this)
        .compose(stepGraph -> {
          setSteps(stepGraph);
          getStatus().setOverallStepCount((int) stepGraph.stepStream().count());
          Input.activateInputsCache(getId());
          return prepare().compose(v -> validate());
        })
        .compose(isReady -> {
          if (isPipeline() || isReady) {
            getStatus().setState(SUBMITTED);
            return store().compose(v -> start()).map(true);
          }
          else {
            Input.clearInputsCache(getId());
            logger.info("{}: Job is not ready for submission yet. Not all pre-conditions are met.", getId());
            return store().map(false);
          }
        });
  }

  private static <E, R> List<Future<R>> forEach(List<E> elements, Function<E, Future<R>> action) {
    List<Future<R>> futures = new ArrayList<>();
    for (E element : elements)
      futures.add(action.apply(element));
    return futures;
  }

  /**
   * Calls {@link Step#prepare(String, JobClientInfo)} on all steps belonging to this job.
   * @return
   */
  protected Future<Void> prepare() {
    logger.info("[{}] Preparing job ...", getId());
    return Future.all(Job.forEach(getSteps().stepStream().collect(Collectors.toList()), step -> prepareStep(step))).mapEmpty();
  }

  private Future<Void> prepareStep(Step step) {
    return ASYNC.run(() -> {
      step.prepare(getOwner(), getClientInfo());
      return null;
    });
  }

  /**
   * Returns failed futures in case of validation errors.
   * @return true if the job is ready for execution, false otherwise
   */
  protected Future<Boolean> validate() {
    logger.info("[{}] Validating job ...", getId());
    //TODO: Collect exceptions and forward them accordingly as one exception object with (potentially) multiple error objects inside
    return Future.all(Job.forEach(nonFinalSteps().toList(), step -> validateStep(step)))
        .compose(cf -> Future.succeededFuture(cf.list().stream().allMatch(isReady -> (boolean) isReady)));
  }

  private static Future<Boolean> validateStep(Step step) {
    return ASYNC.run(() -> {
      logger.info("[{}] Validating {}-step ...", step.getGlobalStepId(), step.getClass().getSimpleName());
      boolean isReady = step.validate();
      logger.info("[{}] Validation of {}-step completed.", step.getGlobalStepId(), step.getClass().getSimpleName());
      State targetState = isReady ? SUBMITTED : NOT_READY;
      if (step.getStatus().getState() != targetState)
        step.getStatus().setState(targetState);
      return isReady;
    });
  }

  public Future<Void> start() {
    //Is this job ready to be started?
    if (getStatus().getState() != SUBMITTED)
      return Future.failedFuture(new IllegalStateException("Job can not be started as it's not in SUBMITTED state."));

    getStatus().setState(PENDING);
    nonFinalSteps().forEach(step -> step.getStatus().setState(PENDING));

    long t1 = Core.currentTimeMillis();
    return store()
        .compose(v -> startExecution(false))
        .onSuccess(v -> logger.info("Started job {}. Took {}ms.", getId(), Core.currentTimeMillis() - t1));
  }

  private Future<Void> startExecution(boolean resume) {
    logger.info("[{}] Starting job ...", getId());
    /*
    Execute the step graph of this job.
    From now on, the intrinsic state updates will be synchronized
    from the step executions back to the service and cached in the job's step graph.
     */
    return JobExecutor.getInstance().startExecution(this, resume ? getExecutionId() : null);
  }

  /**
   * Cancels the execution of this job.
   *
   * @return A future providing a boolean telling whether the action was performed already.
   */
  public Future<Boolean> cancel() {
    logger.info("[{}] Cancelling job ...", getId());
    getStatus().setState(CANCELLING);

    return storeStatus(null)
        //Cancel the execution in any case, to prevent race-conditions
        .compose(v -> {
          if (isPipeline()) {
            getStatus().setState(CANCELLED);
            return storeStatus(CANCELLING);
          }
          return JobExecutor.getInstance().cancel(getExecutionId());
        })
        /*
        NOTE: Cancellation is still in progress. The JobExecutor will now monitor the different step cancellations
        and update the Job to CANCELLED once all cancellations are completed.
         */
        .map(false);
  }

  /**
   * Retrieves the step from the Job matching the stepId
   * @param stepId
   * @return
   */
  public Step getStepById(String stepId) {
    return getSteps().getStep(stepId);
  }

  private Stream<Step> nonFinalSteps() {
    return getSteps().stepStream().filter(step -> !step.getStatus().getState().isFinal());
  }

  /**
   * Updates the status of a step at this job by replacing it with the specified one.
   * @param step
   * @return
   */
  public Future<Void> updateStep(Step step) {
    final Step existingStep = getStepById(step.getId());
    if (existingStep == null)
      throw new IllegalArgumentException("The provided step with ID " + step.getGlobalStepId() + " was not found.");

    return updateStep(step, existingStep.getStatus().getState(), true);
  }

  public Future<Void> updateStepStatus(String stepId, RuntimeInfo status, boolean cancelOnFailure) {
    final Step step = getStepById(stepId);
    if (step == null)
      throw new IllegalArgumentException("The provided step with ID " + stepId + " was not found.");

    State existingStepState = step.getStatus().getState();

    step.getStatus()
        .withState(status.getState())
        .withErrorCode(status.getErrorCode())
        .withErrorCause(status.getErrorCause())
        .withErrorMessage(status.getErrorMessage());

    return updateStep(step, existingStepState, cancelOnFailure);
  }

  private Future<Void> updateStep(Step step, State previousStepState, boolean cancelOnFailure) {
    //TODO: Once the state was SUCCEEDED it should not be mutable at all anymore
    if (previousStepState != null && !step.getStatus().getState().isFinal() && previousStepState.isFinal())
      //In case the step was already marked to have a final state, ignore any subsequent non-final updates to it
      return Future.succeededFuture();

    if (step.getStatus().getState().isFinal())
      updatePreviousAttempts(step);

    boolean found = getSteps().replaceStep(step);
    if (!found)
      throw new IllegalArgumentException("The provided step with ID " + step.getGlobalStepId()
          + " could not be replaced in the StepGraph of job with ID " + getId() + " as it was not found.");

    //If applicable, update the number of succeeded steps at the runtime status
    if (step.getStatus().getState() == SUCCEEDED)
      getStatus().setSucceededSteps((int) getSteps().stepStream().filter(s -> s.getStatus().getState() == SUCCEEDED).count());

    //Update the job's progress with respect to the step's progress (weighted by the initial execution time estimation of each step)
    int completedWorkUnits = getSteps().stepStream()
        .mapToInt(s -> (int) (s.getEstimatedExecutionSeconds() * s.getStatus().getEstimatedProgress())).sum();
    int overallWorkUnits = getSteps().stepStream().mapToInt(s -> s.getEstimatedExecutionSeconds()).sum();
    getStatus().setEstimatedProgress((float) completedWorkUnits / (float) overallWorkUnits);

    if (previousStepState != FAILED && step.getStatus().getState() == FAILED) {
      getStatus()
          .withState(FAILED)
          .withErrorMessage(step.getStatus().getErrorMessage())
          .withErrorCause(step.getStatus().getErrorCause())
          .withErrorCode(step.getStatus().getErrorCode());
    }

    return storeUpdatedStep(step)
        .compose(v -> storeStatus(null))
        .compose(v -> getStatus().getState() == FAILED && cancelOnFailure ? JobExecutor.getInstance().cancel(getExecutionId()).recover(t -> {
          logger.error("[{}] Error cancelling the job execution. Was it already cancelled before?", getId(), t);
          return Future.succeededFuture();
        }) : Future.succeededFuture());
  }

  private Future<Void> updatePreviousAttempts(Step step) {
    //TODO: Load & iterate event history and count the amount of TasKStateEntered events per step (Set it at step#previousAttempts)
    return Future.succeededFuture();
  }

  /**
   * Resumes this job after it has previously been canceled or failed, and the failure is retryable.
   * @return A future providing a boolean telling whether the action was performed already.
   */
  public Future<Boolean> resume() {
    logger.info("[{}] Resuming job ...", getId());
    if (isResumable()) {
      getStatus().setState(RESUMING);
      getSteps().stepStream().forEach(step -> {
        if (step.getStatus().getState().isValidSuccessor(RESUMING)) //NOTE: Steps with e.g. state SUCCEEDED must not be resumed
          step.getStatus().setState(RESUMING);
      });
      //TODO: Prepare steps to be re-executed here?
      return store()
          .compose(v -> {
            getStatus().setState(PENDING);
            getSteps().stepStream().forEach(step -> {
              if (step.getStatus().getState().isValidSuccessor(PENDING)) //NOTE: Steps with e.g. state SUCCEEDED must not be resumed
                getStatus().setState(PENDING);
            });
            return store()
                .compose(v2 -> startExecution(true));
          }).map(true);
    }
    else
      return Future.failedFuture(new IllegalStateException("Job " + getId() + " is not resumable."));
  }

  public Future<Void> store() {
    //TODO: Validate changes on the job and make sure the job may be stored in the current state
    return JobConfigClient.getInstance().storeJob(this);
  }

  public Future<Void> storeStatus(State expectedPreviousState) {
    logger.info("{}: Store Job-Status:{}", getId(), getStatus().getState());
    return JobConfigClient.getInstance().updateStatus(this, expectedPreviousState);
  }

  public Future<Void> storeUpdatedStep(Step<?> step) {
    logger.info("{} StoreUpdatedStep: {}", step.getGlobalStepId(), getStatus().getState());
    return JobConfigClient.getInstance().updateStep(this, step);
  }

  public static Future<Job> load(String jobId) {
    return JobConfigClient.getInstance().loadJob(jobId);
  }

  public static Future<List<Job>> load(State state, String resourceKey) {
    if (state == null && resourceKey == null)
      return loadAll();
    else
      return JobConfigClient.getInstance().loadJobs(resourceKey, state);
  }

  public static Future<List<Job>> load(boolean newerThan, long  createdAt) {
    return JobConfigClient.getInstance().loadJobs(newerThan, createdAt);
  }

  public static Future<Set<Job>> loadByResourceKey(String resourceKey) {
    return JobConfigClient.getInstance().loadJobs(resourceKey);
  }

  public static Future<List<Job>> loadAll() {
    return JobConfigClient.getInstance().loadJobs();
  }

  public static Future<Void> delete(String jobId) {
    return load(jobId)
        //First delete all the inputs / outputs of the job
        .compose(job -> job.getStatus().getState() == RUNNING
            ? Future.failedFuture(new IllegalStateException("Job can not be deleted as it is in state RUNNING."))
            : job.deleteJobResources())
        //Now finally delete this job's configuration
        .compose(v -> JobConfigClient.getInstance().deleteJob(jobId).mapEmpty());
  }

  /*
  NOTE: This method should **only** be called by the ADMIN API as reaction to a deletion outside the service directly in the job config DB.
  E.g., when a job config was deleted due to a Dynamo TTL
   */
  public Future<Void> deleteJobResources() {
    //Delete StateMachine if still existing
    return JobExecutor.getInstance().deleteExecution(getExecutionId())
        //Delete the inputs of this job
        .compose(b -> deleteInputs())
        //Delete the outputs of all involved steps
        .compose(v -> Future.all(Job.forEach(getSteps().stepStream().collect(Collectors.toList()), step -> deleteStepOutputs(step)))
            .mapEmpty());
  }

  private static Future<Boolean> deleteStepOutputs(Step step) {
    return ASYNC.run(() -> {
      step.deleteOutputs();
      return null;
    });
  }

  /**
   * Calculates the overall loads (necessary resources) of this job by aggregating the resource loads of all steps of this job.
   * The aggregation of parallel steps is done in the way that all resource-loads of parallel running steps will be added, while
   * in the case of sequentially running steps always the maximum of the step's resources will be taken into account.
   *
   * @return A list of overall resource-loads being reserved by this job
   */
  public Future<List<Load>> calculateResourceLoads() {
    return calculateResourceLoads(getSteps())
        .map(resourceLoads -> resourceLoads.entrySet().stream()
            .map(e -> new Load().withResource(e.getKey()).withEstimatedVirtualUnits(e.getValue()))
            .toList());
  }

  private Future<Map<ExecutionResource, Double>> calculateResourceLoads(StepGraph graph) {
    return Future.all(graph.getExecutions().stream()
        .map(execution -> execution instanceof Step step
            ? calculateResourceLoads(step)
            : calculateResourceLoads((StepGraph) execution)).toList())
        .map(cf -> {
          List<Map<ExecutionResource, Double>> loadsToAggregate = cf.list();
          Map<ExecutionResource, Double> loads = new HashMap<>();
          loadsToAggregate.forEach(load -> addLoads(loads, load, !graph.isParallel()));
          return loads;
        });
  }

  private Future<Map<ExecutionResource, Double>> calculateResourceLoads(Step step) {
    logger.info("Calculating resource loads for step {}.{} of type {} ...", getId(), step.getId(), step.getClass().getSimpleName());
    return ASYNC.run(() -> step.getAggregatedNeededResources());
  }

  public UploadUrl createUploadUrl(boolean compressed) {
    return createUploadUrl(compressed, DEFAULT_INPUT_SET_NAME);
  }

  public UploadUrl createUploadUrl(boolean compressed, String setName) {
    return new UploadUrl()
        .withCompressed(compressed)
        .withS3Key(inputS3Prefix(getId(), setName) + "/" + UUID.randomUUID() + (compressed ? ".gz" : ""));
  }

  public Future<Void> consumeInput(ModelBasedInput input) {
    if (!isPipeline())
      return Future.failedFuture(new IllegalStateException("This job does not accept ModelBasedInputs."));
    final State state = getStatus().getState();
    if (state != RUNNING)
      return Future.failedFuture(new IllegalStateException("This job can not consume any input as it is not RUNNING. Current state: " + state));
    return JobExecutor.getInstance().sendInput(this, input);
  }

  private Future<Void> deleteInputs() {
    //TODO: Asyncify!
    Input.deleteInputs(getId());
    return Future.succeededFuture();
  }

  public Future<List<Input>> loadInputs(String setName) {
    return ASYNC.run(() -> Input.loadInputs(getId(), setName));
  }

  public Future<List<Output>> loadOutputs() {
    return ASYNC.run(() -> steps.stepStream()
        .map(step -> (List<Output>) step.loadUserOutputs())
        .flatMap(ol -> ol.stream())
        .collect(Collectors.toList()));
  }

  @JsonView({Public.class})
  public boolean isResumable() {
    if (getStatus() == null || getSteps() == null)
      return false;
    return getStatus().getState().isValidSuccessor(RESUMING) && getSteps()
        .stepStream()
        .allMatch(step -> step.getStatus().getState() == CANCELLED
                || step.getStatus().getState() == SUCCEEDED
                || step.getStatus().getState() == FAILED && step.getStatus().isFailedRetryable());
  }

  public String getId() {
    return id;
  }

  public Job setId(String id) {
    this.id = id;
    return this;
  }

  public Job withId(String id) {
    setId(id);
    return this;
  }

  @JsonView(Static.class)
  public String getResourceKey() {
    //Always use key from the source except when the source is Files
    if (getSource() == null)
      return null;
    return getSource() instanceof Files<?> ? getTarget().getKey() : getSource().getKey();
  }

  public String getSecondaryResourceKey() {
    if (secondaryResourceKey != null)
      return secondaryResourceKey;

    String key = getResourceKey();
    if (key == null)
      return null;

    try {
      Extension extension = HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadSpace(key).getExtension();
      if (extension != null)
        secondaryResourceKey = extension.getSpaceId();
    }
    catch (WebClientException e) {
      //Ignore if space is not present anymore
      if (!(e instanceof ErrorResponseException errorResponseException && errorResponseException.getStatusCode() == 404))
        throw new RuntimeException(e);
    }
    return secondaryResourceKey;
  }

  private void setSecondaryResourceKey(String secondaryResourceKey) {
    this.secondaryResourceKey = secondaryResourceKey;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public Job withDescription(String description) {
    setDescription(description);
    return this;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public Job setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
    return this;
  }

  public Job withCreatedAt(long createdAt) {
    setCreatedAt(createdAt);
    return this;
  }

  public long getUpdatedAt() {
    return updatedAt;
  }

  public Job setUpdatedAt(long updatedAt) {
    this.updatedAt = updatedAt;
    return this;
  }

  public Job withUpdatedAt(long updatedAt) {
    setUpdatedAt(updatedAt);
    return this;
  }

  public long getKeepUntil() {
    return keepUntil;
  }

  public void setKeepUntil(long keepUntil) {
    if (keepUntil < 1704067200000l) //Smaller than 2024-01-01 (pre-release)
      //The value was specified in seconds. Translate it into ms.
      keepUntil *= 1000;
    //TODO: Check that the value is not larger than some allowed max period (e.g. 2yrs or so)
    this.keepUntil = keepUntil;
  }

  public Job withKeepUntil(long keepUntil) {
    setKeepUntil(keepUntil);
    return this;
  }

  public Map<String, Input> getInputs() {
    return inputs;
  }

  public void setInputs(Map<String, Input> inputs) {
    this.inputs = inputs;
  }

  public Job withInputs(Map<String, Input> inputs) {
    setInputs(inputs);
    return this;
  }

  public String getOwner() {
    return owner;
  }

  public Job setOwner(String owner) {
    this.owner = owner;
    return this;
  }

  public Job withOwner(String owner) {
    setOwner(owner);
    return this;
  }

  @JsonView(Static.class)
  public StepGraph getSteps() {
    return steps;
  }

  public Job setSteps(StepGraph steps) {
    this.steps = steps;
    return this;
  }

  public Job withSteps(StepGraph steps) {
    setSteps(steps);
    return this;
  }

  public String getExecutionId() {
    return executionId;
  }

  public Job setExecutionId(String executionId) {
    this.executionId = executionId;
    return this;
  }

  public Job withExecutionId(String executionId) {
    setExecutionId(executionId);
    return this;
  }

  public JobClientInfo getClientInfo() {
    return clientInfo;
  }

  public void setClientInfo(JobClientInfo clientInfo) {
    this.clientInfo = clientInfo;
  }

  public Job withClientInfo(JobClientInfo clientInfo) {
    setClientInfo(clientInfo);
    return this;
  }

  public ProcessDescription getProcess() {
    return process;
  }

  public void setProcess(ProcessDescription process) {
    this.process = process;
  }

  public Job withProcess(ProcessDescription process) {
    setProcess(process);
    return this;
  }

  public DatasetDescription getSource() {
    return source;
  }

  public Job setSource(DatasetDescription source) {
    this.source = source;
    return this;
  }

  public Job withSource(DatasetDescription source) {
    setSource(source);
    return this;
  }

  public DatasetDescription getTarget() {
    return target;
  }

  public Job setTarget(DatasetDescription target) {
    this.target = target;
    return this;
  }

  public Job withTarget(DatasetDescription target) {
    setTarget(target);
    return this;
  }

  public RuntimeStatus getStatus() {
    return status;
  }

  public Job setStatus(RuntimeStatus status) {
    this.status = status;
    return this;
  }

  public Job withStatus(RuntimeStatus status) {
    setStatus(status);
    return this;
  }

  @JsonIgnore
  public boolean isPipeline() {
    return getSource() instanceof DynamicStream;
  }
}
