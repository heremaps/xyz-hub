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

package com.here.xyz.jobs;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT;
import static com.here.xyz.jobs.RuntimeInfo.State.CANCELLED;
import static com.here.xyz.jobs.RuntimeInfo.State.CANCELLING;
import static com.here.xyz.jobs.RuntimeInfo.State.FAILED;
import static com.here.xyz.jobs.RuntimeInfo.State.PENDING;
import static com.here.xyz.jobs.RuntimeInfo.State.RESUMING;
import static com.here.xyz.jobs.RuntimeInfo.State.SUBMITTED;
import static com.here.xyz.jobs.steps.inputs.Input.inputS3Prefix;
import static com.here.xyz.jobs.steps.resources.Load.addLoads;
import static com.here.xyz.util.Random.randomAlpha;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.config.JobConfigClient;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.steps.JobCompiler;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.execution.JobExecutor;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.jobs.steps.resources.ExecutionResource;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.util.Async;
import com.here.xyz.util.service.Core;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@JsonInclude(NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Job implements XyzSerializable {
  //Framework defined properties:
  @JsonView(Static.class)
  private String id;
  @JsonView(Static.class)
  private RuntimeStatus status;
  @JsonView({Public.class, Static.class})
  private long createdAt;
  @JsonView({Public.class, Static.class})
  private long updatedAt;
  @JsonView({Public.class, Static.class})
  private long keepUntil;
  //Caller defined properties:
  @JsonView(Static.class)
  private String owner;
  @JsonView({Public.class, Static.class})
  private String description;
  @JsonView({Public.class, Static.class})
  private DatasetDescription source;
  @JsonView({Public.class, Static.class})
  private DatasetDescription target;
  //Framework derived properties:
  @JsonView(Static.class)
  private StepGraph steps;
  @JsonView(Static.class)
  private String executionId;

  public static final Async async = new Async(20, Core.vertx, Job.class);
  private static final Logger logger = LogManager.getLogger();

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
  public Job() {
    //Define the framework standard properties
    setId(randomAlpha());
    setStatus(new RuntimeStatus());
    setCreatedAt(Core.currentTimeMillis());
    setUpdatedAt(getCreatedAt());
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
    //TODO: Do not re-compile if the steps are set already?
    return JobCompiler.getInstance().compile(this)
        .compose(stepGraph -> {
          setSteps(stepGraph);
          return validate();
        })
        .compose(isReady -> {
          if (isReady) {
            getStatus().setState(SUBMITTED);
            return store().compose(v -> start()).map(true);
          }
          else {
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
   * Returns failed futures in case of validation errors.
   * @return true if the job is ready for execution, false otherwise
   */
  protected Future<Boolean> validate() {
    //TODO: Collect exceptions and forward them accordingly as one exception object with (potentially) multiple error objects inside
    return Future.all(Job.forEach(getSteps().stepStream().collect(Collectors.toList()), step -> validateStep(step)))
        .compose(cf -> Future.succeededFuture(cf.list().stream().allMatch(validation -> (boolean) validation)));
  }

  private static Future<Boolean> validateStep(Step step) {
    return async.run(() -> {
      boolean isReady = step.validate();
      if (isReady && step.getStatus().getState() != SUBMITTED)
        step.getStatus().setState(SUBMITTED);
      return isReady;
    });
  }

  public Future<Void> start() {
    //Is this job ready to be started?
    if (getStatus().getState() != SUBMITTED)
      return Future.failedFuture(new IllegalStateException("Job can not be started as it's not in SUBMITTED state."));

    getStatus().setState(PENDING);
    getSteps().stepStream().forEach(step -> step.getStatus().setState(PENDING));

    return store()
        .compose(v -> startExecution(false));
  }

  private Future<Void> startExecution(boolean resume) {
    /*
    Execute the step graph of this job. From now on the intrinsic state updates
    will be updated on-demand (using #updateStepStatus() either by the user or the framework) and cached in the job's step graph.
     */
    return JobExecutor.getInstance().startExecution(this, resume ? getExecutionId() : null);
  }

  public Future<Void> cancel() {
    getStatus().setState(CANCELLING);
    getSteps().stepStream().forEach(step -> getStatus().setState(CANCELLING)); //TODO: Only cancel the ones which were not succeeded yet

    return store()
        //Cancel the execution in any case, to prevent race-conditions
        .compose(v -> JobExecutor.getInstance().cancel(getExecutionId()))
        .compose(cancellingPerformed -> {
          //Execution was cancelled successfully (or was not needed). Update the job status that now is CANCELLED and store it.
          getStatus().setState(CANCELLED);

          Future<Void> resultingFuture = Future.succeededFuture();
          if (cancellingPerformed)
            //If a cancellation was actually performed, sync the step status one last time
            resultingFuture = updateStepStatus();
          else
            //If no cancellation was performed, that means the execution was not started yet, so all steps can be set to CANCELLED state.
            getSteps().stepStream().forEach(step -> getStatus().setState(CANCELLED));

          return resultingFuture
              .compose(v2 -> store());
        });
  }

  /**
   * Updates the intrinsic status of all steps and caches it on this job.
   * @return A future which succeeds when all step status has been updated successfully.
   */
  public Future<Void> updateStepStatus() {
    //TODO: Have a method JobExecutor#updateStepStatus(StepGraph, executionId) which will be called here
    return null;
  }

  public Future<Void> resume() {
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
          });
    }
    else
      return Future.failedFuture(new IllegalStateException("Job " + getId() + " is not resumable."));
  }

  public Future<Void> store() {
    //TODO: Validate changes on the job and make sure the job may be stored in the current state
    //TODO: Make sure to reflect the correct resource key
    return JobConfigClient.getInstance().storeJob(getTarget().getKey(), this);
  }

  public static Future<Job> load(String jobId) {
    return JobConfigClient.getInstance().loadJob("", jobId); //TODO: Specify resourceKey
  }

  public static Future<Void> delete(String jobId) {
    return load(jobId)
        //Delete the inputs of this job
        .compose(job -> job.deleteInputs().map(job))
        //Delete the outputs of all involved steps
        .compose(job -> Future.all(Job.<Step, Boolean>forEach(job.getSteps().stepStream().collect(Collectors.toList()), step -> deleteStepOutputs(step))).mapEmpty())
        //Now finally delete this job's configuration
        .compose(v -> JobConfigClient.getInstance().deleteJob("", jobId).mapEmpty()); //TODO: Specify resourceKey
  }

  private static Future<Boolean> deleteStepOutputs(Step step) {
    return async.run(() -> {
      step.deleteOutputs();
      return null;
    });
  }

  /**
   * Calculates the overall loads (needed resources) of this job by aggregating the resource loads of all steps of this job.
   * The aggregation of parallel steps is done in the way that all resource-loads of parallel running steps will be added while
   * in the case of sequentially running steps always the maximum of each pairwise equal resources will be taken into account.
   *
   * @return A list of overall resource-loads being reserved by this job
   */
  public List<Load> calculateResourceLoads() {
    //TODO: Run asynchronous!
    return calculateResourceLoads(getSteps())
        .entrySet()
        .stream()
        .map(e -> new Load().withResource(e.getKey()).withEstimatedVirtualUnits(e.getValue())).collect(Collectors.toList());
  }

  private Map<ExecutionResource, Double> calculateResourceLoads(StepGraph graph) {
    //TODO: Run asynchronous!
    Map<ExecutionResource, Double> loads = new HashMap<>();
    graph.getExecutions().forEach(execution -> addLoads(loads, execution instanceof Step step
        ? calculateResourceLoads(step)
        : calculateResourceLoads((StepGraph) execution), !graph.isParallel()));
    return loads;
  }

  private Map<ExecutionResource, Double> calculateResourceLoads(Step step) {
    //TODO: Asyncify!
    logger.info("Calculating resource loads for step {}.{} of type {} ...", getId(), step.getId(), step.getClass().getSimpleName());
    return step.getAggregatedNeededResources();
  }

  public UploadUrl createUploadUrl() {
    return new UploadUrl().withS3Key(inputS3Prefix(getId()) + "/" + UUID.randomUUID());
  }

  private Future<Void> deleteInputs() {
    //TODO: implement
    return null;
  }

  public Future<List<Input>> loadInputs() {
    return AsyncS3Client.getInstance()
        .scanFolderAsync(Input.inputS3Prefix(getId()))
        .map(summaries -> summaries.stream()
                .map(s3ObjectSummary -> new UploadUrl().withS3Key(s3ObjectSummary.getKey()).withByteSize(s3ObjectSummary.getSize()))
                .collect(Collectors.toList()));
  }

  public Future<List<Output>> loadOutputs() {
    //TODO: Make this asynchronously
    List<Output> outputs =  steps.stepStream()
            .map(step -> (List<Output>) step.loadOutputs(true))
            .flatMap(ol -> ol.stream())
            .collect(Collectors.toList());
    return Future.succeededFuture(outputs);
  }

  public boolean isResumable() {
    return getStatus().getState().isValidSuccessor(RESUMING) && getSteps()
        .stepStream()
        .allMatch(step -> step.getStatus().getState() == CANCELLED || step.getStatus().getState() == FAILED && step.isFailedRetryable());
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
    this.keepUntil = keepUntil;
  }

  public Job withKeepUntil(long keepUntil) {
    setKeepUntil(keepUntil);
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
}
