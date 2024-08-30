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

package com.here.xyz.jobs.steps;

import static com.here.xyz.jobs.steps.outputs.Output.MODEL_BASED_PREFIX;
import static com.here.xyz.jobs.steps.resources.Load.addLoad;
import static com.here.xyz.util.Random.randomAlpha;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.Typed;
import com.here.xyz.jobs.JobClientInfo;
import com.here.xyz.jobs.RuntimeInfo;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.execution.RunEmrJob;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.ModelBasedOutput;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.jobs.steps.resources.ExecutionResource;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.util.S3Client;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = LambdaBasedStep.class),
    @JsonSubTypes.Type(value = RunEmrJob.class)
})
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_DEFAULT)
public abstract class Step<T extends Step> implements Typed, StepExecution {

  //TODO: Apply views properly to all properties

  @JsonView({Internal.class, Static.class})
  private long estimatedUploadBytes = -1;
  @JsonView({Internal.class, Static.class})
  private float estimationFactor = 1f;
  @JsonView({Public.class, Static.class})
  private String id = "s_" + randomAlpha(6);
  private String jobId;
  private Set<String> previousStepIds = Set.of();
  private RuntimeInfo status = new RuntimeInfo();
  @JsonIgnore
  private List<Input> inputs;
  @JsonView({Internal.class, Static.class})
  private boolean pipeline;
  @JsonView({Internal.class, Static.class})
  private boolean useSystemInput;

  /**
   * Provides a list of the resource loads which will be consumed by this step during its execution.
   * If no shared resources are utilized, an empty list may be returned.
   *
   * NOTE: This method will not be called inside the execution runtime environment, but within the Job Service directly.
   *
   * @return A list of all loads being sparked on shared resources
   */
  @JsonIgnore
  public abstract List<Load> getNeededResources();

  @JsonIgnore
  public Map<ExecutionResource, Double> getAggregatedNeededResources() {
    //TODO: asyncify the call to getNeededResources()
    List<Load> neededResources = getNeededResources();
    if (neededResources == null)
      return Collections.emptyMap();

    Map<ExecutionResource, Double> loads = new HashMap<>();
    neededResources.forEach(load -> addLoad(loads, load, false));
    return loads;
  }

  public RuntimeInfo getStatus() {
    //TODO: Called by the framework node to get the (previously updated & cached) step state .. Status updates come through CW event bridge
    return status;
  }

  /**
   * This method might be called multiple times prior to the execution of this step.
   * This method should be implemented in a way to make sure that all calls will always return the same value for the same step
   * configuration. E.g., the calculated value should be stored inside a private field of this step.
   *
   * @return A feasible maximum execution time. Steps that are exceeding their timeout will fail.
   */
  @JsonIgnore
  public abstract int getTimeoutSeconds();

  /**
   * This method might be called multiple times during the preparation and the execution of this step.
   * This method should not perform any heavy operations. It should return quickly.
   * If applicable, the calculated value should be stored inside a private field of this step.
   *
   * @return An estimation for the execution time in seconds that should be calculated once prior to the execution.
   */
  @JsonIgnore
  public abstract int getEstimatedExecutionSeconds();

  protected String bucketName() {
    return Config.instance.JOBS_S3_BUCKET;
  }

  protected String bucketRegion() {
    return Config.instance.AWS_REGION; //TODO: Get from bucket accordingly
  }

  private String stepS3Prefix() {
    return jobId + "/" + getId();
  }

  private Set<String> previousS3Prefixes() {
    return getPreviousStepIds().stream().map(previousStepId -> jobId + "/" + previousStepId).collect(Collectors.toSet());
  }

  protected final String inputS3Prefix() {
    return Input.inputS3Prefix(jobId);
  }

  protected final String outputS3Prefix(boolean userOutput, boolean onlyModelBased) {
    return outputS3Prefix(stepS3Prefix(), userOutput, onlyModelBased);
  }

  protected final String outputS3Prefix(String stepS3Prefix, boolean userOutput, boolean onlyModelBased) {
    return Output.stepOutputS3Prefix(stepS3Prefix, userOutput, onlyModelBased);
  }

  protected final Set<String> previousOutputS3Prefixes(boolean userOutput, boolean onlyModelBased) {
    return previousS3Prefixes().stream().map(previousStepPrefix -> outputS3Prefix(previousStepPrefix, userOutput, onlyModelBased))
        .collect(Collectors.toSet());
  }

  /**
   * Can be called (multiple times) by an implementing subclass to register a list of outputs which have been produced as an outcome of
   * the step execution. The framework will care about storing the outputs and providing them to the step following this one.
   * The following step will be able to access these outputs using the {@link #loadPreviousOutputs(boolean)} method.
   *
   * In (potential) later executions of this step (e.g., in case of resume) this step can access outputs
   * which have been formerly registered by using the method {@link #loadOutputs(boolean)}.
   *
   * @param outputs The list of outputs to be registered for this step
   * @param userOutput Whether the specified outputs should be visible to the user (or just to the system)
   */
  protected void registerOutputs(List<Output> outputs, boolean userOutput) throws IOException {
    for (int i = 0; i < outputs.size(); i++)
      outputs.get(i).store(outputS3Prefix(stepS3Prefix(), userOutput, outputs.get(i) instanceof ModelBasedOutput)
          + "/" + UUID.randomUUID() + ".json"); //TODO: Use proper file suffix & content type
  }

  protected List<Output> loadPreviousOutputs(boolean userOutput) {
    return loadPreviousOutputs(userOutput, Output.class);
  }

  protected List<Output> loadPreviousOutputs(boolean userOutput, Class<? extends Output> type) {
    return loadOutputs(true, userOutput);
  }

  public List<Output> loadOutputs(boolean userOutput) {
    return loadOutputs(false, userOutput);
  }

  private List<Output> loadOutputs(boolean previousStep, boolean userOutput) {
    Set<String> s3Prefixes = previousStep ? previousOutputS3Prefixes(userOutput, false)
        : Set.of(outputS3Prefix(userOutput, false));

    return s3Prefixes
        .stream()
        //TODO: Scan the different folders in parallel
        .flatMap(s3Prefix -> S3Client.getInstance().scanFolder(s3Prefix)
            .stream()
            .map(s3ObjectSummary -> s3ObjectSummary.getKey().contains(MODEL_BASED_PREFIX)
                ? ModelBasedOutput.load(s3ObjectSummary.getKey())
                : new DownloadUrl().withS3Key(s3ObjectSummary.getKey()).withByteSize(s3ObjectSummary.getSize())))
        .collect(Collectors.toList());
  }

  protected List<S3DataFile> loadStepInputs() {
    return useSystemInput
        ? loadPreviousOutputs(false).stream().map(output -> (S3DataFile) output).toList()
        : loadInputs().stream().map(output -> (S3DataFile) output).toList();
  }

  /**
   * NOTE: Calling this method may block the execution for some time, depending on the number of inputs to be listed.
   * That's the case because the metadata for each input has to be requested separately.
   * @return
   */
  protected List<Input> loadInputs() {
    if (inputs == null)
      inputs = Input.loadInputs(getJobId());
    return inputs;
  }

  protected int currentInputsCount(Class<? extends Input> inputType) {
    return Input.currentInputsCount(jobId, inputType);
  }

  protected <I extends Input> List<I> loadInputsSample(int maxSampleSize, Class<I> inputType) {
    return Input.loadInputsSample(jobId, maxSampleSize, inputType);
  }

  @JsonIgnore
  public abstract String getDescription();

  /**
   * If applicable for the step-implementation, this method will be called on the target execution system to execute the actual
   * task which this step is depicting.
   * Implementing subclasses may expect parameters needed to fulfill their task.
   * These parameters can be provided in the form of public fields / getter-setter-pairs.
   * The job framework will make sure that these parameters will be transferred
   * to the target execution system so that this method will be able to access them.
   *
   * @example
   *
   * <pre>
   * public class SampleStep extends Step {
   *
   *   public String someParameter1;
   *   public int someParameter2;
   *
   *   ...
   *
   *   @Override
   *   public void execute() {
   *     String someParameter = getSomeParameter1();
   *     int someOtherParameter = getSomeParameter2();
   *
   *     ... Execute some task
   *   }
   *
   *   ...
   *
   * }
   * </pre>
   *
   * @throws Exception Any kind of execution that was not caught by the step implementation
   */
  public abstract void execute() throws Exception;

  /**
   * Same semantics as in {@link #execute()}, but this method will be called in case of a resumption
   * of the step after it has been canceled / failed previously.
   *
   * NOTE: The implementation of the step might have to perform some additional cleanup before
   *  starting the execution again from a feasible resumption point.
   *
   * @throws Exception Any kind of execution that was not caught by the step implementation
   */
  public abstract void resume() throws Exception;

  /**
   * Executes all steps that are necessary to cancel a current execution of this step.
   * This method will be called by the framework
   * if the job containing this step is canceled by the user or by the framework.
   * If this step actually is currently not running, the call may be ignored by implementing subclasses.
   * If there is some processing currently going on, the step-implementation should care about stopping all related tasks / resources.
   * If the internally running tasks could not safely be stopped for some reason, implementing subclasses should throw an exception
   * to inform the framework that there was an issue while cancelling.
   * This could be important for the communication to the job-owner
   * as this step would go into the FAILED state and would be marked as not being failedRetryable such a case.
   */
  public abstract void cancel() throws Exception;

  /**
   * Cleans up all outputs that have been generated by this step.
   * For outputs which are still in use by other steps, the reference-counter is decreased by 1 in the output's metadata.
   *
   * NOTE: This method will not be called inside the execution runtime environment, but within the Job Service directly.
   *
   * @return
   */
  public void deleteOutputs() {
    /*
    TODO: Respect re-usability of outputs. If other steps are re-using (some) outputs of this step, check the reference counter of
      the according steps and only delete the outputs if their reference counter drops to 0
     */
    S3Client.getInstance().deleteFolder(stepS3Prefix());
  }

  public void prepare(String owner, JobClientInfo ownerAuth) {
    //Nothing to do by default. May be overridden.
  }

  /**
   * Checks if all pre-conditions are met.
   * Throws a {@link ValidationException} if some parameters are invalid.
   * Returns <code>false</code> if not all pre-conditions are met (e.g., inputs missing) yet, <code>true</code> otherwise.
   *
   * NOTE: This method will not be called inside the execution runtime environment, but within the Job Service directly.
   *
   * NOTE: This method could be called multiple times if the input state of the containing job
   * was changed by the user again (e.g., missing inputs have been provided, parameters have been changed).
   * In that case, re-validation may be necessary.
   *
   * @return true if the step is ready for execution, false otherwise
   * @throws ValidationException If one or multiple parameters of the step are invalid
   */
  public abstract boolean validate() throws ValidationException;

  public String getId() {
    return id;
  }

  void setId(String id) {
    this.id = id;
  }

  T withId(String id) {
    setId(id);
    return (T) this;
  }

  public String getJobId() {
    return jobId;
  }

  void setJobId(String jobId) {
    this.jobId = jobId;
  }

  T withJobId(String jobId) {
    setJobId(jobId);
    return (T) this;
  }

  @JsonIgnore
  public String getGlobalStepId() {
    return getJobId() + "." + getId();
  }

  public Set<String> getPreviousStepIds() {
    return previousStepIds;
  }

  public void setPreviousStepIds(Set<String> previousStepIds) {
    this.previousStepIds = previousStepIds;
  }

  public T withPreviousStepIds(Set<String> previousStepId) {
    setPreviousStepIds(previousStepId);
    return (T) this;
  }

  /**
   * Helper which returns an estimation of uncompressed byte size of all available uploads.
   * @return The sum of uncompressed bytes of all uploaded input files
   */
  @JsonIgnore
  public long getUncompressedUploadBytesEstimation() {
    return estimatedUploadBytes != -1 ? estimatedUploadBytes
        : (estimatedUploadBytes = (long) Math.ceil(loadInputs()
            .stream()
            .mapToLong(input -> input instanceof UploadUrl uploadUrl ? uploadUrl.getEstimatedUncompressedByteSize() : 0)
            .sum() * estimationFactor));
  }

  @JsonIgnore
  public void setUncompressedUploadBytesEstimation(long uncompressedUploadBytesEstimation) {
    estimatedUploadBytes = uncompressedUploadBytesEstimation;
  }

  public T withUncompressedUploadBytesEstimation(long uncompressedUploadBytesEstimation) {
    setUncompressedUploadBytesEstimation(uncompressedUploadBytesEstimation);
    return (T) this;
  }

  public float getEstimationFactor() {
    return estimationFactor;
  }

  public void setEstimationFactor(float estimationFactor) {
    this.estimationFactor = estimationFactor;
  }

  public T withEstimationFactor(float estimationFactor) {
    setEstimationFactor(estimationFactor);
    return (T) this;
  }

  public boolean isPipeline() {
    return pipeline;
  }

  public void setPipeline(boolean pipeline) {
    this.pipeline = pipeline;
  }

  public T withPipeline(boolean pipeline) {
    setPipeline(pipeline);
    return (T) this;
  }

  public boolean isUseSystemInput() {
    return useSystemInput;
  }

  public void setUseSystemInput(boolean useSystemInput) {
    this.useSystemInput = useSystemInput;
  }

  public T withUseSystemInput(boolean useSystemInput) {
    setUseSystemInput(useSystemInput);
    return (T) this;
  }
}
