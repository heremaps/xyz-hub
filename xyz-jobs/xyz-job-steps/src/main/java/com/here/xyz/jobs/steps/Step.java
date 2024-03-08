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

import static com.here.xyz.jobs.steps.resources.Load.addLoad;
import static com.here.xyz.util.Random.randomAlpha;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.Typed;
import com.here.xyz.jobs.RuntimeInfo;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.jobs.steps.resources.ExecutionResource;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@JsonSubTypes({
    @JsonSubTypes.Type(value = LambdaBasedStep.class)
})
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class Step<T extends Step> implements Typed, StepExecution {
  @JsonView({Public.class, Static.class})
  private String id = "s_" + randomAlpha(6);
  private String jobId;
  private String previousStepId;
  @JsonView({Public.class, Static.class})
  boolean failedRetryable;

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

  public abstract RuntimeInfo getStatus();

  public abstract int getTimeoutSeconds();

  protected String bucketName() {
    return Config.instance.JOBS_S3_BUCKET;
  }

  protected String bucketRegion() {
    return Config.instance.JOBS_REGION; //TODO: Get from bucket accordingly
  }

  private String stepS3Prefix(boolean previousStep) {
    return jobId + "/" + (previousStep ? previousStepId : id);
  }

  protected final String inputS3Prefix() {
    return Input.inputS3Prefix(jobId);
  }

  protected final String outputS3Prefix(boolean previousStep, boolean userOutput) {
    return stepS3Prefix(previousStep) + "/outputs" + (userOutput ? "/user" : "/system");
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
  protected void registerOutputs(List<Output> outputs, boolean userOutput) {
    for (int i = 0; i < outputs.size(); i++)
      outputs.get(i).store(outputS3Prefix(false, userOutput) + "output" + i + ".json"); //TODO: Use proper file name
  }

  protected List<Output> loadPreviousOutputs(boolean userOutput) {
    return loadOutputs(true, userOutput);
  }

  protected List<Output> loadOutputs(boolean userOutput) {
    return loadOutputs(false, userOutput);
  }

  private List<Output> loadOutputs(boolean previousStep, boolean userOutput) {
    return S3Client.getInstance().scanFolder(outputS3Prefix(previousStep, userOutput))
        .stream()
        .map(s3ObjectSummary -> new DownloadUrl().withS3Key(s3ObjectSummary.getKey()).withByteSize(s3ObjectSummary.getSize()))
        .collect(Collectors.toList());
  }

  /**
   * NOTE: Calling this method may block the execution for some time, depending on the number of inputs to be listed.
   * That's the case because the metadata for each input has to be requested separately.
   * @return
   */
  protected List<Input> loadInputs() {
    return S3Client.getInstance().scanFolder(inputS3Prefix())
        .stream()
        .map(s3ObjectSummary -> new UploadUrl()
            .withS3Key(s3ObjectSummary.getKey())
            .withByteSize(s3ObjectSummary.getSize())
            .withCompressed(inputIsCompressed(s3ObjectSummary.getKey())))
        .collect(Collectors.toList());

    //TODO: Run metadata retrieval requests partially in parallel in multiple threads
  }

  private boolean inputIsCompressed(String s3Key) {
    ObjectMetadata metadata = S3Client.getInstance().loadMetadata(s3Key);
    return metadata.getContentEncoding() != null && metadata.getContentEncoding().equalsIgnoreCase("gzip");
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
  public abstract void deleteOutputs();

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

  public boolean isFailedRetryable() {
    return failedRetryable;
  }

  public void setFailedRetryable(boolean failedRetryable) {
    this.failedRetryable = failedRetryable;
  }

  public Step withFailedRetryable(boolean failedRetryable) {
    setFailedRetryable(failedRetryable);
    return this;
  }
}
