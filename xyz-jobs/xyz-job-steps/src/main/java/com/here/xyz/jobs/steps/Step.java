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

package com.here.xyz.jobs.steps;

import static com.here.xyz.jobs.steps.Step.InputSet.USER_PROVIDER;
import static com.here.xyz.jobs.steps.Step.Visibility.USER;
import static com.here.xyz.jobs.steps.inputs.Input.defaultBucket;
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
import com.here.xyz.jobs.steps.execution.DelegateStep;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.execution.RunEmrJob;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.inputs.InputFromOutput;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.ModelBasedOutput;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.jobs.steps.resources.ExecutionResource;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.util.S3Client;
import com.here.xyz.util.service.aws.S3Uri;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = LambdaBasedStep.class),
    @JsonSubTypes.Type(value = RunEmrJob.class),
    @JsonSubTypes.Type(value = DelegateStep.class)
})
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_DEFAULT)
public abstract class Step<T extends Step> implements Typed, StepExecution {
  private static final Logger logger = LogManager.getLogger();

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
  protected List<OutputSet> outputSets = List.of();
  @JsonView({Internal.class, Static.class})
  private List<InputSet> inputSets = List.of();
  @JsonView({Internal.class, Static.class})
  private Map<String, String> outputMetadata;
  @JsonView({Internal.class, Static.class})
  private boolean notReusable = false;

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

  protected String bucketRegion() {
    return Config.instance.AWS_REGION; //TODO: Get from bucket accordingly
  }

  private String stepS3Prefix() {
    return jobId + "/" + getId();
  }

  protected void registerOutputs(List<Output> outputs, String outputSetName) throws IOException {
    registerOutputs(outputs, getOutputSet(outputSetName));
  }

  /**
   * Can be called (multiple times) by an implementing subclass to register a list of outputs which have been produced as an outcome of
   * the step execution. The framework will care about storing the outputs and providing them to the later steps intending to consume
   * these outputs.
   * The following step will be able to access these outputs using the {@link #loadInputs(Class[])} method.
   *
   * In (potential) later executions of this step (e.g., in case of resume) this step can access outputs
   * which have been formerly registered by using the method {@link #loadOutputs(String)}.
   *
   * @param outputs The list of outputs to be registered for this step
   * @param outputSet The output set for which to register the outputs
   */
  protected void registerOutputs(List<Output> outputs, OutputSet outputSet) throws IOException {
    for (int i = 0; i < outputs.size(); i++) {
      final Output output = outputs.get(i);
      if (outputSet.modelBased && !(output instanceof ModelBasedOutput))
        throw new IllegalArgumentException("Can not register output of type " + output.getClass().getSimpleName() + " as the output set "
            + outputSet.name + " does not accept model based outputs.");
      if (!outputSet.modelBased && output instanceof ModelBasedOutput)
        throw new IllegalArgumentException("Can not register output of type " + output.getClass().getSimpleName() + " as the output set "
            + outputSet.name + " does only accept model based outputs.");
      output.store(toS3Path(outputSet) + "/" + (output.getFileName() != null ? output.getFileName() : (UUID.randomUUID() + outputSet.fileSuffix)));
    }
  }

  public List<Output> loadUserOutputs() {
    return loadOutputs(USER);
  }

  public List<Output> loadOutputs(Visibility visibility) {
    return outputSets.stream()
        .filter(outputSet -> outputSet.visibility == visibility)
        .flatMap(outputSet -> loadStepOutputs(outputSet).stream())
        .toList();
  }

  protected List<Output> loadOutputs(String outputSetName) {
    return loadStepOutputs(getOutputSet(outputSetName));
  }

  public OutputSet getOutputSet(String outputSetName) {
    try {
      return outputSets.stream().filter(set -> set.name.equals(outputSetName)).findFirst().get();
    }
    catch (NoSuchElementException e) {
      throw new IllegalArgumentException("No outputSet was found with name: " + outputSetName);
    }
  }

  /**
   * Loads the outputs that have been created by this step (so far).
   * This method could be used in case of a resume (see: {@link #execute(boolean)}) is being performed
   * e.g., to check a previous state of the progress.
   * @param outputSet The outputSet for which to load the outputs
   * @return The outputs that have been registered for the specified outputSet (so far).
   */
  private List<Output> loadStepOutputs(OutputSet outputSet) {
    return loadOutputs(Set.of(toS3Path(outputSet)), outputSet.modelBased);
  }

  private List<Output> loadOutputs(Set<String> s3Prefixes, boolean modelBased) {
    return s3Prefixes
        .stream()
        //TODO: Scan the different folders in parallel
        .flatMap(s3Prefix -> S3Client.getInstance().scanFolder(s3Prefix)
            .stream()
            .filter(s3ObjectSummary -> s3ObjectSummary.size() > 0)
            .map(s3ObjectSummary -> modelBased
                ? ModelBasedOutput.load(s3ObjectSummary.key(), outputMetadata)
                : new DownloadUrl()
                    .withS3Key(s3ObjectSummary.key())
                    .withByteSize(s3ObjectSummary.size())
                    .withMetadata(outputMetadata)))
        .collect(Collectors.toList());
  }

  /**
   * NOTE: Calling this method may block the execution for some time, depending on the number of inputs to be listed.
   * That's the case because the metadata for each input has to be requested separately.
   * TODO: Remove loading the native S3 metadata mentioned above
   *
   * Loads the inputs of this step.
   * That could be inputs being provided by the user or outputs of prior steps.
   * The result will only contain inputs that match (one of) the provided input type(s).
   *
   * @return All inputs for this step, filtered by the specified input type(s).
   */
  protected List<Input> loadInputs(Class<? extends Input>... inputTypes) {
    logger.info("[{}] Loading job inputs ...", getGlobalStepId());
    if (inputs == null) {
      inputs = new LinkedList<>();
      for (InputSet inputSet : inputSets) {
        //TODO: load the different inputSets in parallel
        inputs.addAll(loadInputs(inputSet));
      }
    }
    return filterInputs(inputs, inputTypes);
  }

  /**
   * Loads the inputs for the specified {@link InputSet} of this step.
   * That could be inputs being provided by the user or outputs of prior steps.
   * The result will only contain inputs that match (one of) the provided input type(s).
   *
   * @return All inputs for the specified {@link InputSet} of this step, filtered by the specified input type(s).
   */
  protected List<Input> loadInputs(InputSet inputSet, Class<? extends Input>... inputTypes) {
    return filterInputs(loadInputs(inputSet), inputTypes);
  }

  /**
   * Loads the inputs of a previous step for the specified {@link InputSet}.
   * This is an internal helper method that should never be called directly by any implementing subclass.
   *
   * @param inputSet
   * @return All inputs from the specified InputSet
   */
  private List<Input> loadInputs(InputSet inputSet) {
    if (inputSet.providerId == null)
      throw new IllegalArgumentException("Incorrect input was provided: Missing source input provider");
    if (inputSet.name == null)
      throw new IllegalArgumentException("Incorrect input was provided: Missing referenced set name");

    if (USER_PROVIDER.equals(inputSet.providerId))
      return Input.loadInputs(getJobId(), inputSet.name);
    else
      return loadOutputsFor(inputSet).stream().map(output -> (Input) transformToInput(output).withMetadata(inputSet.metadata())).toList();
  }

  /**
   * Loads the outputs of a previous step for the specified inputSet.
   * This is an internal helper method that should never be called directly by any implementing subclass.
   *
   * @param inputSet The inputSet from which to load the inputs.
   *  That inputSet can depict the user inputs or the outputs of a previous step in the same job or in another succeeded
   *  job that ran earlier.
   * @return All outputs for the specified InputSet
   */
  private List<Output> loadOutputsFor(InputSet inputSet) {
    return loadOutputs(Set.of(inputSet.toS3Path(jobId)), inputSet.modelBased());
  }

  private static List<Input> filterInputs(List<Input> inputs, Class<? extends Input>[] inputTypes) {
    if (inputTypes.length == 0)
      return inputs;
    return inputs.stream()
        .filter(input -> Arrays.stream(inputTypes)
            .anyMatch(inputType -> input.getClass().isAssignableFrom(inputType)))
        .toList();
  }

  //TODO: Remove that workaround once the inputs & outputs were streamlined to one single inheritance chain
  private static Input transformToInput(Output output) {
    if (output instanceof S3DataFile s3File)
      return new UploadUrl()
          .withS3Bucket(s3File.getS3Bucket())
          .withS3Key(s3File.getS3Key())
          .withCompressed(s3File.isCompressed())
          .withByteSize(s3File.getByteSize());
    else
      return new InputFromOutput().withDelegate(output);
  }

  protected int currentInputsCount(Class<? extends Input> inputType) {
    return getInputSets().stream()
        .filter(inputSet -> USER_PROVIDER.equals(inputSet.providerId))
        .mapToInt(userInputSet -> Input.currentInputsCount(jobId, inputType, userInputSet.name))
        .sum();
  }

  protected <I extends Input> List<I> loadInputsSample(int maxSampleSize, Class<I> inputType) {
    return getInputSets().stream()
        .filter(inputSet -> USER_PROVIDER.equals(inputSet.providerId))
        .flatMap(userInputSet -> Input.loadInputsSample(jobId, userInputSet.name, maxSampleSize, inputType).stream())
        .unordered()
        .limit(maxSampleSize)
        .toList();
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
   * NOTE: If <code>resume</code> is <code>true</code>, the implementation of the step might have to perform
   * some additional cleanup or preparations before starting the execution again from a feasible resumption point.
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
   * @param resume `true` if the execution is a resumption, that is if the execution is *not* called for the first time
   * @throws Exception Any kind of execution that was not caught by the step implementation
   */
  public abstract void execute(boolean resume) throws Exception;

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

  /**
   * Will be called right after the job compilation, but just before the step validation call.
   * Can be used to perform some preparations for the step that are already executed in the service node.
   * NOTE: That means this method will not run within the target runtime environment!
   * The execution of this method must not take a long time (e.g., <2s), because it would block
   * the job-creation response that is about to be sent to the user that creates the job.
   *
   * @param owner
   * @param ownerAuth
   */
  public void prepare(String owner, JobClientInfo ownerAuth) throws ValidationException {
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

  /**
   * Should be overridden in subclasses to enable the possibility to check whether two steps of two different StepGraphs
   * are equivalent in their actions and their outcome.
   * That means that the other step would produce exactly the same outputs
   * for the provided inputs and step-parameters / step-fields (step-settings).
   *
   * Not overriding this method for a step, means that this step will never be found to be equivalent to some other provided step.
   * That could be the case if a step implementation cannot guarantee to provide the same outputs again by any condition.
   *
   * @param other The step of a different StepGraph
   * @return `true` only if this step and the provided ones produce the same outputs for the same inputs & settings
   */
  @Override
  public boolean isEquivalentTo(StepExecution other) {
    return !(other instanceof Step) || other instanceof DelegateStep ? other.isEquivalentTo(this) : false;
  }

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

  public T withJobId(String jobId) {
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

  /**
   * A little helper method to quickly find out whether this step will directly use user inputs or not.
   * @return Whether this step depends on user outputs or not.
   */
  public boolean usesUserInput() {
    return inputSets.stream().anyMatch(inputSet -> inputSet.providerId == null);
  }

  public List<OutputSet> getOutputSets() {
    return new ArrayList<>(outputSets);
  }

  @JsonIgnore
  protected void setOutputSets(List<OutputSet> outputSets) {
    outputSets.forEach(outputSet -> outputSet.setStepId(getId()));
    this.outputSets = outputSets;
  }

  public List<InputSet> getInputSets() {
    return inputSets;
  }

  public void setInputSets(List<InputSet> inputSets) {
    this.inputSets = inputSets;
  }

  public T withInputSets(List<InputSet> inputSets) {
    setInputSets(inputSets);
    return (T) this;
  }

  public T withOutputSetVisibility(String outputSetName, Visibility visibility) {
    OutputSet outputSet = getOutputSet(outputSetName);
    outputSet.visibility = visibility;
    return (T) this;
  }

  public Map<String, String> getOutputMetadata() {
    return outputMetadata;
  }

  public void setOutputMetadata(Map<String, String> outputMetadata) {
    this.outputMetadata = outputMetadata;
  }

  public T withOutputMetadata(Map<String, String> metadata) {
    setOutputMetadata(metadata);
    return (T) this;
  }

  public boolean isNotReusable() {
    return notReusable;
  }

  public void setNotReusable(boolean notReusable) {
    this.notReusable = notReusable;
  }

  public T withNotReusable(boolean notReusable) {
    setNotReusable(notReusable);
    return (T) this;
  }

  @JsonIgnore
  protected boolean isUserInputsExpected() {
    return getInputSets().stream().anyMatch(inputSet -> USER_PROVIDER.equals(inputSet.providerId));
  }

  @JsonIgnore
  protected boolean isUserInputsPresent(Class<? extends Input> inputType) {
    return currentInputsCount(inputType) > 0;
  }

  /**
   * Use this constructor to reference the outputs of a step belonging to a different job than the one the consuming step belongs to.
   * @param jobId The other job's id
   * @param providerId The ID of the entity that provided the inputs (e.g., a step ID or "USER")
   * @param name The name for the set of outputs to be produced
   */
  public record InputSet(String jobId, String providerId, String name, boolean modelBased, Map<String, String> metadata) {
    public static final String DEFAULT_INPUT_SET_NAME = "inputs"; //Depicts the input set used if no set name is defined
    public static final String USER_PROVIDER = "USER";
    public static final Supplier<InputSet> USER_INPUTS = () -> new InputSet();

    public InputSet(String jobId, String providerId, String name, boolean modelBased) {
      this(jobId, providerId, name, modelBased, null);
    }

    /**
     * Use this constructor to reference the outputs of a step belonging to the same job as the consuming step.
     * @param providerId
     * @param name
     */
    public InputSet(String providerId, String name, boolean modelBased) {
      this(null, providerId, name, modelBased);
    }

    public InputSet(OutputSet outputSetOfOtherStep) {
      this(outputSetOfOtherStep.getStepId(), outputSetOfOtherStep.name, outputSetOfOtherStep.modelBased);
    }

    public InputSet(OutputSet outputSetOfOtherStep, Map<String, String> metadata) {
      this(outputSetOfOtherStep.getJobId(), outputSetOfOtherStep.getStepId(), outputSetOfOtherStep.name, outputSetOfOtherStep.modelBased,
          metadata);
    }

    /**
     * Use this constructor to depict the global / user inputs of the same job the consuming step belongs to.
     */
    public InputSet() {
      //TODO: Currently only non-modelbased user inputs are supported
      this(null, USER_PROVIDER, DEFAULT_INPUT_SET_NAME, false);
    }

    public String toS3Path(String consumerJobId) {
      return toS3Uri(consumerJobId).key();
    }

    public S3Uri toS3Uri(String consumerJobId) {
      String jobId = this.jobId != null ? this.jobId : consumerJobId;
      if (USER_PROVIDER.equals(providerId))
        return Input.loadResolvedUserInputPrefixUri(jobId, name);
      return new S3Uri(defaultBucket(), Output.stepOutputS3Prefix(jobId, providerId, name));
    }
  }

  protected String toS3Path(OutputSet outputSet) {
    return Output.stepOutputS3Prefix(outputSet.jobId != null ? outputSet.jobId : getJobId(),
        outputSet.stepId != null ? outputSet.stepId : getId(), outputSet.name);
  }

  @JsonInclude(Include.NON_DEFAULT)
  public static class OutputSet {
    private String jobId;
    private String stepId;
    public String name;
    public Visibility visibility;
    public String fileSuffix;
    public boolean modelBased;

    private OutputSet() {} //NOTE: Only needed for deserialization purposes

    public OutputSet(String name, Visibility visibility, String fileSuffix) {
      this.name = name;
      this.visibility = visibility;
      this.fileSuffix = fileSuffix;
    }

    public OutputSet(String name, Visibility visibility, boolean modelBased) {
      this(name, visibility, ".json");
      this.modelBased = modelBased;
    }

    public OutputSet(OutputSet other, String jobId, Visibility visibility) {
      this(other.name, visibility, other.fileSuffix);
      this.modelBased = other.modelBased;
      this.jobId = jobId;
      this.stepId = other.stepId;
    }

    public String getJobId() {
      return jobId;
    }

    public void setJobId(String jobId) {
      this.jobId = jobId;
    }

    public OutputSet withJobId(String jobId) {
      setJobId(jobId);
      return this;
    }

    public String getStepId() {
      return stepId;
    }

    public void setStepId(String stepId) {
      this.stepId = stepId;
    }

    public OutputSet withStepId(String stepId) {
      setStepId(stepId);
      return this;
    }

    @Override
    public final boolean equals(Object o) {
      if (!(o instanceof Step.OutputSet that))
        return false;

      return Objects.equals(jobId, that.jobId) && Objects.equals(stepId, that.stepId) && name.equals(that.name)
          && visibility == that.visibility && fileSuffix.equals(that.fileSuffix);
    }
  }

  public enum Visibility {
    SYSTEM,
    USER
  }
}
