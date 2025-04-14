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

package com.here.xyz.jobs.steps.execution;

import static com.here.xyz.jobs.RuntimeInfo.State.CANCELLED;
import static com.here.xyz.jobs.RuntimeInfo.State.CANCELLING;
import static com.here.xyz.jobs.RuntimeInfo.State.FAILED;
import static com.here.xyz.jobs.RuntimeInfo.State.RUNNING;
import static com.here.xyz.jobs.RuntimeInfo.State.SUCCEEDED;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.ExecutionMode.SYNC;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.START_EXECUTION;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.STATE_CHECK;
import static com.here.xyz.jobs.steps.execution.StepException.codeFromErrorErrorResponseException;
import static com.here.xyz.jobs.util.AwsClients.cloudwatchEventsClient;
import static com.here.xyz.jobs.util.AwsClients.sfnClient;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.STREAM_ID;
import static software.amazon.awssdk.services.cloudwatchevents.model.RuleState.ENABLED;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.ProcessUpdate;
import com.here.xyz.jobs.steps.execution.db.DatabaseBasedStep;
import com.here.xyz.jobs.steps.impl.transport.TaskedSpaceBasedStep;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.util.JobWebClient;
import com.here.xyz.util.ARN;
import com.here.xyz.util.runtime.LambdaFunctionRuntime;
import com.here.xyz.util.service.aws.SimulatedContext;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.ErrorResponseException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.cloudwatchevents.model.ConcurrentModificationException;
import software.amazon.awssdk.services.cloudwatchevents.model.DeleteRuleRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.ListTargetsByRuleRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.PutRuleRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.PutTargetsRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.RemoveTargetsRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.ResourceNotFoundException;
import software.amazon.awssdk.services.cloudwatchevents.model.Target;
import software.amazon.awssdk.services.sfn.model.InvalidTokenException;
import software.amazon.awssdk.services.sfn.model.SendTaskFailureRequest;
import software.amazon.awssdk.services.sfn.model.SendTaskHeartbeatRequest;
import software.amazon.awssdk.services.sfn.model.SendTaskSuccessRequest;
import software.amazon.awssdk.services.sfn.model.TaskTimedOutException;

@JsonSubTypes({
    @JsonSubTypes.Type(value = DatabaseBasedStep.class),
    @JsonSubTypes.Type(value = RunEmrJob.class),
    @JsonSubTypes.Type(value = SyncLambdaStep.class),
})
public abstract class LambdaBasedStep<T extends LambdaBasedStep> extends Step<T> {
  private static final String TASK_TOKEN_TEMPLATE = "$$.Task.Token";
  private static final String RETRY_COUNT_TEMPLATE = "$$.State.RetryCount";
  private static final String PIPELINE_INPUT_TEMPLATE = "$$.Execution.Input";
  public static final String HEART_BEAT_PREFIX = "HeartBeat-";
  //TODO: Check if there are other possibilities
  @JsonView(Internal.class)
  protected boolean isSimulation = false; //TODO: Remove testing code
  private static final Logger logger = LogManager.getLogger();

  @JsonView(Internal.class)
  private String taskToken = TASK_TOKEN_TEMPLATE; //Will be defined by the Step Function

  @JsonView(Internal.class)
  private int retryCount = -1; //Will be defined by the Step Function

  @JsonView(Internal.class)
  private int stepExecutionHeartBeatTimeoutOverride;

  /**
   * Contains the raw (unparsed & unresolved) pipeline input if this LambdaBasedStep belongs to a job that is a pipeline.
   * This value should not be used directly by implementing steps. Implementing steps can gather the input using the well-known
   * method {@link #loadInputs(Class[])}. Using that method will ensure correct deserialization & resolving of the input references.
   */
  @JsonView(Internal.class)
  private Map<String, Object> pipelineInput;

  private ARN ownLambdaArn; //Will be defined from Lambda's execution context

  private static final String INVOKE_SUCCESS = """
      {"status": "OK"}""";

  public void setStepExecutionHeartBeatTimeoutOverride(int timeOutSeconds) {
    this.stepExecutionHeartBeatTimeoutOverride = timeOutSeconds;
  }

  public int getStepExecutionHeartBeatTimeoutOverride() {
    return stepExecutionHeartBeatTimeoutOverride;
  }

  /**
   * This method must be implemented by subclasses.
   * It will be called multiple times by the framework during the different execution phases of the step.
   * In case the state is unknown or cannot be fetched for any reason a {@link UnknownStateException} should be thrown.
   *
   * @return The current state of the step
   * @throws UnknownStateException If the state is unknown or cannot be fetched
   */
  @JsonIgnore
  public abstract AsyncExecutionState getExecutionState() throws UnknownStateException;

  private void startExecution() throws Exception {
    updateState(RUNNING);
    execute(isResume());

    switch (getExecutionMode()) {
      case SYNC -> updateState(SUCCEEDED);
      case ASYNC -> {
        /*
        NOTE: The registration of the state-check trigger MUST happen *after* the call to #execute()
        to ensure the step config contains the changes being added during its execution
         */
        registerStateCheckTrigger();
        synchronizeStep();
      }
    }
  }

  private void updateState(State newState) {
    getStatus().setState(newState);
    synchronizeStep();
  }

  @JsonIgnore
  private String getStateCheckRuleName() {
    return HEART_BEAT_PREFIX + getGlobalStepId();
  }

  private void registerStateCheckTrigger() {
    if (isSimulation)
      return;

    try {
      logger.info("[{}] Registering state-check trigger {} ...", getGlobalStepId(), getStateCheckRuleName());
      cloudwatchEventsClient().putRule(PutRuleRequest.builder()
          .name(getStateCheckRuleName())
          .state(ENABLED)
          .scheduleExpression("rate(1 minute)")
          .description("Heartbeat trigger for Step " + getGlobalStepId())
          .build());

      cloudwatchEventsClient().putTargets(PutTargetsRequest.builder()
          .rule(getStateCheckRuleName())
          .targets(Target.builder()
              .id(getGlobalStepId())
              .arn(ownLambdaArn.toString())
              .input(new LambdaStepRequest().withType(STATE_CHECK).withStep(this).serialize())
              .build())
          .build());
    }
    catch (Exception e) {
      logger.error("[{}] Unexpected error while registering state-check trigger {}", getGlobalStepId(), getStateCheckRuleName(), e);
      throw new StepException("Unexpected error while registering state-check trigger", e);
    }
  }

  private void unregisterStateCheckTrigger() {
    _unregisterStateCheckTriggerDeferred(0, 1);
  }

  private void _unregisterStateCheckTriggerDeferred(long waitMs, int attempt) {
    if (attempt > 5) {
      logger.error("[{}] Could not unregister state-check trigger {} after {} attempts.", getGlobalStepId(),
          getStateCheckRuleName(), attempt - 1);
      return;
    }

    if (isSimulation)
      return;

    try {
      if (waitMs > 0)
        Thread.sleep(waitMs);
    }
    catch (InterruptedException ignore) {}

    try {
      logger.info("[{}] Unregistering state-check trigger {} ...", getGlobalStepId(), getStateCheckRuleName());
      //List all targets
      List<String> targetIds = cloudwatchEventsClient().listTargetsByRule(
              ListTargetsByRuleRequest.builder().rule(getStateCheckRuleName()).build())
          .targets()
          .stream()
          .map(target -> target.id())
          .collect(Collectors.toList());

      if (targetIds.isEmpty())
        _unregisterStateCheckTriggerDeferred(200, ++attempt);

      //Remove all targets from the rule
      cloudwatchEventsClient().removeTargets(RemoveTargetsRequest.builder().rule(getStateCheckRuleName()).ids(targetIds).build());

      //Remove the rule
      cloudwatchEventsClient().deleteRule(DeleteRuleRequest.builder().name(getStateCheckRuleName()).build());
    }
    catch (ConcurrentModificationException e) {
      logger.info("[{}] Concurrent modification of state-check trigger {} Starting next attempt ...", getGlobalStepId(), getStateCheckRuleName());
      _unregisterStateCheckTriggerDeferred(200, ++attempt);
    }
    catch (ResourceNotFoundException e) {
      logger.error("[{}] Unregistering state-check trigger {} failed as it does not exist (yet / anymore).", getGlobalStepId(), getStateCheckRuleName());
      _unregisterStateCheckTriggerDeferred(200, ++attempt);
    }
    catch (Exception e) {
      logger.error("[{}] Unexpected error while unregistering state-check trigger {}", getGlobalStepId(), getStateCheckRuleName(), e);
    }
  }

  private void checkAsyncExecutionState() {
    try {
      onStateCheck();
      switch (getExecutionState()) {
        case RUNNING -> reportAsyncHeartbeat();
        case SUCCEEDED -> reportAsyncSuccess();
        case FAILED -> reportFailure(null, true);
      }
    }
    catch (UnknownStateException e) {
      /*
      The state is not known currently, maybe one of the next STATE_CHECK requests will be able to reveal the state.
      If the issue persists, the step will fail after the heartbeat timeout.
       */
      logger.warn("Unknown execution state for step {}", getGlobalStepId(), e);
      synchronizeStep();
      //NOTE: No heartbeat must be sent to SFN in this case!
    }
    catch (Exception e) {
      //Unexpected exception, there is an issue in the implementation of the step, so cancel & report non-retryable failure
      //TODO: Check if calling cancel makes sense here
      //cancel();
      throw e;
    }
  }

  private void handleAsyncUpdate(ProcessUpdate processUpdate) {
    boolean isCompleted = onAsyncUpdate(processUpdate);
    if (isSimulation)
      //In simulations we are handling success callbacks by our own
      return;
    if (isCompleted)
      reportAsyncSuccess();
    else
      synchronizeStep();
  }

  /**
   * Only for ASYNC step implementations: Will be called for every step requests of type UPDATE_CALLBACK coming from the
   * underlying remote system.
   *
   * @param processUpdate Some information from the remote system to be passed to the step implementation
   * @return Whether the update lead to the successful completion of the step execution in the remote system.
   */
  protected boolean onAsyncUpdate(ProcessUpdate processUpdate) {
    //Nothing to do by default (may be overridden in subclasses)
    return false;
  }

  protected final void reportAsyncSuccess() {
    try {
      onAsyncSuccess();
    }
    catch (Exception e) {
      reportFailure(e, true);
      return;
    }

    try {
      updateState(SUCCEEDED);
      unregisterStateCheckTrigger();
    }
    finally {
      //TODO: Remove testing code
      if (isSimulation)
        System.out.println(getClass().getSimpleName() + " : SUCCESS");
      //Report success to SFN
      try {
        sfnClient().sendTaskSuccess(SendTaskSuccessRequest.builder()
            .taskToken(taskToken)
            .output(INVOKE_SUCCESS)
            .build());
      }
      catch (TaskTimedOutException | InvalidTokenException e) {
        //NOTE: Happens, for example, when the SFN was canceled, but this step still was able to succeed during the time it was in CANCELLING state
        logger.error("[{}] Task in SFN is already stopped. Could not send task success for step.", getGlobalStepId());
      }
    }
  }

  protected void onAsyncSuccess() throws Exception {
    //Nothing to do by default (may be overridden in subclasses)
  }

  private void reportAsyncHeartbeat() {
    if (isSimulation)
      return;
    //Report heartbeat to SFN and check for a potential necessary cancellation
    try {
      sfnClient().sendTaskHeartbeat(SendTaskHeartbeatRequest.builder().taskToken(taskToken).build());
      getStatus().touch();
      synchronizeStep();
    }
    catch (TaskTimedOutException | InvalidTokenException e) {
      try {
        updateState(CANCELLING);
        cancel();
        updateState(CANCELLED);
        unregisterStateCheckTrigger();
      }
      catch (Exception ex) {
        reportFailure(new RuntimeException("Error during cancellation.", ex), false, true);
      }
    }
  }

  @JsonIgnore
  protected boolean isResume() {
    return retryCount > 0;
  }

  /**
   * Will be called for every STATE_CHECK request being performed for the step.
   * Subclasses may override this method to implement tasks which should be performed on a regular basis during that STATE_CHECK.
   * E.g., overriding implementations can update the estimatedProgress at the step's status object.
   */
  protected void onStateCheck() {
    //Nothing to do by default (may be overridden in subclasses)
  }

  /**
   * Will be called for every async failure callback occurring for this step prior to reporting it to SFN or the Job Framework.
   * This method may inspect the causing error description in the step status to decide whether the exception depicts
   * an error that is or is not retryable.
   *
   * If all failed steps of a job are marked being retryable, the user can retry the execution of the job at a later time.
   *
   * @return Whether the specified exception depicts a retryable error or not.
   */
  protected boolean onAsyncFailure() {
    //Nothing to do by default (may be overridden in subclasses)
    return false;
  }

  private void reportFailure(Exception e, boolean retryable, boolean async) {
    if (async)
      retryable = retryable && onAsyncFailure();

    if (e instanceof StepException stepException)
      retryable = stepException.isRetryable();

    logger.error("{}retryable error during execution of step {}:", retryable ? "" : "Non-", getGlobalStepId(), e);
    getStatus().setFailedRetryable(retryable);
    reportFailure(e, async);
  }

  private void reportFailure(Exception e, boolean async) {
    if (isSimulation) //TODO: Remove testing code
      throw new RuntimeException(e);

    if (async)
      unregisterStateCheckTrigger();

    if (e != null) {
      getStatus()
          .withErrorMessage(e.getMessage())
          .withErrorCause(e.getCause() != null ? e.getCause().getClass().getSimpleName() + ": " + e.getCause().getMessage() : null)
          .withErrorCode(e instanceof StepException stepException ? stepException.getCode() : null);

      if (e instanceof ErrorResponseException responseException)
        getStatus().setErrorCode(codeFromErrorErrorResponseException(responseException));
    }

    try {
      //Update state & sync the status
      updateState(FAILED);

      //Log the error also to the lambda log
      logger.error("Error in step {}: Message: {}, Cause: {}, Code: {}", getGlobalStepId(), getStatus().getErrorMessage(),
          getStatus().getErrorCause(), getStatus().getErrorCode());
    }
    finally {
      //Finally, report failure to SFN
      if (async)
        reportFailureToSfn();
    }
  }

  private void reportFailureToSfn() {
    SendTaskFailureRequest.Builder request = SendTaskFailureRequest.builder()
        .taskToken(taskToken);

    if (getStatus().getErrorMessage() != null)
      request.error(truncate(getStatus().getErrorMessage(), 256));

    if (getStatus().getErrorCause() != null || getStatus().getErrorCode() != null) {
      String errCauseForSfn = (getStatus().getErrorCode() != null ? "Error code: " + getStatus().getErrorCode() + ", " : "")
          + (getStatus().getErrorCause() != null ? getStatus().getErrorCause() : "");
      request.cause(truncate(errCauseForSfn, 256));
    }

    try {
      sfnClient().sendTaskFailure(request.build());
    }
    catch (TaskTimedOutException | InvalidTokenException e) {
      logger.error("[{}] Task in SFN is already stopped. Could not send task failure for step.", getGlobalStepId());
    }
    catch (Exception e) {
      logger.error("[{}] Unexpected error while trying to report a failure to SFN.", getGlobalStepId(), e);
    }
  }

  private String truncate(String string, int maxLength) {
    return string.length() < maxLength ? string : string.substring(0, maxLength - 4) + " ...";
  }

  private void synchronizeStep() {
    if (isSimulation) //TODO: Remove testing code
      return;
    //NOTE: For steps that are part of a pipeline job, do not synchronize the state
    if (isPipeline())
      return;
    logger.info("Synchronizing step {} with the job service ...", getGlobalStepId());
    try {
      JobWebClient.getInstance().postStepUpdate(this);
    }
    catch (ErrorResponseException httpError) {
      HttpResponse<byte[]> errorResponse = httpError.getErrorResponse();
      final HttpRequest failedRequest = errorResponse.request();
      logger.error("Error updating the step {} at the job service - Performing {} {}. Upstream-ID: {}, Status-Code: {}, Response:\n{}",
          getGlobalStepId(), failedRequest.method(), failedRequest.uri(), errorResponse.headers().firstValue(STREAM_ID).orElse(null),
          errorResponse.statusCode(), new String(errorResponse.body()));
    }
    catch (WebClientException httpError) {
      logger.error("Error updating the step {} at the job service", getGlobalStepId(), httpError);
    }
  }

  /**
   * Informs the framework in which mode to execute this LambdaBasedStep.
   * This method might be called multiple times during the execution of this step.
   * This method should be implemented in a way to make sure that all calls will always return the same value for the same step
   * configuration.
   *
   * SYNC:
   *  Returning SYNC depicts the intent of the step to start its *whole execution* inside the Lambda Function's runtime environment directly.
   *  The step's execute() method will be called, and it might run as long as defined by the timeout for this step. Once the execution is
   *  completed (which basically means that the execute() method returns), the whole execution is depicted as being complete and
   *  the orchestration of the containing job can continue.
   *
   * ASYNC:
   *  Returning ASYNC depicts the intent of the step to only *start the execution* and *not completing it* in the runtime environment
   *  completely.
   *  The step's execute() method will be called, and it should only start the execution inside some remote system and return very quickly
   *  right after that. From that point on, the step is depicted to be in RUNNING state until a SUCCESS_CALLBACK or a FAILURE_CALLBACK
   *  request is sent back to this Lambda Function. For remote systems that do not support sending back a callback request to this
   *  Lambda Function, the STATE_CHECK request can be used to implement a polling mechanism to check the remote system.
   *  STATE_CHECK requests are sent to this Lambda Function in regular intervals automatically. Subclasses can react on such checks
   *  by implementing the method {@link #getExecutionState()} and act accordingly inside. That is, returning the according state of the
   *  task within the remote system, or throwing an {@link UnknownStateException} in case the state is (temporarily) unknown.
   *
   * @return The execution mode. SYNC vs ASYNC.
   */
  public abstract ExecutionMode getExecutionMode();

  @JsonIgnore
  protected ARN getwOwnLambdaArn() {
    return ownLambdaArn;
  }

  protected void onRuntimeShutdown() {
    //Nothing to do here. Subclasses may override this method to implement some steps to be executed as a "shutdown-hook".
  }

  @JsonProperty("taskToken.$")
  @JsonInclude(Include.NON_NULL)
  private String getTaskTokenTemplate() {
    //NOTE: The task token template may only be used for the ASYNC mode
    return getExecutionMode().equals(SYNC) ? null : TASK_TOKEN_TEMPLATE.equals(taskToken) ? taskToken : null;
  }

  @JsonProperty("retryCount.$")
  @JsonInclude(Include.NON_NULL)
  private String getRetryCountTemplate() {
    return retryCount == -1 ? RETRY_COUNT_TEMPLATE : null;
  }

  private void setRetryCount(int retryCount) {
    this.retryCount = retryCount;
  }

  @JsonProperty("pipelineInput.$")
  @JsonInclude(Include.NON_NULL)
  private String getPipelineInputTemplate() {
    return pipelineInput == null ? PIPELINE_INPUT_TEMPLATE : null;
  }

  private void setPipelineInput(Map<String, Object> pipelineInput) {
    this.pipelineInput = pipelineInput;
  }

  @Override
  protected List<Input> loadInputs(Class<? extends Input>... inputTypes) {
    return isPipeline() ? List.of(Input.resolveRawInput(pipelineInput)) : super.loadInputs(inputTypes);
  }

  public enum ExecutionMode {
    SYNC,
    ASYNC
  }

  protected enum AsyncExecutionState {
    RUNNING,
    SUCCEEDED,
    FAILED
  }

  protected static class UnknownStateException extends RuntimeException {
    public UnknownStateException(String message) {
      super(message);
    }
    public UnknownStateException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  public static class LambdaBasedStepExecutor implements RequestStreamHandler {
    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
      LambdaStepRequest request = null;
      try {
        //Initialize Config from environment variables
        if (Config.instance == null) {
          XyzSerializable.fromMap(Map.copyOf(getEnvironmentVariables()), Config.class);
          loadPlugins();
        }

        //Read the incoming request
        request = XyzSerializable.deserialize(inputStream, LambdaStepRequest.class);

        new LambdaFunctionRuntime(context, request.getStep().getGlobalStepId());

        if (request.getStep() == null)
          throw new NullPointerException("Malformed step request, missing step definition.");

        //Set the userAgent of the web clients correctly
        HubWebClient.userAgent = JobWebClient.userAgent = "XYZ-JobStep-" + request.getStep().getClass().getSimpleName();

        //Set the own lambda ARN accordingly
        if (context instanceof SimulatedContext) {
          request.getStep().ownLambdaArn = new ARN("arn:aws:lambda:" + Config.instance.AWS_REGION + ":000000000000:function:job-step");
          request.getStep().isSimulation = true;
        }
        else
          request.getStep().ownLambdaArn = new ARN(context.getInvokedFunctionArn());
        Config.instance.AWS_REGION = request.getStep().ownLambdaArn.getRegion();
        //Can be set to debug on a later state
        logger.info("[{}] Received Request {}", request.getStep().getGlobalStepId(), XyzSerializable.serialize(request));

        //If this is the actual execution call, call the subclass execution, if not, check the status and just send a heartbeat or success (the appToken must be part of the incoming lambda event)
        //If this is not the actual execution call but only a heartbeat call, then check the execution state and do the proper action, but do **not** call the sub-class execution method

        if (request.getType() == START_EXECUTION) {
          try {
            logger.info("Starting the execution of step {} ...", request.getStep().getGlobalStepId());
            request.getStep().startExecution();
            logger.info("Execution of step {} has been started successfully ...", request.getStep().getGlobalStepId());
          }
          catch (Exception e) {
            //Report error synchronously
            request.getStep().reportFailure(e, false, false);
            throw new RuntimeException("Error executing request of type " + request.getType() + " for step " + request.getStep().getGlobalStepId(), e);
          }
        }

        try {
          switch (request.getType()) {
            case STATE_CHECK -> {
              logger.info("Checking async execution state of step {} ...", request.getStep().getGlobalStepId());
              request.getStep().checkAsyncExecutionState();
              logger.info("Async execution state of step {} has been checked & reported successfully.", request.getStep().getGlobalStepId());
            }
            case UPDATE_CALLBACK -> {
              logger.info("Handling async process update for step {} ...", request.getStep().getGlobalStepId());
              request.getStep().handleAsyncUpdate(request.getProcessUpdate());
              logger.info("Handled async process update for step {} successfully.", request.getStep().getGlobalStepId());
            }
            case SUCCESS_CALLBACK -> {
              logger.info("Reporting async success for step {} ...", request.getStep().getGlobalStepId());
              request.getStep().reportAsyncSuccess();
              logger.info("Reported async success for step {} successfully.", request.getStep().getGlobalStepId());
            }
            case FAILURE_CALLBACK -> {
              logger.info("Cancelling and reporting async failure for step {} ...", request.getStep().getGlobalStepId());
              try {
                request.getStep().cancel();
              }
              catch (Exception e) {
                logger.error("Error during cancellation of step {}.", request.getStep().getGlobalStepId(), e);
              }
              //NOTE: Assume that the error information has been injected into the status object by the callback caller already
              request.getStep().reportFailure(null, false, true);
              logger.info("Reported async failure for step {} failure successfully.", request.getStep().getGlobalStepId());
            }
          }
        }
        catch (Exception e) {
          request.getStep().reportFailure(e, false, true); //TODO: Distinguish between sync / async execution once sync error reporting was implemented
          throw new RuntimeException("Error executing request of type {} for step " + request.getStep().getGlobalStepId(), e);
        }
      }
      finally {
        if (request != null && request.getStep() != null)
          //The lambda call is complete, call the shutdown hook
          request.getStep().onRuntimeShutdown();
      }

      outputStream.write(INVOKE_SUCCESS.getBytes());
    }

    protected Map<String, String> getEnvironmentVariables() {
      return System.getenv();
    }

    protected static void loadPlugins() {
      for (String plugin : Config.instance.stepPlugins())
        try {
          Class.forName(plugin);
        }
        catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
    }
  }

  public static class LambdaStepRequest implements XyzSerializable {
    private RequestType type;
    private LambdaBasedStep step;
    private ProcessUpdate processUpdate;

    public RequestType getType() {
      return type;
    }

    public void setType(RequestType type) {
      this.type = type;
    }

    public LambdaStepRequest withType(RequestType type) {
      setType(type);
      return this;
    }

    public LambdaBasedStep getStep() {
      return step;
    }

    public void setStep(LambdaBasedStep step) {
      this.step = step;
    }

    public LambdaStepRequest withStep(LambdaBasedStep step) {
      setStep(step);
      return this;
    }

    public ProcessUpdate getProcessUpdate() {
      return processUpdate;
    }

    public void setProcessUpdate(ProcessUpdate processUpdate) {
      this.processUpdate = processUpdate;
    }

    public LambdaStepRequest withProcessUpdate(ProcessUpdate processUpdate) {
      setProcessUpdate(processUpdate);
      return this;
    }

    public enum RequestType {
      START_EXECUTION, //Sent by Step Function when the actual execution should be started (ASYNC) / performed (SYNC)
      STATE_CHECK, //For ASYNC mode only: Sent periodically by a CW Events Rule to check the inner step state and report heartbeats to the Step Function
      UPDATE_CALLBACK, //For ASYNC mode only: A request, sent by the underlying foreign system to inform the step about any updates about the process within the foreign system
      SUCCESS_CALLBACK, //For ASYNC mode only: A request, sent by the underlying foreign system to inform the step about its success
      FAILURE_CALLBACK //For ASYNC mode only: A request, sent by the underlying foreign system to inform the step about its failure
    }

    @JsonSubTypes({
            @JsonSubTypes.Type(value = TaskedSpaceBasedStep.SpaceBasedTaskUpdate.class, name = "SpaceBasedTaskUpdate"),
    })
    public static class ProcessUpdate<T extends ProcessUpdate> implements Typed {

    }
  }
}
