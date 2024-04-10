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

package com.here.xyz.jobs.steps.execution;

import static com.here.xyz.jobs.RuntimeInfo.State.CANCELLED;
import static com.here.xyz.jobs.RuntimeInfo.State.FAILED;
import static com.here.xyz.jobs.RuntimeInfo.State.RUNNING;
import static com.here.xyz.jobs.RuntimeInfo.State.SUCCEEDED;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.STATE_CHECK;
import static com.here.xyz.jobs.util.AwsClients.cloudwatchEventsClient;
import static com.here.xyz.jobs.util.AwsClients.sfnClient;
import static software.amazon.awssdk.services.cloudwatchevents.model.RuleState.ENABLED;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.execution.db.DatabaseBasedStep;
import com.here.xyz.jobs.util.JobWebClient;
import com.here.xyz.util.ARN;
import com.here.xyz.util.service.aws.SimulatedContext;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.cloudwatchevents.model.DeleteRuleRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.ListTargetsByRuleRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.PutRuleRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.PutTargetsRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.RemoveTargetsRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.ResourceNotFoundException;
import software.amazon.awssdk.services.cloudwatchevents.model.Target;
import software.amazon.awssdk.services.sfn.model.SendTaskFailureRequest;
import software.amazon.awssdk.services.sfn.model.SendTaskHeartbeatRequest;
import software.amazon.awssdk.services.sfn.model.SendTaskSuccessRequest;
import software.amazon.awssdk.services.sfn.model.TaskTimedOutException;

@JsonSubTypes({
    @JsonSubTypes.Type(value = DatabaseBasedStep.class)
})
public abstract class LambdaBasedStep<T extends LambdaBasedStep> extends Step<T> {
  public static final String TASK_TOKEN_TEMPLATE = "$$.Task.Token";
  protected boolean isSimulation = false; //TODO: Remove testing code
  private static final Logger logger = LogManager.getLogger();

  //TODO: Allow the implementations to define their heartbeat interval & timeout?

  /*
  TODO: The Lambda invokers role (not execution role) must have the following permissions:

   - Invoke the Lambda function

   It must be assumable (trust-policy) by:

   - The Step Function which should call the step
   - The CW Event Rule which triggers the state checks / heartbeats
   - Any foreign system's role that wants to invoke the lambda for callbacks (e.g. RDS instances, also add it to the instance accordingly!)
     see: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL-Lambda.html
   */

  @JsonView(Internal.class)
  private String taskToken = TASK_TOKEN_TEMPLATE; //Will be defined by the Step Function (using the $$.Task.Token placeholder)
  private ARN ownLambdaArn; //Will be defined from Lambda's execution context
  String invokersRoleArn; //Will be defined by the framework alongside the START_EXECUTION request being relayed by the Step Function

  private static final String INVOKE_SUCCESS = """
      {"status": "OK"}""";

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
    switch (getExecutionMode()) {
      case SYNC -> {
        execute();
        updateState(SUCCEEDED);
      }
      case ASYNC -> {
        execute();
        registerStateCheckTrigger();
      }
    }
  }

  private void updateState(State newState) {
    getStatus().setState(newState);
    synchronizeStepState();
  }

  private void registerStateCheckTrigger() {
    if (isSimulation)
      return;

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
            .roleArn(invokersRoleArn)
            .input(new LambdaStepRequest().withType(STATE_CHECK).withStep(this).serialize())
            .build())
        .build());
  }

  @JsonIgnore
  private String getStateCheckRuleName() {
    return "HeartBeat-" + getGlobalStepId();
  }

  //TODO: Also call this on cancel?
  private void unregisterStateCheckTrigger() {
    if (isSimulation)
      return;

    try {
      //List all targets
      List<String> targetIds = cloudwatchEventsClient().listTargetsByRule(
              ListTargetsByRuleRequest.builder().rule(getStateCheckRuleName()).build())
          .targets()
          .stream()
          .map(target -> target.id())
          .collect(Collectors.toList());

      //Remove all targets from the rule
      cloudwatchEventsClient().removeTargets(RemoveTargetsRequest.builder().rule(getStateCheckRuleName()).ids(targetIds).build());

      //Remove the rule
      cloudwatchEventsClient().deleteRule(DeleteRuleRequest.builder().name(getStateCheckRuleName()).build());
    }
    catch (ResourceNotFoundException e) {
      //Ignore the exception, as the rule is not existing (yet)
    }
  }

  private void checkAsyncExecutionState() {
    try {
      switch (getExecutionState()) {
        case RUNNING -> reportAsyncHeartbeat();
        case SUCCEEDED -> reportAsyncSuccess();
        case FAILED -> reportAsyncFailure(null);
      }
    }
    catch (UnknownStateException e) {
      /*
      The state is not known currently, maybe one of the next STATE_CHECK requests will be able to reveal the state.
      If the issue persists, the step will fail after the heartbeat timeout.
       */
      logger.warn("Unknown execution state for step {}.{}", getJobId(), getId(), e);
      //NOTE: No heartbeat must be sent to SFN in this case!
    }
    catch (Exception e) {
      //TODO: log exception
      //Unexpected exception, there is an issue in the implementation of the step, so cancel & report non-retryable failure
      //TODO: Check if calling cancel makes sense here
      //cancel();
      reportAsyncFailure(e, false);
    }
  }

  protected void onAsyncSuccess() throws Exception {
    //Nothing to do by default (may be overridden in subclasses)
  }

  protected final void reportAsyncSuccess() {
    try {
      onAsyncSuccess();
    }
    catch (Exception e) {
      reportAsyncFailure(e);
      return;
    }

    updateState(SUCCEEDED);
    unregisterStateCheckTrigger();

    //Report success to SFN
    if (!isSimulation) { //TODO: Remove testing code
      sfnClient().sendTaskSuccess(SendTaskSuccessRequest.builder()
          .taskToken(taskToken)
          .output(INVOKE_SUCCESS)
          .build());
    }
    else
      //TODO: Remove testing code
      System.out.println(getClass().getSimpleName() + " : SUCCESS");
  }

  private void reportAsyncHeartbeat() {
    if (isSimulation)
      return;
    //Report heartbeat to SFN and check for a potential necessary cancellation
    try {
      sfnClient().sendTaskHeartbeat(SendTaskHeartbeatRequest.builder().taskToken(taskToken).build());
      getStatus().touch();
      synchronizeStepState();
    }
    catch (TaskTimedOutException e) {
      try {
        cancel();
        updateState(CANCELLED);
      }
      catch (Exception ex) {
        logger.error("Error during cancellation of step {}.{}", getJobId(), getId(), ex);
        reportAsyncFailure(ex, false);
      }
    }
  }

  protected boolean onAsyncFailure(Exception e) {
    //Nothing to do by default (may be overridden in subclasses)
    return false;
  }

  private void reportAsyncFailure(Exception e, boolean retryable) {
    retryable = retryable && onAsyncFailure(e);
    logger.error((retryable ? "" : "Non-") + "retryable error during execution of step {}.{}:", getJobId(), getId(), e);
    setFailedRetryable(retryable);
    reportAsyncFailure(e);
  }

  //TODO: Implement also sync error reporting
  private void reportAsyncFailure(Exception e) {
    if (isSimulation) //TODO: Remove testing code
      throw new RuntimeException(e);

    unregisterStateCheckTrigger();
    updateState(FAILED);

    //Report failure to SFN
    SendTaskFailureRequest.Builder request = SendTaskFailureRequest.builder()
        .taskToken(taskToken);

    if (e!= null)
      request
          .error(e != null ? e.getMessage() : null)
          .cause(e.getCause() != null ? e.getCause().getMessage() : null);

    try {
      sfnClient().sendTaskFailure(request.build());
    }
    catch (TaskTimedOutException ex) {
      logger.error("Task in SFN is already stopped. Could not send task failure for step {}.{}. Original exception was:",
          getJobId(), getId(), ex);
    }
  }

  private void synchronizeStepState() {
    try {
      //TODO: Add error & cause to this step instance, so it gets serialized into the step JSON being sent to the service?
      JobWebClient.getInstance().postStepUpdate(this);
    }
    catch (WebClientException httpError) {
      logger.error("Error updating the step state of step {}.{} at the job service", getJobId(), getId(), httpError);
    }
  }

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
    return TASK_TOKEN_TEMPLATE.equals(taskToken) ? taskToken : null;
  }

  protected enum ExecutionMode {
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
      //Initialize Config from environment variables
      if (Config.instance == null)
        XyzSerializable.fromMap(Map.copyOf(getEnvironmentVariables()), Config.class);
      //Read the incoming request
      LambdaStepRequest request = XyzSerializable.deserialize(inputStream, LambdaStepRequest.class);

      if (request.getStep() == null)
        throw new NullPointerException("Malformed step request, missing step definition.");

      //Set the own lambda ARN accordingly
      if (context instanceof SimulatedContext) {
        request.getStep().ownLambdaArn = new ARN("arn:aws:lambda:" + Config.instance.AWS_REGION + ":000000000000:function:job-step");
        request.getStep().isSimulation = true;
      }
      else
        request.getStep().ownLambdaArn = new ARN(context.getInvokedFunctionArn());
      Config.instance.AWS_REGION = request.getStep().ownLambdaArn.getRegion();

      //If this is the actual execution call, call the subclass execution, if not, check the status and just send a heartbeat or success (the appToken must be part of the incoming lambda event)
      //If this is not the actual execution call but only a heartbeat call, then check the execution state and do the proper action, but do **not** call the sub-class execution method
      //IF the incoming event is a cancellation event (check if SF is sending one) call the cancel method!
      try {
        switch (request.getType()) {
          case START_EXECUTION -> {
            logger.info("Starting the execution of step {} ...", request.getStep().getGlobalStepId());
            request.getStep().startExecution();
            logger.info("Execution of step {} has been started successfully ...", request.getStep().getGlobalStepId());
          }
          case STATE_CHECK -> {
            logger.info("Checking async execution state of step {} ...", request.getStep().getGlobalStepId());
            request.getStep().checkAsyncExecutionState();
            logger.info("Async execution state of step {} has been checked & reported successfully.", request.getStep().getGlobalStepId());
          }
          case SUCCESS_CALLBACK -> {
            logger.info("Reporting async success for step {} ...", request.getStep().getGlobalStepId());
            request.getStep().reportAsyncSuccess();
            logger.info("Reported async success for step {} successfully.", request.getStep().getGlobalStepId());
          }
          case FAILURE_CALLBACK -> {
            logger.info("Reporting async failure for step {} ...", request.getStep().getGlobalStepId());
            request.getStep().reportAsyncFailure(null, false); //TODO: Read error information from request payload (once specified)
            logger.info("Reported async failure for step {} failure successfully.", request.getStep().getGlobalStepId());
          }
        }
      }
      catch (Exception e) {
        request.getStep().reportAsyncFailure(e, false); //TODO: Distinguish between sync / async execution once sync error reporting was implemented
        throw new RuntimeException("Error executing request of type {} for step " + request.getStep().getGlobalStepId(), e);
      }

      //The lambda call is complete, call the shutdown hook
      request.getStep().onRuntimeShutdown();

      outputStream.write(INVOKE_SUCCESS.getBytes());
    }

    protected Map<String, String> getEnvironmentVariables() {
      return System.getenv();
    }
  }

  public static class LambdaStepRequest implements XyzSerializable {
    private RequestType type;
    private LambdaBasedStep step;

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

    public enum RequestType {
      START_EXECUTION, //Sent by Step Function when the actual execution should be started (ASYNC) / performed (SYNC)
      STATE_CHECK, //For ASYNC mode only: Sent periodically by a CW Events Rule to check the inner step state and report heartbeats to the Step Function
      SUCCESS_CALLBACK, //For ASYNC mode only: A request, sent by the underlying foreign system to inform the step about its success
      FAILURE_CALLBACK //For ASYNC mode only: A request, sent by the underlying foreign system to inform the step about its failure
    }
  }
}
