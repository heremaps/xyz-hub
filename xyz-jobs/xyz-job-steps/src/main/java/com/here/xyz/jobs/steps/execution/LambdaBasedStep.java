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

import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.STATE_CHECK;
import static software.amazon.awssdk.services.cloudwatchevents.model.RuleState.ENABLED;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.execution.db.DatabaseBasedStep;
import com.here.xyz.util.ARN;
import com.here.xyz.util.service.aws.SimulatedContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.services.cloudwatchevents.CloudWatchEventsClient;
import software.amazon.awssdk.services.cloudwatchevents.model.DeleteRuleRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.ListTargetsByRuleRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.PutRuleRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.PutTargetsRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.RemoveTargetsRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.Target;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.SendTaskFailureRequest;
import software.amazon.awssdk.services.sfn.model.SendTaskHeartbeatRequest;
import software.amazon.awssdk.services.sfn.model.SendTaskSuccessRequest;

@JsonSubTypes({
    @JsonSubTypes.Type(value = DatabaseBasedStep.class)
})
public abstract class LambdaBasedStep<T extends LambdaBasedStep> extends Step<T> {
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

  /*
  TODO: The Lambda execution role (not invokers role) must have the following permissions:

  - Create CW rule
  - Add CW rule targets
  - Remove CW rule targets
  - Delete CW rule

  - Send step success to SFN
  - Send step failure to SFN
  - Send step heartbeat to SFN

  - Assume other roles?
   */
  @JsonProperty("taskToken.$")
  private String taskToken = "$$.Task.Token"; //Will be defined by the Step Function (using the $$.Task.Token placeholder)
  private ARN ownLambdaArn; //Will be defined from Lambda's execution context
  private String invokersRoleArn; //Will be defined by the framework alongside the START_EXECUTION request being relayed by the Step Function
  @JsonView(Internal.class)
  private String stateCheckRuleName; //Will be defined (for ASYNC ExecutionMode) by this step when it created the CW event rule trigger

  private SfnClient sfnClient;
  private CloudWatchEventsClient cwEventsClient;
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
    switch (getExecutionMode()) {
      case SYNC -> execute();
      case ASYNC -> {
        execute(); //TODO: Catch exceptions
        registerStateCheckTrigger();
      }
    }
  }

  private static <T extends AwsClientBuilder> T prepareClientForLocalStack(T builder) {
    if (Config.instance.LOCALSTACK_ENDPOINT != null) {
      builder.endpointOverride(Config.instance.LOCALSTACK_ENDPOINT);
      builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("localstack",
          "localstack")));
    }
    return builder;
  }

  private SfnClient sfnClient() {
    if (sfnClient == null)
      sfnClient = prepareClientForLocalStack(SfnClient.builder()).build();
    return sfnClient;
  }

  private CloudWatchEventsClient cwEventsClient() {
    if (cwEventsClient == null)
      cwEventsClient = prepareClientForLocalStack(CloudWatchEventsClient.builder()).build();
    return cwEventsClient;
  }

  private void registerStateCheckTrigger() {
    String globalStepId = getJobId() + "." + getId();
    stateCheckRuleName = "HeartBeat-" + globalStepId;

    cwEventsClient().putRule(PutRuleRequest.builder()
        .name(stateCheckRuleName)
        .state(ENABLED)
        .scheduleExpression("rate(1 minute)")
        .description("Heartbeat trigger for Step " + globalStepId)
        .build());

    cwEventsClient().putTargets(PutTargetsRequest.builder()
        .rule(stateCheckRuleName)
        .targets(Target.builder()
            .id(globalStepId)
            .arn(ownLambdaArn.toString())
            .roleArn(invokersRoleArn)
            .input(new LambdaStepRequest().withType(STATE_CHECK).withStep(this).serialize())
            .build())
        .build());
  }

  //TODO: Also call this on cancel?
  private void unregisterStateCheckTrigger() {
    //List all targets
    List<String> targetIds = cwEventsClient().listTargetsByRule(ListTargetsByRuleRequest.builder().rule(stateCheckRuleName).build())
        .targets()
        .stream()
        .map(target -> target.id())
        .collect(Collectors.toList());

    //Remove all targets from the rule
    cwEventsClient().removeTargets(RemoveTargetsRequest.builder().rule(stateCheckRuleName).ids(targetIds).build());

    //Remove the rule
    cwEventsClient().deleteRule(DeleteRuleRequest.builder().name(stateCheckRuleName).build());
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
      The state is not known currently, maybe one of the next heartbeat requests will be able to reveal the state.
      If the issue persists, the step will fail after the heartbeat timeout.
       */
      //TODO: Log this occurrence
    }
    catch (Exception e) {
      //TODO: log exception
      //Unexpected exception, there is an issue in the implementation of the step, so cancel & report non-retryable failure
      //TODO: Check if calling cancel makes sense here
      //cancel();
      reportAsyncFailure(e, false);
    }
  }

  protected void onAsyncSuccess() {
    //Nothing to do by default (may be overridden in subclasses)
  }

  protected final void reportAsyncSuccess() {
    //TODO: synchronize the step state before?
    onAsyncSuccess();
    unregisterStateCheckTrigger();
    //Report success to SFN
    if (sfnClient != null)
      sfnClient().sendTaskSuccess(SendTaskSuccessRequest.builder().taskToken(taskToken).build());
    else
      //TODO: Remove testing code
      System.out.println(getClass().getSimpleName() + " : SUCCESS");
  }

  private void reportAsyncHeartbeat() {
    //Report heartbeat to SFN
    sfnClient().sendTaskHeartbeat(SendTaskHeartbeatRequest.builder().taskToken(taskToken).build());
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

  private void reportAsyncFailure(Exception e) {
    if (sfnClient == null)
      throw new RuntimeException(e);

    //TODO: synchronize the step state with the framework before?
    unregisterStateCheckTrigger();
    //Report failure to SFN
    SendTaskFailureRequest.Builder request = SendTaskFailureRequest.builder()
        .taskToken(taskToken);

    if (e!= null)
      request
          .error(e != null ? e.getMessage() : null)
          .cause(e.getCause() != null ? e.getCause().getMessage() : null);

    sfnClient().sendTaskFailure(request.build());
  }

  public abstract ExecutionMode getExecutionMode();

  @JsonIgnore
  protected ARN getwOwnLambdaArn() {
    return ownLambdaArn;
  }

  protected void onRuntimeShutdown() {
    //Nothing to do here. Subclasses may override this method to implement some steps to be executed as a "shutdown-hook".
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
        XyzSerializable.fromMap(Map.copyOf(System.getenv()), Config.class);
      //Read the incoming request
      LambdaStepRequest request = XyzSerializable.deserialize(inputStream, LambdaStepRequest.class);

      //Set the own lambda ARN accordingly
      if (context instanceof SimulatedContext)
        request.getStep().ownLambdaArn = new ARN("arn:aws:lambda:" + Config.instance.JOBS_REGION + ":000000000000:function:job-step");
      else
        request.getStep().ownLambdaArn = new ARN(context.getInvokedFunctionArn());
      Config.instance.JOBS_REGION = request.getStep().ownLambdaArn.getRegion();

      //If this is the actual execution call, call the subclass execution, if not, check the status and just send a heartbeat or success (the appToken must be part of the incoming lambda event)
      //If this is not the actual execution call but only a heartbeat call, then check the execution state and do the proper action, but do **not** call the sub-class execution method
      //IF the incoming event is a cancellation event (check if SF is sending one) call the cancel method!
      switch (request.getType()) {
        case START_EXECUTION -> {
          try {
            request.getStep().startExecution();
          }
          catch (Exception e) {
            request.getStep().reportAsyncFailure(e, false);
          }
        }
        case CANCEL_EXECUTION -> {
          try {
            request.getStep().cancel();
          }
          catch (Exception e) {
            //TODO: log exception
            //TODO: report failure?
            //TODO: is the failure resumable? - most likely not if the cancellation was not properly executed, because the inner state is unknown
            throw new RuntimeException(e);
          }
        }
        case STATE_CHECK -> request.getStep().checkAsyncExecutionState();
        case SUCCESS_CALLBACK -> request.getStep().reportAsyncSuccess();
        //TODO: FAILURE_CALLBACK
      }

      //The lambda call is complete, call the shutdown hook
      request.getStep().onRuntimeShutdown();

      outputStream.write(INVOKE_SUCCESS.getBytes());
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
      CANCEL_EXECUTION, //Sent by Step Function
      STATE_CHECK, //For ASYNC mode only: Sent periodically by a CW Events Rule to check the inner step state and report heartbeats to the Step Function
      SUCCESS_CALLBACK, //For ASYNC mode only: A request, sent by the underlying foreign system to inform the step about its success
      FAILURE_CALLBACK //For ASYNC mode only: A request, sent by the underlying foreign system to inform the step about its failure
    }
  }
}
