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

import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.ExecutionMode.ASYNC;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.START_EXECUTION;
import static com.here.xyz.util.Random.randomAlpha;

import com.amazonaws.arn.Arn;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.StepExecution;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.CreateStateMachineRequest;
import software.amazon.awssdk.services.sfn.model.CreateStateMachineResponse;
import software.amazon.awssdk.services.sfn.model.StartExecutionRequest;
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse;
import software.amazon.awssdk.services.sfn.model.StopExecutionRequest;
import software.amazon.awssdk.services.stepfunctions.builder.StateMachine;
import software.amazon.awssdk.services.stepfunctions.builder.states.Branch;
import software.amazon.awssdk.services.stepfunctions.builder.states.ParallelState;
import software.amazon.awssdk.services.stepfunctions.builder.states.State;
import software.amazon.awssdk.services.stepfunctions.builder.states.TaskState;

public class StateMachineExecutor extends JobExecutor {
  public static final String LAMBDA_INVOKE_RESOURCE = "arn:aws:states:::lambda:invoke";
  private Arn stateMachineArn;
  private SfnClient sfnClient;
  private static final int STATE_MACHINE_EXECUTION_TIMEOUT_SECONDS = 36 * 3600; //36h
  private static final int MIN_STEP_TIMEOUT_SECONDS = 60;
  private static final int STEP_EXECUTION_HEARTBEAT_TIMEOUT_SECONDS = 3 * 60; //3min

  @Override
  public Future<String> execute(Job job) {
    return executeStateMachine(job.getId(), compileToStateMachine(job, job.getSteps()));
  }

  @Override
  public Future<String> execute(StepGraph formerGraph, Job job) {
    StepGraph newGraph = job.getSteps();
    //TODO: Implement graph diff logic here for partially re-usable jobs and put it into "resultGraph"
    StepGraph resultGraph = null;
    //TODO: Overwrite the re-used steps in the new graph with the counterpart of the formerGraph
    return executeStateMachine(job.getId(), compileToStateMachine(job, resultGraph));
  }

  @Override
  public Future<String> resume(Job job, String executionId) {
    //TODO: Research about resuming SFN
    return null;
  }

  @Override
  public Future<Boolean> cancel(String executionId) {
    sfnClient.stopExecution(StopExecutionRequest.builder()
            .executionArn(executionId)
            .cause("CANCELLED") //TODO: Infer better cause
        .build());
    return null;
  }

  //TODO: Care about retention of created State Machines! (e.g., automatically delete State Machines one week after having been completed successfully)

  private Future<String> executeStateMachine(String jobId, StateMachine stateMachineDefinition) {
    //TODO: Asyncify!

    CreateStateMachineResponse creationResponse = sfnClient.createStateMachine(CreateStateMachineRequest.builder()
            .name(jobId)
            .definition(fixStateMachineDefinition(stateMachineDefinition))
            //.roleArn(...) TODO: Get from env vars
        .build());

    StartExecutionResponse startExecutionResponse = sfnClient.startExecution(StartExecutionRequest.builder()
            .stateMachineArn(creationResponse.stateMachineArn())
            .name(jobId)
            //.input(...)
        .build());

    return Future.succeededFuture(startExecutionResponse.executionArn());
  }

  private String fixStateMachineDefinition(StateMachine stateMachine) {
    try {
      Map<String, Object> machineDefinition = XyzSerializable.deserialize(stateMachine.toJson(), Map.class);
      fixLambdaTaskStates(machineDefinition);
      return XyzSerializable.serialize(machineDefinition);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private void fixLambdaTaskStates(Map<String, Object> machineParts) {
    for (Entry<String, Object> e : machineParts.entrySet())
      if (e.getValue() instanceof Map subMap) {
        if (subMap.get("Type") instanceof String type && "Task".equals(type))
          tryFixLambdaTaskState(subMap);
        else
          fixLambdaTaskStates(subMap);
      }
  }

  private void tryFixLambdaTaskState(Map<String, Object> taskState) {
    if (taskState.containsKey("Resource") && taskState.get("Resource") instanceof String resource
        && resource.contains(LAMBDA_INVOKE_RESOURCE)) {
      try {
        LambdaTaskResource lambdaTaskResource = XyzSerializable.deserialize(resource);
        taskState.put("Resource", lambdaTaskResource.resource);
        taskState.put("Parameters", Map.of("FunctionName", lambdaTaskResource.functionName, "Payload", lambdaTaskResource.payload));
      }
      catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private StateMachine compileToStateMachine(Job job, StepGraph stepGraph) {
    StateMachine.Builder machineBuilder = StateMachine.builder()
        .comment(job.getDescription())
        .timeoutSeconds(STATE_MACHINE_EXECUTION_TIMEOUT_SECONDS);

    if (stepGraph.isParallel()) {
      //The top-level graph is parallel, compile it into a single ParallelState and add it
      NamedState parallelState = compileToParallelState(job, stepGraph);
      machineBuilder.state(parallelState.stateName, parallelState.stateBuilder);
    }
    else
      //The top-level graph is sequential, compile all steps into the according states and add them
      compileExecutions(job, stepGraph.getExecutions()).forEach(state -> machineBuilder.state(state.stateName, state.stateBuilder));

    return machineBuilder.build();
  }

  /**
   * Creates a {@link NamedState} which contains a random ID as name and a ParallelState(-Builder) for the specified parallel StepGraph.
   * @param job
   * @param stepGraph
   * @return
   */
  private NamedState<ParallelState.Builder> compileToParallelState(Job job, StepGraph stepGraph) {
    if (!stepGraph.isParallel())
      throw new IllegalArgumentException("Only parallel step graphs can be compiled by this method.");

    ParallelState.Builder parallelState = ParallelState.builder()
        .branches(stepGraph.getExecutions()
            .stream()
            .map(execution -> execution instanceof StepGraph sg && !sg.isParallel()
                ? compileToBranch(job, sg.getExecutions())
                : compileToBranch(job, List.of(execution)))
            .collect(Collectors.toList()).toArray(Branch.Builder[]::new));

    return new NamedState<>("p_" + randomAlpha(6), parallelState);
  }


  /**
   * Creates a Branch(-Builder) for the specified sequential steps.
   * @param job
   * @param executions
   * @return
   */
  private Branch.Builder compileToBranch(Job job, List<StepExecution> executions) {
    Branch.Builder parallelBranch = Branch.builder();
    //Compile all sequential executions into states and add them to the branch
    compileExecutions(job, executions).forEach(state -> parallelBranch.state(state.stateName, state.stateBuilder()));
    return parallelBranch;
  }

  /**
   * Compiles the specified executions into a list of {@link NamedState}s while keeping the order.
   * NOTE: Sequential subgraphs will be unwrapped and their compiled states will be added instead.
   * @param job
   * @param executions
   * @return
   */
  private List<NamedState> compileExecutions(Job job, List<StepExecution> executions) {
    List<NamedState> states = new ArrayList<>();
    executions.stream().forEach(execution -> {
      if (execution instanceof StepGraph sg) {
        if (sg.isParallel())
          //It's a parallel step-graph so compile it into a single (anonymous) parallel state and add it to the map
          states.add(compileToParallelState(job, sg));
        else
          //It's a sequential subgraph so compile all elements to states and add them
          states.addAll(compileExecutions(job, sg.getExecutions()));
      }
      else
        //It's a step, compile it into a state & add it to the map
        states.add(compile(job, (Step<?>) execution));
    });
    return states;
  }

  private NamedState<TaskState.Builder> compile(Job job, Step<?> step) {
    NamedState<TaskState.Builder> state = new NamedState<>(step.getId(), TaskState.builder());
    if (step instanceof LambdaBasedStep lambdaStep)
      compile(job, lambdaStep, state);
    else
      throw new NotImplementedException("The provided step implementation (" + step.getClass().getSimpleName() + ") is not supported.");
    //TODO: Add other implementations here (e.g. EmrStep)

    state.stateBuilder
        .comment(step.getDescription())
        .timeoutSeconds(Math.max(step.getTimeoutSeconds(), MIN_STEP_TIMEOUT_SECONDS))
        .heartbeatSeconds(STEP_EXECUTION_HEARTBEAT_TIMEOUT_SECONDS);
    return state;
  }

  private void compile(Job job, LambdaBasedStep<?> lambdaStep, NamedState<TaskState.Builder> state) {
    String taskResource = LAMBDA_INVOKE_RESOURCE + (lambdaStep.getExecutionMode() == ASYNC ? ".waitForTaskToken" : "");
    String lambdaArn = null; //TODO: Get Lambda ARN from env vars and set into parameters
    LambdaStepRequest payload = new LambdaStepRequest()
        .withType(START_EXECUTION)
        .withStep(lambdaStep);

    state.stateBuilder.resource(new LambdaTaskResource(taskResource, lambdaArn, payload).serialize());
  }

  private record NamedState<SB extends State.Builder>(String stateName, SB stateBuilder) {}

  private record LambdaTaskResource(String resource, String functionName, LambdaStepRequest payload) implements Typed {}
}
