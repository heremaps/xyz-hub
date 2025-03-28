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

import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.ExecutionMode.ASYNC;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.START_EXECUTION;
import static com.here.xyz.util.Random.randomAlpha;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.service.Config;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.StepExecution;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep.ExecutionMode;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest;
import com.here.xyz.util.ARN;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import software.amazon.awssdk.services.stepfunctions.builder.StateMachine;
import software.amazon.awssdk.services.stepfunctions.builder.states.Branch;
import software.amazon.awssdk.services.stepfunctions.builder.states.EndTransition;
import software.amazon.awssdk.services.stepfunctions.builder.states.NextStateTransition;
import software.amazon.awssdk.services.stepfunctions.builder.states.NextStateTransition.Builder;
import software.amazon.awssdk.services.stepfunctions.builder.states.ParallelState;
import software.amazon.awssdk.services.stepfunctions.builder.states.State;
import software.amazon.awssdk.services.stepfunctions.builder.states.TaskState;

public class GraphTransformer {
  private static final String LAMBDA_INVOKE_RESOURCE = "arn:aws:states:::lambda:invoke";
  private static final String EMR_INVOKE_RESOURCE = "arn:aws:states:::emr-serverless:startJobRun.sync";
  private static final int STATE_MACHINE_EXECUTION_TIMEOUT_SECONDS = 36 * 3600; //36h
  private static final int MIN_STEP_TIMEOUT_SECONDS = 5 * 60;
  private static final int STEP_EXECUTION_HEARTBEAT_TIMEOUT_SECONDS = 3 * 60; //3min
  private Map<String, LambdaTaskParameters> lambdaTaskParametersLookup = new HashMap<>(); //TODO: This is a workaround for an open issue with AWS SDK2 for StepFunctions
  private Map<String, Map<String, Object>> taskParametersLookup = new HashMap<>(); //TODO: This is a workaround for an open issue with AWS SDK2 for StepFunctions
  private final ARN stepLambdaArn;
  private boolean isPipeline;

  GraphTransformer(ARN stepLambdaArn, boolean isPipeline) {
    this.stepLambdaArn = stepLambdaArn;
    this.isPipeline = isPipeline;
  }

  //TODO: This is a workaround for an open issue with AWS SDK2 for StepFunctions
  protected String fixStateMachineDefinition(StateMachine stateMachine) {
    try {
      Map<String, Object> machineDefinition = XyzSerializable.deserialize(stateMachine.toJson(), Map.class);
      fixLambdaTaskStates(machineDefinition);
      return XyzSerializable.serialize(machineDefinition); //TODO: Use Internal view for serialization here!
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private void fixLambdaTaskStates(Map<String, Object> machineParts) {
    for (Entry<String, Object> e : machineParts.entrySet())
      if (e.getValue() instanceof Map subMap) {
        if (subMap.get("Type") instanceof String type && "Task".equals(type))
          tryFixLambdaTaskState(e.getKey(), subMap);
        else
          fixLambdaTaskStates(subMap);
      }
      else if (e.getValue() instanceof List subList)
        fixLambdaTaskStates(subList);
  }

  private void fixLambdaTaskStates(List<Object> machineParts) {
    machineParts.forEach(machinePart -> fixLambdaTaskStates(machinePart));
  }

  private void fixLambdaTaskStates(Object machinePart) {
    if (machinePart instanceof List machineParts)
      fixLambdaTaskStates(machineParts);
    else if (machinePart instanceof Map machineParts)
      fixLambdaTaskStates(machineParts);
  }

  //TODO: This is a workaround for an open issue with AWS SDK2 for StepFunctions
  private void tryFixLambdaTaskState(String taskName, Map<String, Object> taskState) {
    if (taskState.containsKey("Resource") && taskState.get("Resource") instanceof String resource) {
      if (resource.contains(LAMBDA_INVOKE_RESOURCE)) {
        LambdaTaskParameters lambdaParameters = lambdaTaskParametersLookup.get(taskName);
        taskState.put("Parameters", Map.of("FunctionName", lambdaParameters.lambdaArn, "Payload", lambdaParameters.payload));
      }
      else if (resource.equals(EMR_INVOKE_RESOURCE))
        taskState.put("Parameters", taskParametersLookup.get(taskName));
    }
  }

  /**
   * Removes all {@link DelegateStep} instances from the given {@code StepGraph},
   * returning a new {@code StepGraph} containing only the relevant steps.
   *
   * <p>This method recursively traverses the given {@code StepGraph}. If a nested {@code StepGraph}
   * contains non-{@link DelegateStep} instances, it is cleaned and added to the new graph.
   * Any {@code StepGraph} or {@link StepExecution} containing only {@link DelegateStep}
   * instances is excluded from the result.</p>
   *
   * <p>The returned graph is a new instance and does not modify the original input {@link StepGraph}.</p>
   *
   * @param stepGraph the input {@link StepGraph} to be cleaned.
   * @return a new {@link StepGraph} with all {@link DelegateStep} instances removed.
   */
  protected StepGraph pruneDelegateSteps(StepGraph stepGraph) {
    //Create a new StepGraph instance for the pruned result
    StepGraph result = new StepGraph().withParallel(stepGraph.isParallel());

    for (StepExecution stepExecution : stepGraph.getExecutions()) {
      if (stepExecution instanceof StepGraph subGraph) {
        //Recursively prune and add the nested StepGraph
        StepGraph prunedSubGraph = pruneDelegateSteps(subGraph);

        //Add the pruned StepGraph only if it is not empty
        if (!prunedSubGraph.isEmpty())
          result.getExecutions().add(prunedSubGraph);
      }
      else if (!(stepExecution instanceof DelegateStep))
        //Add only non-DelegateSteps
        result.getExecutions().add(stepExecution);
    }

    return result;
  }

  protected String compileToStateMachine(String stateMachineDescription, StepGraph stepGraph) {
    stepGraph = pruneDelegateSteps(stepGraph);

    StateMachine.Builder machineBuilder = StateMachine.builder()
        .comment(stateMachineDescription)
        .timeoutSeconds(STATE_MACHINE_EXECUTION_TIMEOUT_SECONDS);

    NamedState firstState;
    NamedState lastState;
    if (stepGraph.isParallel()) {
      //The top-level graph is parallel, compile it into a single ParallelState and add it
      NamedState parallelState = firstState = lastState = compileToParallelState(stepGraph, null);
      machineBuilder.state(parallelState.stateName, parallelState.stateBuilder);
    }
    else {
      //The top-level graph is sequential, compile all steps into the according states and add them
      final List<NamedState> sequentialStates = compileExecutions(stepGraph.getExecutions(), null);
      sequentialStates.forEach(state -> machineBuilder.state(state.stateName, state.stateBuilder));
      firstState = sequentialStates.get(0);
      lastState = sequentialStates.get(sequentialStates.size() - 1);
    }

    machineBuilder.startAt(firstState.stateName);
    setEnd(lastState.stateBuilder);

    return fixStateMachineDefinition(machineBuilder.build());
  }

  /**
   * Creates a {@link NamedState} which contains a random ID as name and a ParallelState(-Builder) for the specified parallel StepGraph.
   * @param stepGraph
   * @return
   */
  private NamedState<ParallelState.Builder> compileToParallelState(StepGraph stepGraph, State.Builder previousState) {
    if (!stepGraph.isParallel())
      throw new IllegalArgumentException("Only parallel step graphs can be compiled by this method.");

    ParallelState.Builder parallelState = ParallelState.builder()
        .branches(stepGraph.getExecutions()
            .stream()
            .map(execution -> execution instanceof StepGraph sg && !sg.isParallel()
                ? compileToBranch(sg.getExecutions())
                : compileToBranch(List.of(execution)))
            .collect(Collectors.toList()).toArray(Branch.Builder[]::new));

    String parallelStateName = "p_" + randomAlpha(6);
    setNext(previousState, parallelStateName);
    return new NamedState<>(parallelStateName, parallelState);
  }

  /**
   * Creates a Branch(-Builder) for the specified sequential steps.
   * @param executions
   * @return
   */
  private Branch.Builder compileToBranch(List<StepExecution> executions) {
    Branch.Builder parallelBranch = Branch.builder();
    //Compile all sequential executions into states and add them to the branch
    final List<NamedState> sequentialStates = compileExecutions(executions, null);
    sequentialStates.forEach(state -> parallelBranch.state(state.stateName, state.stateBuilder()));
    parallelBranch.startAt(sequentialStates.get(0).stateName);
    setEnd(sequentialStates.get(sequentialStates.size() - 1).stateBuilder);
    return parallelBranch;
  }

  /**
   * Compiles the specified executions into a list of {@link NamedState}s while keeping the order.
   * NOTE: Sequential subgraphs will be unwrapped and their compiled states will be added instead.
   * @param executions
   * @return
   */
  private List<NamedState> compileExecutions(List<StepExecution> executions, State.Builder previousState) {
    List<NamedState> states = new ArrayList<>();

    for (StepExecution execution : executions) {
      if (execution instanceof StepGraph sg) {
        if (sg.isParallel())
          //It's a parallel step-graph so compile it into a single (anonymous) parallel state and add it to the map
          states.add(compileToParallelState(sg, previousState));
        else
          //It's a sequential subgraph so compile all elements to states and add them
          states.addAll(compileExecutions(sg.getExecutions(), previousState));
      }
      else
        //It's a step, compile it into a state & add it to the map
        states.add(compile((Step<?>) execution, previousState));

      previousState = states.get(states.size() - 1).stateBuilder;
    }
    return states;
  }

  private NamedState<TaskState.Builder> compile(Step<?> step, State.Builder previousState) {
    NamedState<TaskState.Builder> state = new NamedState<>(step.getClass().getSimpleName() + "." + step.getId(),
        TaskState.builder());
    if (step instanceof RunEmrJob emrStep)
      compile(emrStep, state);
    else if (step instanceof LambdaBasedStep lambdaStep)
      compile(lambdaStep, state);
    else
      throw new NotImplementedException("The provided step implementation (" + step.getClass().getSimpleName() + ") is not supported.");
    //TODO: Add other implementations here (e.g. EcsBasedStep)

    state.stateBuilder
        .comment(step.getDescription())
        .timeoutSeconds(Math.max(step.getTimeoutSeconds(), MIN_STEP_TIMEOUT_SECONDS));

    setNext(previousState, state.stateName);
    return state;
  }

  private void setNext(State.Builder previousState, String nextStateName) {
    if (previousState != null) {
      Builder nextTransition = NextStateTransition.builder().nextStateName(nextStateName);
      if (previousState instanceof TaskState.Builder taskState)
        taskState.transition(nextTransition);
      else if (previousState instanceof ParallelState.Builder parallelState)
        parallelState.transition(nextTransition);
    }
  }

  private void setEnd(State.Builder lastState) {
    EndTransition.Builder endTransition = EndTransition.builder();
    if (lastState instanceof TaskState.Builder taskState)
      taskState.transition(endTransition);
    else if (lastState instanceof ParallelState.Builder parallelState)
      parallelState.transition(endTransition);
  }

  private void compile(LambdaBasedStep<?> lambdaStep, NamedState<TaskState.Builder> state) {
    final ExecutionMode executionMode = lambdaStep.getExecutionMode();
    if (isPipeline && executionMode == ASYNC)
      throw new IllegalStateException("The ASYNC execution mode is not supported for jobs that have a dynamic streaming input");
    String taskResource = LAMBDA_INVOKE_RESOURCE + (executionMode == ASYNC ? ".waitForTaskToken" : "");
    LambdaStepRequest payload = new LambdaStepRequest()
        .withType(START_EXECUTION)
        .withStep(lambdaStep);

    //TODO: This is a workaround for an open issue with AWS SDK2 for StepFunctions
    lambdaTaskParametersLookup.put(state.stateName, new LambdaTaskParameters(stepLambdaArn.toString(), payload.toMap()));

    state.stateBuilder.resource(taskResource);
    if (executionMode == ASYNC)
      state.stateBuilder.heartbeatSeconds(lambdaStep.getStepExecutionHeartBeatTimeoutOverride() == 0 ? STEP_EXECUTION_HEARTBEAT_TIMEOUT_SECONDS :
              lambdaStep.getStepExecutionHeartBeatTimeoutOverride());
  }

  private void compile(RunEmrJob emrStep, NamedState<TaskState.Builder> state) {
    if (Config.instance.LOCALSTACK_ENDPOINT != null) {
      compile((LambdaBasedStep<?>) emrStep, state);
      return;
    }

    taskParametersLookup.put(state.stateName, Map.of(
        "Name", emrStep.getEmrJobName(),
        "ApplicationId", emrStep.getApplicationId(),
        "ExecutionRoleArn", emrStep.getExecutionRoleArn(),
        "JobDriver", Map.of(
            "SparkSubmit", Map.of(
                "EntryPoint", emrStep.getJarUrl(),
                "EntryPointArguments", emrStep.getResolvedScriptParams(),
                "SparkSubmitParameters", emrStep.getSparkParams()
            )
        )
    ));
    state.stateBuilder.resource(EMR_INVOKE_RESOURCE);
  }

  private record NamedState<SB extends State.Builder>(String stateName, SB stateBuilder) {}

  private record LambdaTaskParameters(String lambdaArn, Map<String, Object> payload) {}
}
