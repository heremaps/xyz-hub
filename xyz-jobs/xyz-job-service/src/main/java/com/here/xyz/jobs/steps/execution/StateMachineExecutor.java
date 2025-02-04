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

import static com.here.xyz.jobs.util.AwsClients.sfnClient;
import static software.amazon.awssdk.services.sfn.model.StateMachineType.EXPRESS;
import static software.amazon.awssdk.services.sfn.model.StateMachineType.STANDARD;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.service.Config;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.inputs.ModelBasedInput;
import com.here.xyz.util.ARN;
import io.vertx.core.Future;
import java.util.Arrays;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.sfn.model.CreateStateMachineRequest;
import software.amazon.awssdk.services.sfn.model.CreateStateMachineResponse;
import software.amazon.awssdk.services.sfn.model.DeleteStateMachineRequest;
import software.amazon.awssdk.services.sfn.model.RedriveExecutionRequest;
import software.amazon.awssdk.services.sfn.model.StartExecutionRequest;
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse;
import software.amazon.awssdk.services.sfn.model.StopExecutionRequest;

class StateMachineExecutor extends JobExecutor {
  private static final Logger logger = LogManager.getLogger();
  private static final String STATE_MACHINE_NAME_PREFIX = "job-";

  StateMachineExecutor() {}

  private GraphTransformer transformer(Job job) {
    return new GraphTransformer(Config.instance.STEP_LAMBDA_ARN, job.isPipeline());
  }

  @Override
  public Future<String> execute(Job job) {
    logger.info("[{}] Initiating SFN execution of job ...", job.getId());
    //NOTE: In case of a pipeline job, *only create* the state machine and do not execute it
    return createStateMachine(job.getId(), transformer(job).compileToStateMachine(job.getDescription(), job.getSteps()), job.isPipeline())
        .compose(stateMachineArn -> job.isPipeline() ? Future.succeededFuture(stateMachineArn) : executeStateMachine(job.getId(), stateMachineArn, null));
  }

  /**
   * This method can be used to *partially* re-run a job that has been run before.
   * The former step graph has to partially match the step graph of the specified job.
   * The common steps will not be part of the resulting state machine and their outputs will be re-used for the processing of the
   * following steps.
   *
   * @param formerGraph
   * @param job
   * @return
   */
  @Override
  public Future<String> execute(StepGraph formerGraph, Job job) {
    StepGraph newGraph = job.getSteps();
    //TODO: Implement graph diff logic here for partially re-usable jobs and put it into "resultGraph"
    StepGraph resultGraph = null;
    //TODO: Overwrite the re-used steps in the new graph with the counterpart of the formerGraph
    return createStateMachine(job.getId(), transformer(job).compileToStateMachine(job.getDescription(), resultGraph), job.isPipeline())
        .compose(stateMachineArn -> job.isPipeline() ? Future.succeededFuture(stateMachineArn) : executeStateMachine(job.getId(), stateMachineArn, null));
  }

  @Override
  public Future<Void> sendInput(Job job, ModelBasedInput input) {
    return executeStateMachine(job.getId(), job.getExecutionId(), input.serialize())
        .map(sfnExecutionId -> {
          logger.info("Executing express workflow step function {} with execution ID {} for job {} ...", job.getExecutionId(),
              sfnExecutionId, job.getId());
          return null;
        });
  }

  @Override
  public Future<String> resume(Job job, String executionId) {
    logger.info("[{}] Re-driving SFN execution of job ...", job.getId());
    try {
      //TODO: Asyncify!
      sfnClient().redriveExecution(RedriveExecutionRequest.builder()
          .executionArn(executionId)
          .build());
      return Future.succeededFuture(executionId);
    }
    catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  @Override
  public Future<Void> cancel(String executionId) {
    try {
      //TODO: Asyncify!
      sfnClient().stopExecution(StopExecutionRequest.builder()
          .executionArn(executionId)
          .cause("CANCELLED") //TODO: Infer better cause
          .build());

      /*
      Start checking for cancellations to make sure the job config will be updated properly once all its steps have been properly canceled.
      NOTE: That will also happen once at the startup of the service.
       */
      checkCancellations();

      return Future.succeededFuture();
    }
    catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  @Override
  public Future<Boolean> deleteExecution(String executionId) {
    if (executionId == null)
      return Future.succeededFuture(false);
    try {
      //TODO: Asyncify!
      sfnClient().deleteStateMachine(DeleteStateMachineRequest.builder()
              .stateMachineArn(stateMachineArnFromExecutionId(executionId))
              .build());
      return Future.succeededFuture(true);
    }
    catch (Exception e) {
      return Future.succeededFuture(false);
    }
  }

  private static String stateMachineArnFromExecutionId(String executionId) {
    String[] parts = executionId.split(":");
    if (parts.length != 8)
      throw new IllegalArgumentException("Invalid StateMachine execution ID.");

    String stateMachineName = new ARN(executionId).getResourceWithoutType().split(":")[0];
    return String.join(":", Arrays.asList(executionId.split(":")).subList(0, 5)) + ":stateMachine:" + stateMachineName;
  }

  private Future<String> executeStateMachine(String jobId, String stateMachineArn, String input) {
    logger.info("[{}] Starting SFN state machine execution of job ...", jobId);
    //TODO: Asyncify!

    try {
      StartExecutionResponse startExecutionResponse = sfnClient().startExecution(StartExecutionRequest.builder()
          .stateMachineArn(stateMachineArn)
          .name(jobId)
          .input(input == null ? "{}" : input)
          .build());

      return Future.succeededFuture(startExecutionResponse.executionArn());
    }
    catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  private static Future<String> createStateMachine(String jobId, String stateMachineDefinition, boolean isPipeline) {
    logger.info("[{}] Creating SFN state machine of job ...", jobId);
    //TODO: Asyncify!

    CreateStateMachineResponse creationResponse = sfnClient().createStateMachine(CreateStateMachineRequest.builder()
        .name(STATE_MACHINE_NAME_PREFIX + jobId)
        .definition(stateMachineDefinition)
        .roleArn(Config.instance.STATE_MACHINE_ROLE)
        .type(isPipeline ? EXPRESS : STANDARD)
        .build());
    return Future.succeededFuture(creationResponse.stateMachineArn());
  }
}
