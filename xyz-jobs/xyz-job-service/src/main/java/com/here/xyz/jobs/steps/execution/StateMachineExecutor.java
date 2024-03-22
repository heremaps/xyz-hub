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

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.service.Config;
import com.here.xyz.jobs.steps.StepGraph;
import io.vertx.core.Future;
import software.amazon.awssdk.services.sfn.model.CreateStateMachineRequest;
import software.amazon.awssdk.services.sfn.model.CreateStateMachineResponse;
import software.amazon.awssdk.services.sfn.model.RedriveExecutionRequest;
import software.amazon.awssdk.services.sfn.model.StartExecutionRequest;
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse;
import software.amazon.awssdk.services.sfn.model.StopExecutionRequest;

class StateMachineExecutor extends JobExecutor {
  private GraphTransformer graphTransformer = new GraphTransformer(Config.instance.STEP_LAMBDA_ARN);

  StateMachineExecutor() {}

  @Override
  public Future<String> execute(Job job) {
    return executeStateMachine(job.getId(), graphTransformer.compileToStateMachine(job.getDescription(), job.getSteps()));
  }

  @Override
  public Future<String> execute(StepGraph formerGraph, Job job) {
    StepGraph newGraph = job.getSteps();
    //TODO: Implement graph diff logic here for partially re-usable jobs and put it into "resultGraph"
    StepGraph resultGraph = null;
    //TODO: Overwrite the re-used steps in the new graph with the counterpart of the formerGraph
    return executeStateMachine(job.getId(), graphTransformer.compileToStateMachine(job.getDescription(), resultGraph));
  }

  @Override
  public Future<String> resume(Job job, String executionId) {
    try {
      //TODO: Asyncify!
      sfnClient().redriveExecution(RedriveExecutionRequest.builder()
          .executionArn(executionId)
          .build());
      return Future.succeededFuture();
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

  //TODO: Care about retention of created State Machines! (e.g., automatically delete State Machines one week after having been completed successfully)

  private Future<String> executeStateMachine(String jobId, String stateMachineDefinition) {
    //TODO: Asyncify!

    CreateStateMachineResponse creationResponse = sfnClient().createStateMachine(CreateStateMachineRequest.builder()
            .name(Config.instance.STATE_MACHINE_PREFIX + "-" + jobId)
            .definition(stateMachineDefinition)
            //.roleArn(...) TODO: Get from env vars
        .build());

    StartExecutionResponse startExecutionResponse = sfnClient().startExecution(StartExecutionRequest.builder()
            .stateMachineArn(creationResponse.stateMachineArn())
            .name(jobId)
        .build());

    return Future.succeededFuture(startExecutionResponse.executionArn());
  }
}
