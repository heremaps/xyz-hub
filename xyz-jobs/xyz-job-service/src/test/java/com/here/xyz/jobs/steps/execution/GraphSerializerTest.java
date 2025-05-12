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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.service.Config;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.execution.fusion.SimpleTestStepWithLoad;
import com.here.xyz.jobs.steps.execution.fusion.SimpleTestStepWithLoad.TestResource;
import com.here.xyz.jobs.steps.resources.ExecutionResource;
import com.here.xyz.util.service.Core;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

public class GraphSerializerTest {

  static {
    Core.vertx = Vertx.vertx();
    new Config();
    Config.instance.PARALLEL_STEPS_SUPPORTED = true;
  }

  @Test
  public void optimizeNotExecutableGraph() {
    StepGraph stepGraph = new StepGraph()
            .addExecution(new SimpleTestStepWithLoad<>("Step1"))
            .addExecution(new SimpleTestStepWithLoad<>("Step2"))
            .addExecution(new SimpleTestStepWithLoad<>("Step3"))
            .addExecution(new SimpleTestStepWithLoad<>("Step4"))
            .withParallel(true);

    Job job = new Job().withExecutionSteps(stepGraph);

    Future<Map<ExecutionResource, Double>> freeVirtualUnits = getDummyFreeUnits(10.0);
    boolean mayExecuteBefore = await(JobExecutor.getInstance().mayExecute(freeVirtualUnits, job), 2);
    assertEquals(false, mayExecuteBefore);

    await(GraphSerializerTool.optimize(freeVirtualUnits, job), 2);
    assertTrue(!job.getExecutionSteps().isParallel());
    assertEquals(2, ((StepGraph) job.getExecutionSteps().getExecutions().get(0)).getExecutions().size());
    assertEquals(2, ((StepGraph) job.getExecutionSteps().getExecutions().get(1)).getExecutions().size());

    boolean mayExecuteAfter = await(JobExecutor.getInstance().mayExecute(freeVirtualUnits, job), 2);
    assertEquals(true, mayExecuteAfter);
  }

  private Future<Map<ExecutionResource, Double>> getDummyFreeUnits(Double value) {
    return Future.succeededFuture(Map.of(TestResource.getInstance(), value));
  }

  private static <T> T await(Future<T> future, long sec) {
    Awaitility.await()
            .atMost(sec, TimeUnit.SECONDS)
            .until(future::isComplete);

    if (future.succeeded()) {
      return future.result();
    } else {
      throw new RuntimeException("Future failed", future.cause());
    }
  }

}
