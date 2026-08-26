/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.jobs.config;

import static com.here.xyz.jobs.RuntimeInfo.State.PENDING;
import static com.here.xyz.jobs.RuntimeInfo.State.RUNNING;
import static com.here.xyz.jobs.RuntimeInfo.State.SUCCEEDED;
import static com.here.xyz.util.Random.randomAlpha;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.config.StepConfigClient.StepConfig;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.execution.fusion.SimpleTestStep;
import com.here.xyz.util.service.Core;
import io.vertx.core.Vertx;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StepConfigClientIT {

  private static final String DYNAMO_HOST = System.getProperty("job.host", "localhost");
  private static final String TABLE_ARN = "arn:aws:dynamodb:" + DYNAMO_HOST + ":000000008000:table/xyz-job-step-configs-local";

  private StepConfigClient client;
  private long keepUntil;

  @BeforeAll
  public static void initVertx() {
    if (Core.vertx == null)
      Core.vertx = Vertx.vertx();
  }

  @BeforeEach
  public void setUp() {
    client = new StepConfigClient(TABLE_ARN);
    client.initLocalTable();
    keepUntil = (System.currentTimeMillis() + 3_600_000) / 1000;
  }

  @Test
  public void storeStepsThenLoadReturnsAllStepsOfTheJob() {
    String jobId = randomAlpha(6);
    List<Step<?>> steps = List.of(step(jobId, RUNNING), step(jobId, PENDING), step(jobId, SUCCEEDED));
    client.storeSteps(steps, keepUntil);

    Map<String, StepConfig> stepConfigMap = client.loadStepsForJob(jobId).stream()
        .collect(Collectors.toMap(StepConfig::getId, config -> config));

    assertEquals(steps.size(), stepConfigMap.size(), "every stored step must be loaded back");
    for (Step<?> step : steps) {
      StepConfig config = stepConfigMap.get(step.getId());
      assertNotNull(config, "step " + step.getId() + " must be loaded");
      assertEquals(jobId, config.getJobId(), "each item is partitioned by its jobId");
      assertEquals(step.getStatus().getState().toString(), config.getState(), "the step's state is persisted");
      assertEquals(step.getId(), config.getStep().get("id"), "the full step config is persisted");
      assertNotNull(config.getStep().get("type"), "the persisted step config carries its type");
    }
  }

  @Test
  public void loadIsScopedToTheRequestedJob() {
    String jobA = randomAlpha(6);
    String jobB = randomAlpha(6);
    client.storeSteps(List.of(step(jobA, RUNNING), step(jobA, RUNNING)), keepUntil);
    client.storeSteps(List.of(step(jobB, RUNNING)), keepUntil);

    assertEquals(2, client.loadStepsForJob(jobA).size());
    assertEquals(1, client.loadStepsForJob(jobB).size());
    assertTrue(client.loadStepsForJob(randomAlpha(6)).isEmpty(), "an unknown job has no step configs");
  }

  @Test
  public void storeStepUpsertsUntilFinalThenRejectsFurtherWrites() {
    String jobId = randomAlpha(6);
    Step<?> step = step(jobId, RUNNING);

    client.storeStep(step, keepUntil);
    assertEquals("RUNNING", storedState(jobId, step.getId()));

    //A non-final -> final update is allowed and overwrites.
    step.getStatus().setState(SUCCEEDED);
    client.storeStep(step, keepUntil);
    assertEquals("SUCCEEDED", storedState(jobId, step.getId()));

    //Once the stored step is in a final state, any further write is rejected.
    assertThrows(ConditionalCheckFailedException.class, () -> client.storeStep(step, keepUntil));
    assertEquals("SUCCEEDED", storedState(jobId, step.getId()), "the final state must not be overwritten");
  }

  @Test
  public void deleteStepsByJobIdRemovesOnlyThatJob() {
    String jobA = randomAlpha(6);
    String jobB = randomAlpha(6);
    client.storeSteps(List.of(step(jobA, RUNNING), step(jobA, PENDING)), keepUntil);
    client.storeSteps(List.of(step(jobB, RUNNING)), keepUntil);

    client.deleteSteps(jobA);

    assertTrue(client.loadStepsForJob(jobA).isEmpty(), "all step configs of the deleted job are gone (no orphans)");
    assertEquals(1, client.loadStepsForJob(jobB).size(), "other jobs are untouched");
  }

  @Test
  public void deleteStepsByExplicitIdsRemovesOnlyThose() {
    String jobId = randomAlpha(6);
    Step<?> keep = step(jobId, RUNNING);
    Step<?> remove1 = step(jobId, RUNNING);
    Step<?> remove2 = step(jobId, RUNNING);
    client.storeSteps(List.of(keep, remove1, remove2), keepUntil);

    client.deleteSteps(jobId, List.of(remove1.getId(), remove2.getId()));

    List<StepConfig> remaining = client.loadStepsForJob(jobId);
    assertEquals(1, remaining.size());
    assertEquals(keep.getId(), remaining.get(0).getId());
  }

  @Test
  public void emptyStoreAndDeleteAreNoOps() {
    String jobId = randomAlpha(6);
    client.storeSteps(List.of(), keepUntil);
    client.deleteSteps(jobId, List.of());
    assertTrue(client.loadStepsForJob(jobId).isEmpty());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Step<?> step(String jobId, State state) {
    SimpleTestStep step = new SimpleTestStep(randomAlpha(4));
    step.withJobId(jobId);
    step.getStatus().setState(state);
    return step;
  }

  private String storedState(String jobId, String stepId) {
    return client.loadStepsForJob(jobId).stream()
        .filter(config -> config.getId().equals(stepId))
        .map(StepConfig::getState)
        .findFirst()
        .orElse(null);
  }
}
