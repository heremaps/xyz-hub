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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.config.StepConfigClient.StepConfig;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.execution.fusion.SimpleTestStep;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.aws.dynamo.DynamoClient;
import io.vertx.core.Vertx;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StepConfigClientIT {

  private static final String DYNAMO_HOST = System.getProperty("job.host", "localhost");
  private static final String TABLE_ARN = "arn:aws:dynamodb:" + DYNAMO_HOST + ":000000008000:table/xyz-job-steps-local";

  private StepConfigClient client;
  private Table table;

  @BeforeAll
  public static void initVertx() {
    if (Core.vertx == null)
      Core.vertx = Vertx.vertx();
  }

  @BeforeEach
  public void setUp() {
    client = new StepConfigClient(TABLE_ARN);
    client.initLocalTable();
    table = new DynamoClient(TABLE_ARN, null).getTable();
  }

  @Test
  public void storeStepsThenLoadReturnsAllStepsOfTheJob() {
    String jobId = randomAlpha(6);
    List<Step<?>> steps = List.of(step(jobId, RUNNING), step(jobId, PENDING), step(jobId, SUCCEEDED));
    client.storeSteps(steps);

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
    client.storeSteps(List.of(step(jobA, RUNNING), step(jobA, RUNNING)));
    client.storeSteps(List.of(step(jobB, RUNNING)));

    assertEquals(2, client.loadStepsForJob(jobA).size());
    assertEquals(1, client.loadStepsForJob(jobB).size());
    assertTrue(client.loadStepsForJob(randomAlpha(6)).isEmpty(), "an unknown job has no step configs");
  }

  @Test
  public void storeStepUpsertsUntilFinalThenRejectsFurtherWrites() {
    String jobId = randomAlpha(6);
    Step<?> step = step(jobId, RUNNING);

    client.storeStep(step);
    assertEquals("RUNNING", storedState(jobId, step.getId()));

    //A non-final -> final update is allowed and overwrites.
    step.getStatus().setState(SUCCEEDED);
    client.storeStep(step);
    assertEquals("SUCCEEDED", storedState(jobId, step.getId()));

    //Once the stored step is in a final state, any further write is rejected.
    assertThrows(ConditionalCheckFailedException.class, () -> client.storeStep(step));
    assertEquals("SUCCEEDED", storedState(jobId, step.getId()), "the final state must not be overwritten");
  }

  @Test
  public void storedStepsHaveNoKeepUntil() {
    String jobId = randomAlpha(6);
    Step<?> batchStored = step(jobId, RUNNING);
    Step<?> singleStored = step(jobId, RUNNING);
    client.storeSteps(List.of(batchStored));
    client.storeStep(singleStored);

    //The steps are deleted with their job, they must not expire before it
    for (Step<?> step : List.of(batchStored, singleStored))
      assertFalse(table.getItem("jobId", jobId, "id", step.getId()).hasAttribute("keepUntil"),
          "step " + step.getId() + " must not have a keepUntil");
  }

  @Test
  public void storeStepRemovesTheKeepUntilOfAStepStoredBefore() {
    String jobId = randomAlpha(6);
    Step<?> step = step(jobId, RUNNING);
    //A step item as it was stored before the steps stopped expiring
    table.putItem(Item.fromMap(Map.of(
        "jobId", jobId,
        "id", step.getId(),
        "step", step.toMap(Static.class),
        "state", RUNNING.toString(),
        "keepUntil", (System.currentTimeMillis() + 3_600_000) / 1000)));

    step.getStatus().setState(SUCCEEDED);
    client.storeStep(step);

    Item item = table.getItem("jobId", jobId, "id", step.getId());
    assertEquals("SUCCEEDED", item.getString("state"));
    assertFalse(item.hasAttribute("keepUntil"), "the keepUntil of the step must be removed");
  }

  @Test
  public void deleteStepsByJobIdRemovesOnlyThatJob() {
    String jobA = randomAlpha(6);
    String jobB = randomAlpha(6);
    client.storeSteps(List.of(step(jobA, RUNNING), step(jobA, PENDING)));
    client.storeSteps(List.of(step(jobB, RUNNING)));

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
    client.storeSteps(List.of(keep, remove1, remove2));

    client.deleteSteps(jobId, List.of(remove1.getId(), remove2.getId()));

    List<StepConfig> remaining = client.loadStepsForJob(jobId);
    assertEquals(1, remaining.size());
    assertEquals(keep.getId(), remaining.get(0).getId());
  }

  @Test
  public void emptyStoreAndDeleteAreNoOps() {
    String jobId = randomAlpha(6);
    client.storeSteps(List.of());
    client.deleteSteps(jobId, List.of());
    assertTrue(client.loadStepsForJob(jobId).isEmpty());
  }

  @Test
  public void loadStepReturnsTheSingleStepByJobIdAndStepId() {
    String jobId = randomAlpha(6);
    Step<?> target = step(jobId, RUNNING);
    Step<?> other = step(jobId, PENDING);
    client.storeSteps(List.of(target, other));

    StepConfig loaded = client.loadStep(jobId, target.getId());

    assertNotNull(loaded, "the requested step must be returned via a single getItem");
    assertEquals(jobId, loaded.getJobId(), "the item is identified by its jobId");
    assertEquals(target.getId(), loaded.getId(), "the returned item is the requested step, not the other one");
    assertEquals(target.getStatus().getState().toString(), loaded.getState(), "the step's state is carried");
    assertEquals(target.getId(), loaded.getStep().get("id"), "the full step config is carried");
    assertNotNull(loaded.getStep().get("type"), "the persisted step config carries its type");
  }

  @Test
  public void loadStepReturnsNullForAnUnknownStepOrJob() {
    String jobId = randomAlpha(6);
    Step<?> step = step(jobId, RUNNING);
    client.storeSteps(List.of(step));

    assertNull(client.loadStep(jobId, "s_does_not_exist"), "an unknown step id must yield null");
    assertNull(client.loadStep(randomAlpha(6), step.getId()), "an unknown job must yield null");
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
