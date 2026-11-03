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

package com.here.xyz.jobs.service;

import static com.here.xyz.jobs.RuntimeInfo.State.NOT_READY;
import static com.here.xyz.jobs.RuntimeInfo.State.PENDING;
import static com.here.xyz.jobs.steps.execution.JobExecutor.SchedulerState.SchedulerRuntimeState.PAUSED;
import static com.here.xyz.util.Random.randomAlpha;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.config.InMemJobConfigClient;
import com.here.xyz.jobs.config.JobConfigClient;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.service.JobStatusWriteConflictsTest.ReadyTestStep;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.JobCompiler;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.compiler.JobCompilationInterceptor;
import com.here.xyz.jobs.steps.execution.JobExecutor;
import com.here.xyz.jobs.steps.execution.JobExecutor.SchedulerState;
import com.here.xyz.jobs.steps.execution.JobExecutor.SchedulerStatePatch;
import com.here.xyz.jobs.steps.execution.fusion.SimpleTestStep;
import com.here.xyz.util.service.Core;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Submitting a job again, e.g., once its inputs are available, compiles it again. The steps of the previous compilation must be
 * deleted then, because the stored job does not reference them anymore.
 */
public class JobResubmissionTest {
  private static final String PREFIX = JobResubmissionTest.class.getSimpleName() + "_" + randomAlpha(5);
  private static final int STEP_COUNT = 5;
  //Whether the steps of the compiled jobs are ready for the execution
  private static volatile boolean stepsReady;
  private static DeletionRecordingConfigClient configClient;
  private static Object previousConfigClient;
  private static SchedulerState previousSchedulerState;

  @BeforeAll
  public static void setUp() throws Exception {
    if (Core.vertx == null)
      Core.vertx = Vertx.vertx();
    if (Config.instance == null)
      new Config();
    XyzSerializable.registerSubtypes(StepGraph.class, SimpleTestStep.class);
    previousConfigClient = configClientField().get(null);
    JobCompiler.registerCompilationInterceptor(TestCompiler.class);
    previousSchedulerState = JobExecutor.loadSchedulerState().result();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    JobCompiler.deregisterCompilationInterceptor(TestCompiler.class);
    configClientField().set(null, previousConfigClient);
  }

  @BeforeEach
  public void init() throws Exception {
    configClient = new DeletionRecordingConfigClient();
    configClientField().set(null, configClient);
    //The scheduler is paused, so that submitted jobs stay PENDING
    JobExecutor.applySchedulerState(new SchedulerStatePatch(PAUSED, null, null));
  }

  @AfterEach
  public void resetScheduler() {
    JobExecutor.applySchedulerState(new SchedulerStatePatch(previousSchedulerState.state(),
        previousSchedulerState.singleJobAllowedPoliciesEnabled(), previousSchedulerState.singleJobPerResourceEnabled()));
  }

  @Test
  public void startOfANotReadyJobDeletesTheStepsOfThePreviousCompilation() throws Exception {
    Job job = newJob("_start");
    //The job waits for its inputs
    stepsReady = false;
    assertFalse(await(job.submit()));
    List<String> firstStepIds = stepIds(await(Job.load(job.getId())));
    assertEquals(STEP_COUNT, firstStepIds.size());
    assertTrue(configClient.deletedStepIds(job.getId()).isEmpty(), "The first submission has no steps to replace");

    stepsReady = true;
    assertTrue(await(await(Job.load(job.getId())).submit()));

    Job storedJob = await(Job.load(job.getId()));
    assertEquals(PENDING, storedJob.getStatus().getState());
    assertEquals(firstStepIds, configClient.deletedStepIds(job.getId()));
    assertTrue(Collections.disjoint(firstStepIds, stepIds(storedJob)), "The stored job must reference the new steps only");
  }

  @Test
  public void submissionOfAStillNotReadyJobDeletesTheStepsOfThePreviousCompilation() throws Exception {
    Job job = newJob("_notReady");
    stepsReady = false;
    assertFalse(await(job.submit()));
    List<String> firstStepIds = stepIds(await(Job.load(job.getId())));

    assertFalse(await(await(Job.load(job.getId())).submit()));

    Job storedJob = await(Job.load(job.getId()));
    assertEquals(NOT_READY, storedJob.getStatus().getState());
    assertEquals(firstStepIds, configClient.deletedStepIds(job.getId()));
    assertTrue(Collections.disjoint(firstStepIds, stepIds(storedJob)));
  }

  @Test
  public void failedDeletionOfThePreviousStepsDoesNotFailTheSubmission() throws Exception {
    Job job = newJob("_failingDeletion");
    stepsReady = false;
    assertFalse(await(job.submit()));
    configClient.failDeletions = true;

    stepsReady = true;
    assertTrue(await(await(Job.load(job.getId())).submit()));

    assertEquals(PENDING, await(Job.load(job.getId())).getStatus().getState());
  }

  private static Job newJob(String idSuffix) {
    return new Job().withId(PREFIX + idSuffix).withDescription(TestCompiler.DESCRIPTION).withSource(new Files<>())
        .withTarget(new Files<>()).create();
  }

  private static List<String> stepIds(Job job) {
    return job.getSteps().stepStream().map(Step::getId).sorted().collect(Collectors.toList());
  }

  private static Field configClientField() throws Exception {
    Field field = JobConfigClient.Provider.class.getDeclaredField("client");
    field.setAccessible(true);
    return field;
  }

  private static <T> T await(Future<T> future) throws Exception {
    return future.toCompletionStage().toCompletableFuture().get(30, SECONDS);
  }

  /**
   * Records which steps were deleted. The {@link InMemJobConfigClient} stores the steps as part of the job, so that deletions
   * would not be visible otherwise.
   */
  private static class DeletionRecordingConfigClient extends InMemJobConfigClient {
    private final Map<String, List<String>> deletedStepIds = new ConcurrentHashMap<>();
    volatile boolean failDeletions;

    @Override
    public Future<Void> deleteStepConfigs(String jobId, List<String> stepIds) {
      if (failDeletions)
        return Future.failedFuture(new RuntimeException("Simulated deletion failure"));
      deletedStepIds.computeIfAbsent(jobId, id -> Collections.synchronizedList(new ArrayList<>())).addAll(stepIds);
      return super.deleteStepConfigs(jobId, stepIds);
    }

    List<String> deletedStepIds(String jobId) {
      return deletedStepIds.getOrDefault(jobId, List.of()).stream().sorted().collect(Collectors.toList());
    }
  }

  /**
   * Compiles the test jobs of this class into {@link #STEP_COUNT} parallel steps, which are ready depending on {@link #stepsReady}.
   */
  public static class TestCompiler implements JobCompilationInterceptor {
    static final String DESCRIPTION = PREFIX + " test job";

    @Override
    public boolean chooseMe(Job job) {
      return DESCRIPTION.equals(job.getDescription());
    }

    @Override
    public CompilationStepGraph compile(Job job) {
      CompilationStepGraph graph = (CompilationStepGraph) new CompilationStepGraph().withParallel(true);
      for (int i = 0; i < STEP_COUNT; i++)
        graph.addExecution(stepsReady ? new ReadyTestStep() : new SimpleTestStep<>("param"));
      return graph;
    }
  }
}
