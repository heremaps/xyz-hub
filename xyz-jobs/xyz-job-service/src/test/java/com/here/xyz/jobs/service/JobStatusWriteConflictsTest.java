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

import static com.here.xyz.jobs.RuntimeInfo.State.PENDING;
import static com.here.xyz.jobs.RuntimeInfo.State.RUNNING;
import static com.here.xyz.jobs.RuntimeInfo.State.SUBMITTED;
import static com.here.xyz.jobs.RuntimeInfo.State.SUCCEEDED;
import static com.here.xyz.jobs.RuntimeStatus.Action.START;
import static com.here.xyz.jobs.steps.execution.JobExecutor.SchedulerState.SchedulerRuntimeState.PAUSED;
import static com.here.xyz.util.Random.randomAlpha;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.cache.Cache;
import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.RuntimeStatus;
import com.here.xyz.jobs.config.InMemJobConfigClient;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.config.JobConfigClient;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.JobCompiler;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.compiler.JobCompilationInterceptor;
import com.here.xyz.jobs.steps.execution.JobExecutor;
import com.here.xyz.jobs.steps.execution.JobExecutor.SchedulerState;
import com.here.xyz.jobs.steps.execution.JobExecutor.SchedulerStatePatch;
import com.here.xyz.jobs.steps.execution.db.Database;
import com.here.xyz.jobs.steps.execution.fusion.SimpleTestStep;
import com.here.xyz.util.service.Core;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Concurrent writes of the job status (DS-1188, DS-2201), checked against the {@link InMemJobConfigClient}, which checks the
 * same write conditions as the DynamoJobConfigClient.
 */
public class JobStatusWriteConflictsTest {
  private static final String PREFIX = JobStatusWriteConflictsTest.class.getSimpleName() + "_" + randomAlpha(5);
  private static final int STEP_COUNT = 8;
  private static final AtomicInteger compilations = new AtomicInteger();
  //If set, the compilations wait for each other, so that concurrent submissions compile at the same time
  private static volatile CyclicBarrier compilationBarrier;
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
    configClientField().set(null, new InMemJobConfigClient());
    JobCompiler.registerCompilationInterceptor(TestCompiler.class);
    previousSchedulerState = JobExecutor.loadSchedulerState().result();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    JobCompiler.deregisterCompilationInterceptor(TestCompiler.class);
    configClientField().set(null, previousConfigClient);
    databaseCache().invalidateAll();
  }

  @AfterEach
  public void resetScheduler() {
    compilationBarrier = null;
    JobExecutor.applySchedulerState(new SchedulerStatePatch(previousSchedulerState.state(),
        previousSchedulerState.singleJobAllowedPoliciesEnabled(), previousSchedulerState.singleJobPerResourceEnabled()));
  }

  @Test
  public void concurrentSubmissionsStartTheJobOnce() throws Exception {
    pauseScheduler();
    Job job = new Job().withId(PREFIX + "_submit").withDescription(TestCompiler.DESCRIPTION).withSource(new Files<>())
        .withTarget(new Files<>()).create();
    //The job is stored as NOT_READY first, e.g., while its inputs are scanned
    await(job.store());
    Job jobOfCreation = await(Job.load(job.getId()));
    Job jobOfStartRequest = await(Job.load(job.getId()));
    int compilationsBefore = compilations.get();
    compilationBarrier = new CyclicBarrier(2);

    Future<Boolean> submission1 = jobOfCreation.submit();
    Future<Boolean> submission2 = jobOfStartRequest.submit();
    boolean submitted1 = await(submission1);
    boolean submitted2 = await(submission2);

    assertEquals(2, compilations.get() - compilationsBefore, "Both submissions must have compiled concurrently");
    assertNotEquals(submitted1, submitted2, "Exactly one submission must have been done");
    Job winner = submitted1 ? jobOfCreation : jobOfStartRequest;
    Job loser = submitted1 ? jobOfStartRequest : jobOfCreation;
    Job storedJob = await(Job.load(job.getId()));
    //The scheduler is paused, so the started job stays PENDING
    assertEquals(PENDING, storedJob.getStatus().getState());
    assertEquals(stepIds(winner), stepIds(storedJob), "The step graph of the submission that was done must be stored");
    assertTrue(Set.of(SUBMITTED, PENDING).contains(loser.getStatus().getState()),
        "The other submission must provide the stored status, but it was " + loser.getStatus().getState());
  }

  @Test
  public void startOfASubmittedJobReturnsTheCurrentStatus() throws Exception {
    pauseScheduler();
    Job job = new Job().withId(PREFIX + "_start").withDescription(TestCompiler.DESCRIPTION).withSource(new Files<>())
        .withTarget(new Files<>()).create();
    assertTrue(await(job.submit()));
    int compilationsBefore = compilations.get();

    RuntimeStatus status = await(new JobApi().tryExecuteAction(null, new RuntimeStatus().withDesiredAction(START),
        await(Job.load(job.getId()))));

    assertEquals(PENDING, status.getState());
    assertEquals(compilationsBefore, compilations.get(), "A job that was submitted already must not be compiled again");
    assertEquals(PENDING, await(Job.load(job.getId())).getStatus().getState());
  }

  @Test
  public void startExecutionDoesNotFailAJobThatWasStartedConcurrently() throws Exception {
    //No databases are needed by the test steps, this avoids loading them from the hub
    databaseCache().put("ALL_DATABASES", List.of());
    //The concurrent start stored RUNNING already, while this instance was loaded in PENDING state
    Job job = storeJob(PREFIX + "_execution", PENDING, PENDING);
    Job concurrentlyStartedJob = await(Job.load(job.getId()));
    concurrentlyStartedJob.getStatus().setState(RUNNING);
    await(concurrentlyStartedJob.storeStatus(PENDING));

    await(JobExecutor.getInstance().startExecution(job, null));

    assertEquals(RUNNING, await(Job.load(job.getId())).getStatus().getState());
    assertEquals(RUNNING, job.getStatus().getState(), "The job instance must reflect the stored status");
  }

  @Test
  public void nonFinalUpdateOfAFinalStepIsIgnored() throws Exception {
    Job job = storeJob(PREFIX + "_outOfOrder", RUNNING, PENDING);
    String stepId = job.getSteps().stepStream().findFirst().get().getId();
    //Loaded before the step succeeded
    Job staleJob = await(Job.load(job.getId()));
    await(await(Job.load(job.getId())).updateStepStatus(stepId, new RuntimeInfo().withState(SUCCEEDED), true));

    await(staleJob.updateStepStatus(stepId, new RuntimeInfo().withState(RUNNING), true));

    Job storedJob = await(Job.load(job.getId()));
    assertEquals(SUCCEEDED, storedJob.getStepById(stepId).getStatus().getState());
    assertEquals(RUNNING, storedJob.getStatus().getState());
  }

  @Test
  public void succeededStepsAreCountedAfterParallelCompletions() throws Exception {
    Job job = storeJob(PREFIX + "_parallel", RUNNING, RUNNING);
    //All step updates load the job before any of the others stores its update
    List<Job> loadedJobs = IntStream.range(0, STEP_COUNT).mapToObj(i -> loadJob(job.getId())).toList();
    List<String> stepIds = stepIds(job);

    await(Future.all(IntStream.range(0, STEP_COUNT)
        .mapToObj(i -> loadedJobs.get(i).updateStepStatus(stepIds.get(i), new RuntimeInfo().withState(SUCCEEDED), true))
        .toList()));
    //Precondition: The step updates overwrote each other's count
    assertEquals(1, await(Job.load(job.getId())).getStatus().getSucceededSteps());

    await(JobAdminApi.processSfnStateChangeEvent(sfnEvent(job.getId(), "SUCCEEDED")));

    Job storedJob = await(Job.load(job.getId()));
    assertEquals(SUCCEEDED, storedJob.getStatus().getState());
    assertEquals(STEP_COUNT, storedJob.getStatus().getSucceededSteps());
    assertEquals(storedJob.getStatus().getOverallStepCount(), storedJob.getStatus().getSucceededSteps());
  }

  @Test
  public void stepCompletionAfterTheJobSucceededIsCounted() throws Exception {
    Job job = storeJob(PREFIX + "_late", RUNNING, RUNNING);
    List<String> stepIds = stepIds(job);
    //Loaded before the state machine reported the job's success
    Job jobOfLateStepUpdate = await(Job.load(job.getId()));
    for (String stepId : stepIds.subList(1, STEP_COUNT))
      await(await(Job.load(job.getId())).updateStepStatus(stepId, new RuntimeInfo().withState(SUCCEEDED), true));
    await(JobAdminApi.processSfnStateChangeEvent(sfnEvent(job.getId(), "SUCCEEDED")));
    assertEquals(STEP_COUNT - 1, await(Job.load(job.getId())).getStatus().getSucceededSteps());

    await(jobOfLateStepUpdate.updateStepStatus(stepIds.get(0), new RuntimeInfo().withState(SUCCEEDED), true));

    Job storedJob = await(Job.load(job.getId()));
    assertEquals(SUCCEEDED, storedJob.getStatus().getState());
    assertEquals(STEP_COUNT, storedJob.getStatus().getSucceededSteps());
  }

  /**
   * Stores a job with {@link #STEP_COUNT} parallel steps.
   */
  private static Job storeJob(String jobId, State state, State stepState) throws Exception {
    StepGraph steps = new StepGraph().withParallel(true);
    for (int i = 0; i < STEP_COUNT; i++) {
      SimpleTestStep<?> step = new SimpleTestStep<>("param").withJobId(jobId);
      step.getStatus().setState(stepState);
      steps.addExecution(step);
    }
    Map<String, Object> jobMap = new Job()
        .withId(jobId)
        .withStatus(new RuntimeStatus().withState(state).withOverallStepCount(STEP_COUNT))
        .withSteps(steps)
        .toMap(Static.class);
    //Simulates a prepared job
    jobMap.put("resourceKeys", Set.of());
    Job job = XyzSerializable.fromMap(jobMap, Job.class);
    await(job.store());
    return job;
  }

  private static JsonObject sfnEvent(String jobId, String status) {
    return new JsonObject()
        .put("source", "aws.states")
        .put("detail", new JsonObject().put("name", jobId).put("status", status));
  }

  private static void pauseScheduler() {
    JobExecutor.applySchedulerState(new SchedulerStatePatch(PAUSED, null, null));
  }

  private static List<String> stepIds(Job job) {
    return job.getSteps().stepStream().map(Step::getId).sorted().collect(Collectors.toList());
  }

  private static Job loadJob(String jobId) {
    try {
      return await(Job.load(jobId));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Field configClientField() throws Exception {
    Field field = JobConfigClient.Provider.class.getDeclaredField("client");
    field.setAccessible(true);
    return field;
  }

  private static Cache<String, List<Database>> databaseCache() throws Exception {
    Field field = Database.class.getDeclaredField("cache");
    field.setAccessible(true);
    return (Cache<String, List<Database>>) field.get(null);
  }

  private static <T> T await(Future<T> future) throws Exception {
    return future.toCompletionStage().toCompletableFuture().get(30, SECONDS);
  }

  /**
   * A step that is always ready for the execution. It is stored as a {@link SimpleTestStep}, because a subtype that is registered
   * after a mapper of the fork deserialized steps already, could not be deserialized by that mapper.
   */
  @JsonTypeName("SimpleTestStep")
  public static class ReadyTestStep extends SimpleTestStep<ReadyTestStep> {
    @Override
    public boolean validate() {
      return true;
    }
  }

  /**
   * Compiles the test jobs of this class into {@link #STEP_COUNT} parallel steps.
   */
  public static class TestCompiler implements JobCompilationInterceptor {
    static final String DESCRIPTION = PREFIX + " test job";

    @Override
    public boolean chooseMe(Job job) {
      return DESCRIPTION.equals(job.getDescription());
    }

    @Override
    public CompilationStepGraph compile(Job job) {
      compilations.incrementAndGet();
      CyclicBarrier barrier = compilationBarrier;
      if (barrier != null)
        try {
          barrier.await(10, SECONDS);
        }
        catch (Exception e) {
          throw new RuntimeException("The concurrent compilation did not happen", e);
        }
      CompilationStepGraph graph = (CompilationStepGraph) new CompilationStepGraph().withParallel(true);
      for (int i = 0; i < STEP_COUNT; i++)
        graph.addExecution(new ReadyTestStep());
      return graph;
    }
  }
}
