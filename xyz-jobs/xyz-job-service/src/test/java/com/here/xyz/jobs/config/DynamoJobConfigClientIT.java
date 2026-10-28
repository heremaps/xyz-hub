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

import static com.here.xyz.jobs.RuntimeInfo.State.CANCELLED;
import static com.here.xyz.jobs.RuntimeInfo.State.FAILED;
import static com.here.xyz.jobs.RuntimeInfo.State.NOT_READY;
import static com.here.xyz.jobs.RuntimeInfo.State.PENDING;
import static com.here.xyz.jobs.RuntimeInfo.State.RUNNING;
import static com.here.xyz.jobs.RuntimeInfo.State.SUBMITTED;
import static com.here.xyz.jobs.RuntimeInfo.State.SUCCEEDED;
import static com.here.xyz.util.Random.randomAlpha;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.RuntimeStatus;
import com.here.xyz.jobs.config.JobConfigClient.FilteredValues;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.DatasetDescription.Space;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.service.Config;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.execution.JobExecutor;
import com.here.xyz.jobs.steps.execution.fusion.SimpleTestStep;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.aws.dynamo.DynamoClient;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Checks that {@link DynamoJobConfigClient} filters the jobs by state before loading their steps (one step-table query per job),
 * with many jobs on a shared base resource key, as seen by the reuse and single-job-per-resource checks.
 * One job item on the base resource key can not be deserialized, the job listings skip it (DS-2715).
 * Also checks the conditions of the status and step writes.
 */
public class DynamoJobConfigClientIT {
  private static final Logger logger = LogManager.getLogger();
  private static final String DYNAMO_HOST = System.getProperty("job.host", "localhost");
  //Dedicated tables, so that a job service using the same DynamoDB local does not see the test jobs (it can't deserialize
  // their test-only steps, which would break its check of the PENDING jobs)
  private static final String TABLE_ARN_PREFIX = "arn:aws:dynamodb:" + DYNAMO_HOST + ":000000008000:table/"
      + DynamoJobConfigClientIT.class.getSimpleName() + "-";
  private static final String PREFIX = DynamoJobConfigClientIT.class.getSimpleName() + "_" + randomAlpha(5);
  //Base resource (detected by the "-stable-" naming convention), shared by all jobs of this test
  private static final String BASE_LAYER = PREFIX + "-stable-base";
  private static final String COMPOSITE_LAYER = PREFIX + "-composite-a";
  private static final int FINISHED_COMPOSITE_JOBS = 30;
  private static final State[] FINISHED_STATES = {SUCCEEDED, FAILED, CANCELLED};
  //A PENDING job on the base layer whose job item can not be deserialized (DS-2715), it is not part of storedJobs
  private static final String BROKEN_JOB_ID = PREFIX + "_broken";

  private static CountingStepConfigClient stepConfigClient;
  private static DynamoJobConfigClient client;
  private static final List<Job> storedJobs = new ArrayList<>();
  private static final List<Job> jobsForConditions = new CopyOnWriteArrayList<>();
  private static Set<String> succeededJobIds;
  private static Set<String> pendingJobIds;
  private static String pendingJobOnBaseLayerId;

  @BeforeAll
  public static void setUp() throws Exception {
    if (Core.vertx == null)
      Core.vertx = Vertx.vertx();
    //Needed for deserializing step graphs
    if (Config.instance == null)
      new Config();
    XyzSerializable.registerSubtypes(StepGraph.class, SimpleTestStep.class);

    //Must be created after Core.vertx is set
    stepConfigClient = new CountingStepConfigClient();
    //Creates the local tables if they do not exist yet
    client = new DynamoJobConfigClient(TABLE_ARN_PREFIX + "jobs", TABLE_ARN_PREFIX + "job-resources", stepConfigClient);
    await(client.init());

    long createdAt = System.currentTimeMillis() - 3_600_000;
    //Many finished jobs on composite layers which all inherit the base layer
    for (int i = 0; i < FINISHED_COMPOSITE_JOBS; i++)
      storeJob(compositeSpace(PREFIX + "-composite-" + i, BASE_LAYER), FINISHED_STATES[i % FINISHED_STATES.length], createdAt++);
    //Some finished jobs directly on the base layer
    storeJob(space(BASE_LAYER), SUCCEEDED, createdAt++);
    storeJob(space(BASE_LAYER), FAILED, createdAt++);
    //PENDING jobs: one directly on the base layer and two on other composite layers only inheriting the base layer
    pendingJobOnBaseLayerId = storeJob(space(BASE_LAYER), PENDING, createdAt++).getId();
    storeJob(compositeSpace(PREFIX + "-composite-p1", BASE_LAYER), PENDING, createdAt++);
    storeJob(compositeSpace(PREFIX + "-composite-p2", BASE_LAYER), PENDING, createdAt++);

    succeededJobIds = idsInState(SUCCEEDED);
    pendingJobIds = idsInState(PENDING);

    storeBrokenJob(createdAt);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (client != null) {
      for (Job job : storedJobs)
        await(client.deleteJob(job.getId()));
      await(client.deleteJob(BROKEN_JOB_ID));
      for (Job job : jobsForConditions)
        await(client.deleteJob(job.getId()));
    }
  }

  @BeforeEach
  public void resetCounter() {
    stepConfigClient.loadedJobIds.clear();
  }

  @Test
  public void loadJobsByStateLoadsStepsOnlyForTheReturnedJobs() throws Exception {
    Set<Job> succeededJobs = await(client.loadJobs(newCompositeJob().getResourceKeys(), SUCCEEDED));

    assertEquals(succeededJobIds, ids(succeededJobs));
    for (Job job : succeededJobs) {
      assertNotNull(job.getSteps(), "The steps of job " + job.getId() + " must be loaded");
      assertEquals(1, job.getSteps().stepStream().count());
      assertEquals(SimpleTestStep.class, job.getSteps().stepStream().findFirst().get().getClass());
    }
    logStepLoads("loadJobs(resourceKeys, SUCCEEDED)");
    assertEquals(succeededJobIds, stepConfigClient.loadedJobIds.keySet(), "Steps must only be loaded for the returned jobs");
    stepConfigClient.loadedJobIds.values().forEach(count -> assertEquals(1, count));
  }

  @Test
  public void loadJobsWithoutStepsLoadsNoSteps() throws Exception {
    Job newJob = newCompositeJob();
    Set<Job> pendingJobs = await(client.loadJobsWithoutSteps(newJob.getResourceKeys(), PENDING));

    assertEquals(pendingJobIds, ids(pendingJobs));
    Map<String, Job> expectedJobs = storedJobs.stream().collect(Collectors.toMap(Job::getId, job -> job));
    for (Job job : pendingJobs) {
      Job expectedJob = expectedJobs.get(job.getId());
      assertNull(job.getSteps());
      assertEquals(PENDING, job.getStatus().getState());
      assertEquals(expectedJob.getCreatedAt(), job.getCreatedAt());
      assertEquals(expectedJob.getResourceKeys(), job.getResourceKeys());
      assertEquals(expectedJob.getResourceKey(), job.getResourceKey());
    }
    logStepLoads("loadJobsWithoutSteps(resourceKeys, PENDING)");
    assertTrue(stepConfigClient.loadedJobIds.isEmpty(), "No steps must be loaded");

    //The base resource key still counts (DS-2782): only the PENDING job directly on the base layer is a conflict
    assertEquals(List.of(pendingJobOnBaseLayerId), JobExecutor.olderPendingConflicts(newJob, pendingJobs).stream()
        .map(Job::getId).toList());
  }

  @Test
  public void loadJobsWithoutStepsReturnsTheSameJobsAsLoadJobs() throws Exception {
    Set<String> resourceKeys = newCompositeJob().getResourceKeys();
    for (State state : List.of(PENDING, SUCCEEDED, FAILED, CANCELLED))
      assertEquals(ids(await(client.loadJobs(resourceKeys, state))), ids(await(client.loadJobsWithoutSteps(resourceKeys, state))),
          "Different jobs for state " + state);
  }

  @Test
  public void loadJobsByPrimaryResourceKeyAndStateLoadsStepsOnlyForTheReturnedJobs() throws Exception {
    Set<Job> pendingJobs = await(client.loadJobsByPrimaryResourceKey(BASE_LAYER, new FilteredValues<>(true, Set.of(PENDING))));

    assertEquals(Set.of(pendingJobOnBaseLayerId), ids(pendingJobs));
    assertNotNull(pendingJobs.iterator().next().getSteps());
    logStepLoads("loadJobsByPrimaryResourceKey(baseKey, include PENDING)");
    assertEquals(Set.of(pendingJobOnBaseLayerId), stepConfigClient.loadedJobIds.keySet(),
        "Steps must only be loaded for the returned jobs");
  }

  @Test
  public void loadJobsWithoutStateLoadsAllJobs() throws Exception {
    Set<Job> jobs = await(client.loadJobs(newCompositeJob().getResourceKeys(), null));

    Set<String> allJobIds = ids(storedJobs);
    assertEquals(allJobIds, ids(jobs));
    jobs.forEach(job -> assertNotNull(job.getSteps()));
    //This is the number of step-table queries each of the checks did before DS-2817
    logStepLoads("loadJobs(resourceKeys, null)");
    assertEquals(allJobIds, stepConfigClient.loadedJobIds.keySet());
  }

  @Test
  public void undeserializableJobIsSkippedInJobListings() throws Exception {
    //Loading the broken job by its ID still fails
    assertThrows(ExecutionException.class, () -> await(client.loadJob(BROKEN_JOB_ID)));

    Set<String> allJobIds = ids(storedJobs);
    Set<String> jobIdsOnBaseLayer = storedJobs.stream().filter(job -> BASE_LAYER.equals(job.getResourceKey())).map(Job::getId)
        .collect(Collectors.toSet());

    //The tables are kept between runs, only look at the jobs of this run
    assertEquals(allJobIds, idsOfThisRun(await(client.loadJobs())));
    assertEquals(pendingJobIds, idsOfThisRun(await(client.loadJobs(PENDING))));
    assertEquals(jobIdsOnBaseLayer, ids(await(client.loadJobsByPrimaryResourceKey(BASE_LAYER))));
    assertEquals(Set.of(pendingJobOnBaseLayerId), ids(await(client.loadJobsByPrimaryResourceKey(BASE_LAYER,
        new FilteredValues<>(true, Set.of(PENDING))))));
    assertEquals(pendingJobIds, ids(await(client.loadJobs(newCompositeJob().getResourceKeys(), PENDING))));
    assertEquals(pendingJobIds, ids(await(client.loadJobsWithoutSteps(newCompositeJob().getResourceKeys(), PENDING))));
    //The scan with filters, as used by the admin API (e.g. the job dashboard)
    assertEquals(allJobIds, ids(await(client.loadJobs(null, null, null, null, null,
        new FilteredValues<>(true, Set.of(BASE_LAYER)), null))));
  }

  /**
   * Stores a PENDING job on the base layer and replaces its source in the job item with one of an unknown type.
   */
  private static void storeBrokenJob(long createdAt) throws Exception {
    Job brokenJob = stored(new Job()
        .withId(BROKEN_JOB_ID)
        .withSource(space(BASE_LAYER))
        .withTarget(new Files<>())
        .withStatus(new RuntimeStatus().withState(PENDING))
        .withCreatedAt(createdAt)
        .withKeepUntil(System.currentTimeMillis() + 3_600_000)
        .withSteps(new StepGraph().addExecution(new SimpleTestStep<>("param").withJobId(BROKEN_JOB_ID))));
    await(client.storeJob(brokenJob));
    new DynamoClient(TABLE_ARN_PREFIX + "jobs", null).getTable().updateItem(new UpdateItemSpec()
        .withPrimaryKey("id", BROKEN_JOB_ID)
        .withUpdateExpression("SET #source = :source")
        .withNameMap(Map.of("#source", "source"))
        .withValueMap(Map.of(":source", Map.of("type", "UnknownDatasetType"))));
  }

  private static Set<String> idsOfThisRun(Collection<Job> jobs) {
    return ids(jobs).stream().filter(jobId -> jobId.startsWith(PREFIX + "_")).collect(Collectors.toSet());
  }

  @Test
  public void updateStatusOnlyWritesTheExpectedPreviousState() throws Exception {
    Job job = storeJobForConditions("_status", NOT_READY, NOT_READY);
    Job submission1 = await(client.loadJob(job.getId()));
    Job submission2 = await(client.loadJob(job.getId()));
    submission1.getStatus().setState(SUBMITTED);
    submission2.getStatus().setState(SUBMITTED);

    await(client.updateStatus(submission1, NOT_READY));
    ExecutionException conflict = assertThrows(ExecutionException.class, () -> await(client.updateStatus(submission2, NOT_READY)));

    assertTrue(JobConfigClient.isStateConflict(conflict), "Unexpected error: " + conflict);
    assertEquals(SUBMITTED, await(client.loadJob(job.getId())).getStatus().getState());
  }

  @Test
  public void nonFinalUpdateOfAFinalStepIsIgnoredWithoutError() throws Exception {
    Job job = storeJobForConditions("_step", RUNNING, SUCCEEDED);
    Step<?> step = job.getSteps().stepStream().findFirst().get();
    //An update that was sent before the final one, but arrived after it
    Map<String, Object> stepMap = step.toMap(Static.class);
    ((Map<String, Object>) stepMap.get("status")).put("state", RUNNING.toString());
    Step<?> outOfOrderUpdate = XyzSerializable.fromMap(stepMap, Step.class);

    CapturingAppender appender = new CapturingAppender();
    org.apache.logging.log4j.core.Logger clientLogger = (org.apache.logging.log4j.core.Logger) LogManager.getLogger(
        DynamoJobConfigClient.class);
    appender.start();
    clientLogger.addAppender(appender);
    try {
      await(client.updateStep(job, outOfOrderUpdate));

      assertEquals(SUCCEEDED, await(client.loadStep(job.getId(), step.getId())).getStatus().getState());
      //The logging is asynchronous
      long deadline = System.currentTimeMillis() + 10_000;
      while (appender.events.stream().noneMatch(event -> event.getLevel() == Level.INFO) && System.currentTimeMillis() < deadline)
        Thread.sleep(50);
      assertTrue(appender.events.stream().anyMatch(event -> event.getLevel() == Level.INFO
          && event.getMessage().getFormattedMessage().contains("Ignored the update")), "The ignored update must be logged");
      assertTrue(appender.events.stream().noneMatch(event -> event.getLevel().isMoreSpecificThan(Level.WARN)),
          "No warning or error must be logged");
    }
    finally {
      clientLogger.removeAppender(appender);
      appender.stop();
    }
  }

  /**
   * Stores a job on its own resource, so it is not found by the other tests of this class.
   */
  private static Job storeJobForConditions(String suffix, State state, State stepState) throws Exception {
    String jobId = PREFIX + suffix;
    SimpleTestStep<?> step = new SimpleTestStep<>("param").withJobId(jobId);
    step.getStatus().setState(stepState);
    Job job = stored(new Job()
        .withId(jobId)
        .withSource(space(PREFIX + "-conditions"))
        .withTarget(new Files<>())
        .withStatus(new RuntimeStatus().withState(state))
        .withCreatedAt(System.currentTimeMillis())
        .withKeepUntil(System.currentTimeMillis() + 3_600_000)
        .withSteps(new StepGraph().addExecution(step)));
    jobsForConditions.add(job);
    await(client.storeJob(job));
    return job;
  }

  private static class CapturingAppender extends AbstractAppender {
    private final List<LogEvent> events = new CopyOnWriteArrayList<>();

    CapturingAppender() {
      super(DynamoJobConfigClientIT.class.getSimpleName() + "-" + randomAlpha(5), null, null, true, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
      events.add(event.toImmutable());
    }
  }

  private static void logStepLoads(String call) {
    logger.info("{}: {} job(s) stored on the base key, {} step-table queries", call, storedJobs.size(),
        stepConfigClient.loadedJobIds.values().stream().mapToInt(Integer::intValue).sum());
  }

  /**
   * A new job on a composite layer extending the base layer, as checked by the reuse and single-job-per-resource checks.
   */
  private static Job newCompositeJob() throws Exception {
    return stored(new Job()
        .withId(PREFIX + "_new")
        .withSource(compositeSpace(COMPOSITE_LAYER, BASE_LAYER))
        .withTarget(new Files<>())
        .withStatus(new RuntimeStatus().withState(PENDING))
        .withCreatedAt(System.currentTimeMillis()));
  }

  private static Job storeJob(DatasetDescription source, State state, long createdAt) throws Exception {
    String jobId = PREFIX + "_" + storedJobs.size();
    Job job = stored(new Job()
        .withId(jobId)
        .withSource(source)
        .withTarget(new Files<>())
        .withStatus(new RuntimeStatus().withState(state))
        .withCreatedAt(createdAt)
        .withKeepUntil(System.currentTimeMillis() + 3_600_000)
        .withSteps(new StepGraph().addExecution(new SimpleTestStep<>("param").withJobId(jobId))));
    storedJobs.add(job);
    await(client.storeJob(job));
    return job;
  }

  private static Set<String> idsInState(State state) {
    return storedJobs.stream().filter(job -> job.getStatus().getState() == state).map(Job::getId).collect(Collectors.toSet());
  }

  private static Set<String> ids(Collection<Job> jobs) {
    return jobs.stream().map(Job::getId).collect(Collectors.toSet());
  }

  /**
   * Simulates a job that was prepared (resource keys are set), as the job service stores it.
   */
  private static Job stored(Job job) {
    Set<String> resourceKeys = new HashSet<>(job.getSource().getResourceKeys());
    resourceKeys.addAll(job.getTarget().getResourceKeys());

    Map<String, Object> persistedJob = job.toMap(Static.class);
    persistedJob.put("resourceKeys", resourceKeys);
    return XyzSerializable.fromMap(persistedJob, Job.class);
  }

  private static Space space(String spaceId) {
    return (Space) new Space<>().withId(spaceId);
  }

  private static Space compositeSpace(String spaceId, String extendedSpaceId) throws Exception {
    Space space = space(spaceId);
    //Simulates the outcome of DatasetDescription.Space#prepare() without having to call the hub
    Field extendedSpaceIdField = Space.class.getDeclaredField("extendedSpaceId");
    extendedSpaceIdField.setAccessible(true);
    extendedSpaceIdField.set(space, Optional.of(extendedSpaceId));
    return space;
  }

  private static <T> T await(Future<T> future) throws Exception {
    return future.toCompletionStage().toCompletableFuture().get(60, SECONDS);
  }

  /**
   * Counts the step-table queries per job ID.
   */
  private static class CountingStepConfigClient extends StepConfigClient {
    private final Map<String, Integer> loadedJobIds = new ConcurrentHashMap<>();

    CountingStepConfigClient() {
      super(TABLE_ARN_PREFIX + "job-steps");
    }

    @Override
    public List<StepConfig> loadStepsForJob(String jobId) {
      loadedJobIds.merge(jobId, 1, Integer::sum);
      return super.loadStepsForJob(jobId);
    }
  }
}
