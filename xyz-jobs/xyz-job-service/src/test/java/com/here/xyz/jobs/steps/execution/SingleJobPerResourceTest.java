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

package com.here.xyz.jobs.steps.execution;

import static com.here.xyz.jobs.RuntimeInfo.State.PENDING;
import static com.here.xyz.jobs.steps.execution.JobExecutor.olderPendingConflicts;
import static com.here.xyz.jobs.steps.execution.JobExecutor.resourcesConflict;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeStatus;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.DatasetDescription.Space;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.service.Config;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.execution.fusion.SimpleTestStep;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SingleJobPerResourceTest {
  //Base / global resource (detected by the "-stable-" naming convention)
  private static final String STABLE_BASE_LAYER = "some-stable-layer";
  //A regular layer that is extended, but is not a base / global resource
  private static final String NORMAL_LAYER = "some-base-layer";

  @BeforeAll
  public static void registerTestSubtypes() {
    //Needed for deserializing step graphs
    if (Config.instance == null)
      new Config();
    XyzSerializable.registerSubtypes(StepGraph.class, SimpleTestStep.class);
  }

  @Test
  public void onlyTheOlderPendingJobDirectlyOnTheStableLayerBlocksACompositeJob() throws Exception {
    Job directOnBase = pendingJob("direct-on-base", space(STABLE_BASE_LAYER), 1);
    Job inheritingB = pendingJob("inheriting-b", compositeSpace("composite-b", STABLE_BASE_LAYER), 2);
    Job inheritingC = pendingJob("inheriting-c", compositeSpace("composite-c", STABLE_BASE_LAYER), 3);
    Job job = pendingJob("job", compositeSpace("composite-a", STABLE_BASE_LAYER), 10);

    assertEquals(List.of(directOnBase.getId()), ids(olderPendingConflicts(job, List.of(inheritingC, directOnBase, inheritingB, job))));
  }

  @Test
  public void newerPendingJobsAndTheJobItselfAreNoOlderPendingConflicts() throws Exception {
    Job job = pendingJob("job", compositeSpace("composite-a", STABLE_BASE_LAYER), 10);
    Job newerOnBase = pendingJob("newer-on-base", space(STABLE_BASE_LAYER), 20);
    Job newerOnSameLayer = pendingJob("newer-on-same-layer", compositeSpace("composite-a", STABLE_BASE_LAYER), 30);

    assertEquals(List.of(), olderPendingConflicts(job, List.of(job, newerOnBase, newerOnSameLayer)));
  }

  @Test
  public void olderPendingConflictsDoNotDependOnTheSteps() throws Exception {
    Job job = pendingJob("job", compositeSpace("composite-a", STABLE_BASE_LAYER), 10);
    List<Job> pendingJobs = List.of(
        pendingJob("on-same-layer", compositeSpace("composite-a", STABLE_BASE_LAYER), 5),
        pendingJob("direct-on-base", space(STABLE_BASE_LAYER), 2),
        pendingJob("inheriting-b", compositeSpace("composite-b", STABLE_BASE_LAYER), 1),
        pendingJob("newer-on-base", space(STABLE_BASE_LAYER), 20),
        job);
    List<Job> pendingJobsWithoutSteps = pendingJobs.stream().map(SingleJobPerResourceTest::withoutSteps).toList();
    pendingJobs.forEach(pendingJob -> assertNotNull(pendingJob.getSteps()));
    pendingJobsWithoutSteps.forEach(pendingJob -> assertNull(pendingJob.getSteps()));

    assertEquals(List.of("direct-on-base", "on-same-layer"), ids(olderPendingConflicts(job, pendingJobs)));
    assertEquals(List.of("direct-on-base", "on-same-layer"), ids(olderPendingConflicts(job, pendingJobsWithoutSteps)));
    assertEquals(List.of("direct-on-base", "on-same-layer"), ids(olderPendingConflicts(withoutSteps(job), pendingJobsWithoutSteps)));
  }

  @Test
  public void compositeJobsSharingOnlyTheStableLayerDoNotConflict() throws Exception {
    Job job = exportJob(compositeSpace("composite-a", STABLE_BASE_LAYER));
    Job otherJob = importJob(compositeSpace("composite-b", STABLE_BASE_LAYER));

    assertEquals(Set.of("composite-a", STABLE_BASE_LAYER), job.getResourceKeys());
    assertEquals(Set.of("composite-b", STABLE_BASE_LAYER), otherJob.getResourceKeys());
    assertConflict(job, otherJob, false);
  }

  @Test
  public void jobsDirectlyOnTheSameStableLayerConflict() {
    Job job = exportJob(space(STABLE_BASE_LAYER));
    Job otherJob = importJob(space(STABLE_BASE_LAYER));

    assertEquals(STABLE_BASE_LAYER, job.getResourceKey());
    assertEquals(STABLE_BASE_LAYER, otherJob.getResourceKey());
    assertConflict(job, otherJob, true);
  }

  @Test
  public void jobDirectlyOnTheStableLayerConflictsWithCompositeJobExtendingIt() throws Exception {
    assertConflict(importJob(space(STABLE_BASE_LAYER)), exportJob(compositeSpace("composite-a", STABLE_BASE_LAYER)), true);
  }

  @Test
  public void compositeJobConflictsWithJobDirectlyOnItsStableLayer() throws Exception {
    assertConflict(exportJob(compositeSpace("composite-a", STABLE_BASE_LAYER)), importJob(space(STABLE_BASE_LAYER)), true);
    assertConflict(importJob(compositeSpace("composite-a", STABLE_BASE_LAYER)), exportJob(space(STABLE_BASE_LAYER)), true);
  }

  @Test
  public void jobsOnTheSameNonBaseLayerConflict() {
    assertConflict(importJob(space("layer")), exportJob(space("layer")), true);
  }

  @Test
  public void jobsOnTheSameCompositeLayerConflict() throws Exception {
    assertConflict(importJob(compositeSpace("composite-a", STABLE_BASE_LAYER)),
        exportJob(compositeSpace("composite-a", STABLE_BASE_LAYER)), true);
  }

  @Test
  public void compositeJobsSharingANonStableBaseLayerConflict() throws Exception {
    assertConflict(importJob(compositeSpace("composite-a", NORMAL_LAYER)),
        exportJob(compositeSpace("composite-b", NORMAL_LAYER)), true);
  }

  @Test
  public void unrelatedJobsDoNotConflict() throws Exception {
    assertConflict(importJob(space("layer-1")), exportJob(space("layer-2")), false);
    assertConflict(importJob(compositeSpace("composite-a", STABLE_BASE_LAYER)), exportJob(space("layer-2")), false);
  }

  private static void assertConflict(Job job, Job otherJob, boolean expectedConflict) {
    assertEquals(expectedConflict, resourcesConflict(job, otherJob));
    assertEquals(expectedConflict, resourcesConflict(otherJob, job));
  }

  private static Job importJob(DatasetDescription target) {
    return stored(new Job().withId("import-" + target.getKey()).withSource(new Files<>()).withTarget(target));
  }

  private static Job exportJob(DatasetDescription source) {
    return stored(new Job().withId("export-" + source.getKey()).withSource(source).withTarget(new Files<>()));
  }

  /**
   * Simulates a job that was prepared, stored and loaded again (as the scheduler sees it).
   */
  private static Job stored(Job job) {
    Set<String> resourceKeys = new HashSet<>(job.getSource().getResourceKeys());
    resourceKeys.addAll(job.getTarget().getResourceKeys());

    Map<String, Object> persistedJob = job.toMap(Static.class);
    persistedJob.put("resourceKeys", resourceKeys);
    return XyzSerializable.fromMap(persistedJob, Job.class);
  }

  /**
   * A stored PENDING export job with a small step graph.
   */
  private static Job pendingJob(String jobId, DatasetDescription source, long createdAt) {
    return stored(new Job()
        .withId(jobId)
        .withSource(source)
        .withTarget(new Files<>())
        .withStatus(new RuntimeStatus().withState(PENDING))
        .withCreatedAt(createdAt)
        .withSteps(new StepGraph().addExecution(new SimpleTestStep<>("param").withJobId(jobId))));
  }

  /**
   * Simulates a job that was loaded without its steps, see {@link com.here.xyz.jobs.config.JobConfigClient#loadJobsWithoutSteps}.
   */
  private static Job withoutSteps(Job job) {
    Map<String, Object> persistedJob = job.toMap(Static.class);
    persistedJob.remove("steps");
    return XyzSerializable.fromMap(persistedJob, Job.class);
  }

  private static List<String> ids(Collection<Job> jobs) {
    return jobs.stream().map(Job::getId).toList();
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
}
