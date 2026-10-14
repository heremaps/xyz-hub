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

import static com.here.xyz.jobs.steps.execution.JobExecutor.resourcesConflict;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.DatasetDescription.Space;
import com.here.xyz.jobs.datasets.Files;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class SingleJobPerResourceTest {
  //Base / global resource (detected by the "-stable-" naming convention)
  private static final String STABLE_BASE_LAYER = "some-stable-layer";
  //A regular layer that is extended, but is not a base / global resource
  private static final String NORMAL_LAYER = "some-base-layer";

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
