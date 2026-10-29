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

package com.here.xyz.jobs.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.here.xyz.jobs.config.StatisticsConfigClient;
import com.here.xyz.jobs.config.StatisticsConfigClient.StatisticsConfig;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.util.service.Core;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.concurrent.TimeUnit;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class JobStatisticsRecorderTest {
  private static final String SPACE = "hrn:here:data::olp-here:my-catalog:address";
  private static final String OTHER_SPACE = "hrn:here:data::olp-here:my-catalog:place";

  static {
    //Steps read their config on construction; nothing here talks to a service
    if (Config.instance == null)
      new Config();
  }

  @BeforeAll
  public static void initVertx() {
    if (Core.vertx == null)
      Core.vertx = Vertx.vertx();
  }

  private final Map<Step, List<Output>> outputs = new IdentityHashMap<>();

  private TaskedImportFilesToSpace importStep(String spaceId, String operation, FeatureStatistics... statistics) {
    TaskedImportFilesToSpace step = new TaskedImportFilesToSpace().withSpaceId(spaceId);
    if (operation != null)
      step.withOutputMetadata(Map.of("layer", spaceId.substring(spaceId.lastIndexOf(':') + 1), "operation", operation));
    outputs.put(step, List.of(statistics));
    return step;
  }

  private static FeatureStatistics statistics(Ref versionRef, long rows, long bytes) {
    return new FeatureStatistics().withFeatureCount(rows).withByteSize(bytes).withVersionRef(versionRef);
  }

  private List<StatisticsConfig> collect(Step... steps) {
    return JobStatisticsRecorder.collect("job1", Stream.of(steps), step -> outputs.getOrDefault(step, List.of()), 1_700_000_000_000L);
  }

  @Test
  public void patchStepsOfOneVersionAreCombined() {
    List<StatisticsConfig> items = collect(
        importStep(SPACE, "added", statistics(new Ref(4), 10, 100)),
        importStep(SPACE, "updated", statistics(new Ref(4), 5, 50)),
        importStep(SPACE, "deleted", statistics(new Ref(4), 3, 30)));

    assertEquals(1, items.size());
    StatisticsConfig item = items.get(0);
    assertEquals(SPACE, item.getSpaceId());
    assertEquals(4, item.getVersion());
    assertEquals("job1", item.getJobId());
    assertEquals(Map.of("added", 10L, "updated", 5L, "deleted", 3L, "bytes", 180L), item.getRequested());
  }

  @Test
  public void fullReleaseIsOneTotalPerLayer() {
    List<StatisticsConfig> items = collect(
        importStep(SPACE, null, statistics(new Ref(1), 1000, 9000)),
        importStep(OTHER_SPACE, null, statistics(new Ref(1), 7, 70)));

    assertEquals(2, items.size());
    assertEquals(Map.of("total", 1000L, "bytes", 9000L), items.get(0).getRequested());
    assertEquals(OTHER_SPACE, items.get(1).getSpaceId());
  }

  @Test
  public void exportStepsAreNotRecorded() {
    ExportSpaceToFiles export = new ExportSpaceToFiles().withSpaceId(SPACE);
    outputs.put(export, List.of(statistics(new Ref(5), 42, 420)));

    assertTrue(collect(export).isEmpty());
  }

  @Test
  public void statisticsWithoutANumericVersionAreSkipped() {
    List<StatisticsConfig> items = collect(
        importStep(SPACE, "added", statistics(null, 1, 1)),
        importStep(SPACE, "added", statistics(new Ref(Ref.HEAD), 1, 1)),
        importStep(SPACE, "added", statistics(new Ref("my-tag"), 1, 1)),
        importStep(SPACE, "added", statistics(new Ref("2..5"), 1, 1)));

    assertTrue(items.isEmpty());
  }

  @Test
  public void numericVersionAcceptsOnlySingleNumbers() {
    assertEquals(3L, JobStatisticsRecorder.numericVersion(new Ref(3)));
    assertNull(JobStatisticsRecorder.numericVersion(new Ref(Ref.HEAD)));
    assertNull(JobStatisticsRecorder.numericVersion(new Ref("tag1")));
    assertNull(JobStatisticsRecorder.numericVersion(new Ref(Ref.ALL_VERSIONS)));
    assertNull(JobStatisticsRecorder.numericVersion(null));
  }

  @Test
  public void aFailingWriteDoesNotFailTheRecording() throws Exception {
    StatisticsConfigClient failingClient = new StatisticsConfigClient("arn:aws:dynamodb:localhost:000000008000:table/unused") {
      @Override
      public void storeStatistics(List<StatisticsConfig> statistics) {
        throw new IllegalStateException("DynamoDB is not reachable");
      }
    };

    assertSucceeds(JobStatisticsRecorder.record("job1", () -> List.of(new StatisticsConfig().withSpaceId(SPACE)), failingClient));
  }

  @Test
  public void aFailingCollectionDoesNotFailTheRecording() throws Exception {
    StatisticsConfigClient client = new StatisticsConfigClient("arn:aws:dynamodb:localhost:000000008000:table/unused") {
      @Override
      public void storeStatistics(List<StatisticsConfig> statistics) {
        fail("nothing must be stored");
      }
    };

    assertSucceeds(JobStatisticsRecorder.record("job1", () -> {
      throw new IllegalStateException("outputs not readable");
    }, client));
  }

  @Test
  public void withoutATableNothingIsRecorded() throws Exception {
    assertSucceeds(JobStatisticsRecorder.record("job1", () -> fail("nothing must be collected"), null));
  }

  private static void assertSucceeds(Future<Void> recording) throws Exception {
    recording.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    assertTrue(recording.succeeded());
  }
}
