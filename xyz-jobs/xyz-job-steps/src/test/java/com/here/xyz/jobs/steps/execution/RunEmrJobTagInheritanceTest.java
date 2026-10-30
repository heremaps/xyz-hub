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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.jobs.steps.Config;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.emrserverless.model.ResourceNotFoundException;

class RunEmrJobTagInheritanceTest {
  private static final String ODIN_APP_ID = "odin_app_id";
  private static final AtomicInteger loadCount = new AtomicInteger();
  private Config previousConfig;

  @BeforeEach
  void setUp() {
    previousConfig = Config.instance;
    new Config();
    Config.instance.LOCALSTACK_ENDPOINT = null;
    RunEmrJob.clearApplicationTagsCache();
    loadCount.set(0);
  }

  @AfterEach
  void tearDown() {
    RunEmrJob.clearApplicationTagsCache();
    Config.instance = previousConfig;
  }

  @Test
  void inheritsOdinAppIdOfApplication() throws Exception {
    RunEmrJob step = emrStep("app1", Map.of(ODIN_APP_ID, "2963", "Project", "other"));

    step.prepare("someOwner", null);

    assertEquals(Map.of(ODIN_APP_ID, "2963"), step.getTags());
  }

  @Test
  void keepsExistingTags() throws Exception {
    RunEmrJob step = emrStep("app1", Map.of(ODIN_APP_ID, "2963"));
    step.getTags().put("owner", "someOwner");

    step.prepare("someOwner", null);

    assertEquals(Map.of("owner", "someOwner", ODIN_APP_ID, "2963"), step.getTags());
  }

  @Test
  void explicitTagTakesPrecedence() throws Exception {
    RunEmrJob step = emrStep("app1", Map.of(ODIN_APP_ID, "2963"));
    step.getTags().put(ODIN_APP_ID, "1234");

    step.prepare("someOwner", null);

    assertEquals("1234", step.getTags().get(ODIN_APP_ID));
  }

  @Test
  void inheritsTagWithImmutableExistingTags() throws Exception {
    Map<String, String> explicitTags = Map.of("owner", "someOwner");
    RunEmrJob step = emrStep("app1", Map.of(ODIN_APP_ID, "2963")).withTags(explicitTags);

    step.prepare("someOwner", null);

    assertEquals(Map.of("owner", "someOwner", ODIN_APP_ID, "2963"), step.getTags());
    assertEquals(Map.of("owner", "someOwner"), explicitTags);
  }

  @Test
  void immutableExplicitTagTakesPrecedence() throws Exception {
    RunEmrJob step = emrStep("app1", Map.of(ODIN_APP_ID, "2963")).withTags(Map.of(ODIN_APP_ID, "1234"));

    step.prepare("someOwner", null);

    assertEquals(Map.of(ODIN_APP_ID, "1234"), step.getTags());
  }

  @Test
  void applicationWithoutOdinAppIdDoesNotFail() {
    RunEmrJob step = emrStep("app1", Map.of("Project", "other"));

    assertDoesNotThrow(() -> step.prepare("someOwner", null));
    assertTrue(step.getTags().isEmpty());
  }

  @Test
  void applicationWithoutAnyTagsDoesNotFail() {
    RunEmrJob step = emrStep("app1", null);

    assertDoesNotThrow(() -> step.prepare("someOwner", null));
    assertTrue(step.getTags().isEmpty());
  }

  @Test
  void failingLookupDoesNotFailAndIsRetried() {
    RunEmrJob failingStep = new TestEmrJob(null, true).withApplicationId("app1");

    assertDoesNotThrow(() -> failingStep.prepare("someOwner", null));
    assertTrue(failingStep.getTags().isEmpty());

    //The failure must not be cached
    RunEmrJob step = emrStep("app1", Map.of(ODIN_APP_ID, "2963"));
    assertDoesNotThrow(() -> step.prepare("someOwner", null));
    assertEquals("2963", step.getTags().get(ODIN_APP_ID));
    assertEquals(2, loadCount.get());
  }

  @Test
  void applicationTagsAreCachedPerApplication() throws Exception {
    emrStep("app1", Map.of(ODIN_APP_ID, "2963")).prepare("someOwner", null);
    emrStep("app1", Map.of(ODIN_APP_ID, "2963")).prepare("someOwner", null);
    assertEquals(1, loadCount.get());

    emrStep("app2", Map.of(ODIN_APP_ID, "2963")).prepare("someOwner", null);
    assertEquals(2, loadCount.get());
  }

  @Test
  void nullTagsAreInitialized() throws Exception {
    RunEmrJob step = emrStep("app1", Map.of(ODIN_APP_ID, "2963")).withTags(null);

    step.prepare("someOwner", null);

    assertEquals(Map.of(ODIN_APP_ID, "2963"), step.getTags());
  }

  @Test
  void noLookupWithoutApplicationId() throws Exception {
    RunEmrJob step = emrStep(null, Map.of(ODIN_APP_ID, "2963"));

    step.prepare("someOwner", null);

    assertTrue(step.getTags().isEmpty());
    assertEquals(0, loadCount.get());
  }

  @Test
  void noLookupWhenRunningLocally() throws Exception {
    Config.instance.LOCALSTACK_ENDPOINT = new URI("http://localhost:4566");
    RunEmrJob step = emrStep("app1", Map.of(ODIN_APP_ID, "2963"));

    step.prepare("someOwner", null);

    assertFalse(step.getTags().containsKey(ODIN_APP_ID));
    assertEquals(0, loadCount.get());
  }

  @Test
  void concurrentStepsShareOneLookup() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    List<RunEmrJob> steps = new ArrayList<>();
    for (int i = 0; i < 8; i++)
      steps.add(new BlockingEmrJob(Map.of(ODIN_APP_ID, "2963"), false, release).withApplicationId("app1"));

    prepareConcurrently(steps, release);

    assertEquals(1, loadCount.get());
    steps.forEach(step -> assertEquals(Map.of(ODIN_APP_ID, "2963"), step.getTags()));
  }

  @Test
  void concurrentStepsShareFailedLookupWhichIsRetriedAfterwards() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    List<RunEmrJob> steps = new ArrayList<>();
    for (int i = 0; i < 8; i++)
      steps.add(new BlockingEmrJob(null, true, release).withApplicationId("app1"));

    prepareConcurrently(steps, release);

    assertEquals(1, loadCount.get());
    steps.forEach(step -> assertTrue(step.getTags().isEmpty()));

    RunEmrJob laterStep = emrStep("app1", Map.of(ODIN_APP_ID, "2963"));
    laterStep.prepare("someOwner", null);
    assertEquals(2, loadCount.get());
    assertEquals(Map.of(ODIN_APP_ID, "2963"), laterStep.getTags());
  }

  @Test
  void concurrentPreparationPropagatesWorkerFailures() {
    CountDownLatch release = new CountDownLatch(1);
    AssertionError failure = new AssertionError("Lookup failed unexpectedly");
    RunEmrJob step = new BlockingEmrJob(null, false, release) {
      @Override
      protected Map<String, String> loadApplicationTags(String applicationId) {
        super.loadApplicationTags(applicationId);
        throw failure;
      }
    }.withApplicationId("app1");

    ExecutionException exception = assertThrows(ExecutionException.class,
        () -> prepareConcurrently(List.of(step), release));

    assertEquals(failure, exception.getCause());
  }

  /**
   * Prepares all steps in parallel and releases the (blocked) lookup only after all threads are waiting,
   * i.e., one thread is waiting within the lookup and all others are waiting for its result.
   */
  private static void prepareConcurrently(List<RunEmrJob> steps, CountDownLatch release) throws Exception {
    List<FutureTask<Void>> preparations = steps.stream()
        .map(step -> new FutureTask<Void>(() -> {
          step.prepare("someOwner", null);
          return null;
        }))
        .toList();
    List<Thread> threads = preparations.stream().map(Thread::new).toList();
    try {
      threads.forEach(Thread::start);

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (threads.stream().anyMatch(thread -> thread.getState() != Thread.State.WAITING)) {
        for (FutureTask<Void> preparation : preparations)
          if (preparation.isDone())
            preparation.get(5, TimeUnit.SECONDS);
        assertTrue(System.nanoTime() < deadline, "Not all threads are waiting for the lookup");
        Thread.sleep(10);
      }

      release.countDown();
      for (FutureTask<Void> preparation : preparations)
        preparation.get(5, TimeUnit.SECONDS);
    }
    finally {
      release.countDown();
      preparations.forEach(preparation -> preparation.cancel(true));
      for (Thread thread : threads) {
        thread.join(5_000);
        assertFalse(thread.isAlive(), "Preparation thread did not terminate");
      }
    }
  }

  private static RunEmrJob emrStep(String applicationId, Map<String, String> applicationTags) {
    return new TestEmrJob(applicationTags, false).withApplicationId(applicationId);
  }

  private static class BlockingEmrJob extends TestEmrJob {
    private final CountDownLatch release;

    BlockingEmrJob(Map<String, String> applicationTags, boolean fail, CountDownLatch release) {
      super(applicationTags, fail);
      this.release = release;
    }

    @Override
    protected Map<String, String> loadApplicationTags(String applicationId) {
      try {
        release.await();
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return super.loadApplicationTags(applicationId);
    }
  }

  private static class TestEmrJob extends RunEmrJob {
    private final Map<String, String> applicationTags;
    private final boolean fail;

    TestEmrJob(Map<String, String> applicationTags, boolean fail) {
      this.applicationTags = applicationTags == null ? null : new HashMap<>(applicationTags);
      this.fail = fail;
    }

    @Override
    protected Map<String, String> loadApplicationTags(String applicationId) {
      loadCount.incrementAndGet();
      if (fail)
        throw ResourceNotFoundException.builder().message("Application not found").build();
      return applicationTags;
    }
  }
}
