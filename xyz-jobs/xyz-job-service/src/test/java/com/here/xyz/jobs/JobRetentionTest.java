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

package com.here.xyz.jobs;

import static com.here.xyz.jobs.Job.LONG_RETENTION;
import static com.here.xyz.jobs.Job.SHORT_RETENTION;
import static com.here.xyz.jobs.RuntimeInfo.State.FAILED;
import static com.here.xyz.jobs.RuntimeInfo.State.NOT_READY;
import static com.here.xyz.jobs.RuntimeInfo.State.PENDING;
import static com.here.xyz.jobs.RuntimeInfo.State.RESUMING;
import static com.here.xyz.jobs.RuntimeInfo.State.RUNNING;
import static com.here.xyz.jobs.RuntimeInfo.State.SUBMITTED;
import static com.here.xyz.jobs.RuntimeInfo.State.SUCCEEDED;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Public;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.config.InMemJobConfigClient;
import com.here.xyz.jobs.config.JobConfigClient;
import com.here.xyz.jobs.datasets.streams.Notifications;
import com.here.xyz.jobs.service.Config;
import com.here.xyz.util.service.Core;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.lang.reflect.Field;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Checks how the framework manages the keepUntil of a job (see {@link Job#updateKeepUntil()}).
 */
public class JobRetentionTest {
  //The time is taken by a cached clock, so allow some tolerance
  private static final long TOLERANCE = 5_000;
  private static Object previousConfigClient;

  @BeforeAll
  public static void setUp() throws Exception {
    if (Core.vertx == null)
      Core.vertx = Vertx.vertx();
    if (Config.instance == null)
      new Config();
    previousConfigClient = configClientField().get(null);
    configClientField().set(null, new InMemJobConfigClient());
  }

  @AfterAll
  public static void tearDown() throws Exception {
    configClientField().set(null, previousConfigClient);
  }

  @Test
  public void createWithoutKeepUntil() {
    Job job = new Job().create();

    assertEquals(job.getCreatedAt() + LONG_RETENTION, job.getKeepUntil());
    assertEquals(SHORT_RETENTION, job.getRetention());
  }

  @Test
  public void createKeepsTheKeepUntilOfTheCreator() {
    long keepUntil = now() + DAYS.toMillis(30);
    Job job = new Job().withKeepUntil(keepUntil).create();

    assertEquals(keepUntil, job.getKeepUntil());
    assertEquals(0, job.getRetention());
  }

  @Test
  public void createIgnoresARetentionOfTheCreator() throws Exception {
    Job withKeepUntil = XyzSerializable.deserialize("{\"keepUntil\": " + (now() + DAYS.toMillis(30)) + ", \"retention\": 123}",
        Job.class).create();
    Job withoutKeepUntil = XyzSerializable.deserialize("{\"retention\": 123}", Job.class).create();

    assertEquals(0, withKeepUntil.getRetention());
    assertEquals(SHORT_RETENTION, withoutKeepUntil.getRetention());
  }

  @Test
  public void notReadyIsNotExtended() {
    Job job = managedJob(NOT_READY, now() + 1_000);

    job.updateKeepUntil();

    assertAbout(now() + 1_000, job.getKeepUntil());
  }

  @ParameterizedTest
  @EnumSource(value = State.class, names = {"SUBMITTED", "PENDING", "RUNNING", "RESUMING", "CANCELLING"})
  public void activeStatesExtendTheKeepUntil(State state) {
    Job job = managedJob(state, now() + 1_000);

    job.updateKeepUntil();

    assertAbout(now() + LONG_RETENTION, job.getKeepUntil());
  }

  @Test
  public void activeStatesDoNotShortenTheKeepUntil() {
    long keepUntil = now() + DAYS.toMillis(30);
    Job job = managedJob(RUNNING, keepUntil);

    job.updateKeepUntil();

    assertEquals(keepUntil, job.getKeepUntil());
  }

  @Test
  public void succeededJobsAreKeptForTheirRetention() {
    Job job = managedJob(SUCCEEDED, now() + LONG_RETENTION);

    job.updateKeepUntil();

    assertAbout(now() + SHORT_RETENTION, job.getKeepUntil());
  }

  @ParameterizedTest
  @EnumSource(value = State.class, names = {"FAILED", "CANCELLED"})
  public void failedAndCancelledJobsAreKeptLonger(State state) {
    Job job = managedJob(state, now() + 1_000);

    job.updateKeepUntil();

    assertAbout(now() + LONG_RETENTION, job.getKeepUntil());
  }

  @Test
  public void retentionDefinedByACompiler() {
    long retention = DAYS.toMillis(30);
    Job succeeded = managedJob(SUCCEEDED, now()).withRetention(retention);
    Job failed = managedJob(FAILED, now()).withRetention(retention);

    succeeded.updateKeepUntil();
    failed.updateKeepUntil();

    assertAbout(now() + retention, succeeded.getKeepUntil());
    assertAbout(now() + retention, failed.getKeepUntil());
  }

  @ParameterizedTest
  @EnumSource(value = State.class, names = {"NOT_READY", "RUNNING", "SUCCEEDED", "FAILED"})
  public void keepUntilOfTheCreatorIsNotChanged(State state) {
    long keepUntil = now() + DAYS.toMillis(30);
    Job job = new Job().withKeepUntil(keepUntil).create().withStatus(new RuntimeStatus().withState(state));

    job.updateKeepUntil();

    assertEquals(keepUntil, job.getKeepUntil());
  }

  @Test
  public void pipelinesAreNotChanged() {
    long keepUntil = now() + DAYS.toMillis(3650);
    Job pipeline = managedJob(SUCCEEDED, keepUntil).withSource(new Notifications());

    pipeline.updateKeepUntil();

    assertEquals(keepUntil, pipeline.getKeepUntil());
  }

  @Test
  public void jobsStoredWithoutRetentionAreNotChanged() {
    //A job that was stored before the framework managed keepUntil
    long keepUntil = now() + DAYS.toMillis(14);
    Job job = XyzSerializable.fromMap(Map.of("id", "someJob", "keepUntil", keepUntil / 1000, "status", Map.of("state", "SUCCEEDED")),
        Job.class);

    job.updateKeepUntil();

    assertEquals(0, job.getRetention());
    assertEquals(keepUntil / 1000 * 1000, job.getKeepUntil());
  }

  @Test
  public void retentionIsStoredButNotPublic() {
    Job job = new Job().create();

    assertEquals(SHORT_RETENTION, ((Number) job.toMap(Static.class).get("retention")).longValue());
    assertFalse(job.toMap(Public.class).containsKey("retention"));
    assertEquals(SHORT_RETENTION, XyzSerializable.fromMap(job.toMap(Static.class), Job.class).getRetention());
  }

  @Test
  public void storedStatusUpdatesTheStoredKeepUntil() throws Exception {
    Job job = new Job().create();
    await(job.store());
    assertEquals(job.getCreatedAt() + LONG_RETENTION, loadKeepUntil(job));

    for (State state : new State[]{SUBMITTED, PENDING, RUNNING}) {
      job.getStatus().setState(state);
      await(job.storeStatus(null));
    }
    assertAbout(now() + LONG_RETENTION, loadKeepUntil(job));

    job.getStatus().setState(SUCCEEDED);
    await(job.storeStatus(RUNNING));
    assertAbout(now() + SHORT_RETENTION, loadKeepUntil(job));
  }

  @Test
  public void resumingAFailedJobExtendsItAgain() throws Exception {
    Job job = managedJob(FAILED, now() + 1_000);
    await(job.store());
    assertAbout(now() + LONG_RETENTION, loadKeepUntil(job));

    job.getStatus().setState(RESUMING);
    job.setKeepUntil(now() + 1_000);
    await(job.storeStatus(FAILED));

    assertAbout(now() + LONG_RETENTION, loadKeepUntil(job));
  }

  /**
   * A job whose keepUntil is managed by the framework, in the specified state.
   */
  private static Job managedJob(State state, long keepUntil) {
    return new Job()
        .create()
        .withStatus(new RuntimeStatus().withState(state))
        .withKeepUntil(keepUntil);
  }

  private static long loadKeepUntil(Job job) throws Exception {
    return await(JobConfigClient.getInstance().loadJob(job.getId())).getKeepUntil();
  }

  private static long now() {
    return Core.currentTimeMillis();
  }

  private static void assertAbout(long expected, long actual) {
    assertTrue(Math.abs(expected - actual) <= TOLERANCE,
        "Expected " + expected + " (+/- " + TOLERANCE + " ms), but was " + actual + " (difference: " + (actual - expected) + " ms)");
  }

  private static Field configClientField() throws Exception {
    Field field = JobConfigClient.Provider.class.getDeclaredField("client");
    field.setAccessible(true);
    return field;
  }

  private static <T> T await(Future<T> future) throws Exception {
    return future.toCompletionStage().toCompletableFuture().get(30, SECONDS);
  }
}
