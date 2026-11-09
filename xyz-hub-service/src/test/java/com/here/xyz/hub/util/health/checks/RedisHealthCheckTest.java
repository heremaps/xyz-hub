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

package com.here.xyz.hub.util.health.checks;

import static com.here.xyz.hub.util.health.schema.Status.Result.CRITICAL;
import static com.here.xyz.hub.util.health.schema.Status.Result.ERROR;
import static com.here.xyz.hub.util.health.schema.Status.Result.OK;
import static com.here.xyz.hub.util.health.schema.Status.Result.TIMEOUT;
import static com.here.xyz.hub.util.health.schema.Status.Result.UNKNOWN;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.here.xyz.hub.cache.RedisCacheClient;
import com.here.xyz.hub.util.health.schema.Status;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

class RedisHealthCheckTest {

  private RedisCacheClient client;
  private RedisHealthCheck check;

  @BeforeEach
  void setUp() {
    client = mock(RedisCacheClient.class);
    check = new RedisHealthCheck("redis://localhost:6379");
    check.setTimeout(2_000);
    when(client.setAsync(anyString(), any(byte[].class), anyLong())).thenReturn(Future.succeededFuture());
    when(client.get(anyString())).thenReturn(Future.succeededFuture("someValue".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void readWaitsForWriteAcknowledgement() throws Exception {
    Promise<Void> acknowledgement = Promise.promise();
    CountDownLatch writeStarted = new CountDownLatch(1);
    when(client.setAsync(anyString(), any(byte[].class), anyLong())).thenAnswer(invocation -> {
      writeStarted.countDown();
      return acknowledgement.future();
    });
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      java.util.concurrent.Future<Status> result = executor.submit(this::execute);
      assertTrue(writeStarted.await(5, SECONDS));
      verify(client, never()).get(anyString());

      acknowledgement.complete();

      assertEquals(OK, result.get(5, SECONDS).getResult());
      ArgumentCaptor<byte[]> value = ArgumentCaptor.forClass(byte[].class);
      verify(client).setAsync(eq("__SAMPLE_HEALTH_CHECK_KEY"), value.capture(), eq(10L));
      assertArrayEquals("someValue".getBytes(StandardCharsets.UTF_8), value.getValue());
      verify(client).get("__SAMPLE_HEALTH_CHECK_KEY");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void alreadyCompletedReadDoesNotLoseItsCompletion() {
    check.setTimeout(5_000);

    assertEquals(OK, assertTimeoutPreemptively(Duration.ofSeconds(2), this::execute).getResult());
    assertNull(check.getResponse());
  }

  @Test
  void failedWriteIsReportedWithoutReading() {
    when(client.setAsync(anyString(), any(byte[].class), anyLong())).thenReturn(Future.failedFuture("Write rejected"));

    assertEquals(ERROR, execute().getResult());
    assertTrue(check.getResponse().getMessage().contains("Write rejected"));
    verify(client, never()).get(anyString());
  }

  @Test
  void failedReadIsReported() {
    when(client.get(anyString())).thenReturn(Future.failedFuture("Read failed"));

    assertEquals(ERROR, execute().getResult());
    assertTrue(check.getResponse().getMessage().contains("Read failed"));
  }

  @Test
  void missingSampleIsCritical() {
    when(client.get(anyString())).thenReturn(Future.succeededFuture(null));

    assertEquals(CRITICAL, execute().getResult());
    assertTrue(check.getResponse().getMessage().contains("sample health check record"));
  }

  @Test
  void wrongSampleIsCritical() {
    when(client.get(anyString())).thenReturn(Future.succeededFuture(new byte[]{1}));

    assertEquals(CRITICAL, execute().getResult());
  }

  @Test
  void pendingWriteTimesOutWithoutReading() {
    when(client.setAsync(anyString(), any(byte[].class), anyLong())).thenReturn(Promise.<Void>promise().future());
    check.setTimeout(20);

    assertEquals(TIMEOUT, execute().getResult());
    verify(client, never()).get(anyString());
  }

  @Test
  void pendingReadTimesOut() {
    when(client.get(anyString())).thenReturn(Promise.<byte[]>promise().future());
    check.setTimeout(20);

    assertEquals(TIMEOUT, execute().getResult());
  }

  @Test
  void interruptionIsPreserved() {
    when(client.setAsync(anyString(), any(byte[].class), anyLong())).thenReturn(Promise.<Void>promise().future());
    Thread.currentThread().interrupt();
    try {
      assertEquals(UNKNOWN, execute().getResult());
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void missingUriIsUnknownWithoutAccessingRedis() {
    check = new RedisHealthCheck(null);

    assertEquals(UNKNOWN, execute().getResult());
    verify(client, never()).setAsync(anyString(), any(byte[].class), anyLong());
  }

  private Status execute() {
    try (MockedStatic<RedisCacheClient> clients = mockStatic(RedisCacheClient.class)) {
      clients.when(RedisCacheClient::getInstance).thenReturn(client);
      return check.execute();
    }
  }
}
