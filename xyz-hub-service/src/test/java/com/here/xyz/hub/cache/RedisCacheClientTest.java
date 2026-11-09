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

package com.here.xyz.hub.cache;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RedisCacheClientTest {

  private Redis redis;
  private RedisCacheClient client;

  @BeforeEach
  void setUp() {
    redis = mock(Redis.class);
    client = mock(RedisCacheClient.class, CALLS_REAL_METHODS);
    doReturn(redis).when(client).getClient();
  }

  @Test
  void asynchronousSetWaitsForRedisAcknowledgement() {
    Promise<Response> acknowledgement = Promise.promise();
    when(redis.send(any(Request.class))).thenReturn(acknowledgement.future());

    Future<Void> write = client.setAsync("sample", new byte[]{1}, 10);
    assertFalse(write.isComplete());
    verify(redis).send(any(Request.class));

    acknowledgement.complete(mock(Response.class));
    assertTrue(write.succeeded());
  }

  @Test
  void asynchronousSetPropagatesWriteFailure() {
    RuntimeException failure = new RuntimeException("Write rejected");
    when(redis.send(any(Request.class))).thenReturn(Future.failedFuture(failure));

    Future<Void> write = client.setAsync("sample", new byte[]{1}, 10);

    assertTrue(write.failed());
    assertSame(failure, write.cause());
  }

  @Test
  void fireAndForgetSetDoesNotWaitForAcknowledgement() {
    Promise<Response> acknowledgement = Promise.promise();
    when(redis.send(any(Request.class))).thenReturn(acknowledgement.future());

    client.set("sample", new byte[]{1}, 10);

    assertFalse(acknowledgement.future().isComplete());
    verify(redis).send(any(Request.class));
    acknowledgement.complete(mock(Response.class));
  }
}
