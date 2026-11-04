/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static com.here.xyz.hub.util.health.schema.Status.Result.CRITICAL;
import static com.here.xyz.hub.util.health.schema.Status.Result.ERROR;
import static com.here.xyz.hub.util.health.schema.Status.Result.OK;
import static com.here.xyz.hub.util.health.schema.Status.Result.TIMEOUT;
import static com.here.xyz.hub.util.health.schema.Status.Result.UNKNOWN;

import com.here.xyz.hub.cache.RedisCacheClient;
import com.here.xyz.hub.util.health.schema.Response;
import com.here.xyz.hub.util.health.schema.Status;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class RedisHealthCheck extends ExecutableCheck {

  private static final String HC_CACHE_KEY = "__SAMPLE_HEALTH_CHECK_KEY";
  private static final byte[] HC_CACHE_VALUE = "someValue".getBytes();
  private final String uri;
  private RedisCacheClient client;

  public RedisHealthCheck(String uri) {
    this.uri = uri;
    setName("Redis");
    setRole(Role.CACHE);
    setTarget(Target.REMOTE);
  }

  @Override
  public Status execute() {
    Status s = new Status();
    Response r = new Response();
    r.setNode(uri);
    if (uri == null) {
      setResponse(r.withMessage("No Redis URI given."));
      return s.withResult(UNKNOWN);
    }

    try {
      if (client == null) {
        client = (RedisCacheClient) RedisCacheClient.getInstance();
      }
    } catch (RuntimeException e) {
      setResponse(r.withMessage("Error when trying to create Redis client: " + e.getMessage()));
      return s.withResult(ERROR);
    }

    try {
      byte[] receivedValue = client.setAsync(HC_CACHE_KEY, HC_CACHE_VALUE, 10)
          .compose(ignored -> client.get(HC_CACHE_KEY))
          .toCompletionStage().toCompletableFuture().get(timeout, MILLISECONDS);
      if (Arrays.equals(HC_CACHE_VALUE, receivedValue)) {
        setResponse(null);
        return s.withResult(OK);
      }
      setResponse(r.withMessage("Wasn't able to retrieve the sample health check record back correctly."));
      return s.withResult(CRITICAL);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      setResponse(r.withMessage("Redis health check was interrupted."));
      return s.withResult(UNKNOWN);
    } catch (TimeoutException e) {
      setResponse(r.withMessage("Timed out while checking Redis."));
      return s.withResult(TIMEOUT);
    } catch (ExecutionException | RuntimeException e) {
      setResponse(r.withMessage("Error when checking Redis: " + e.getMessage()));
      return s.withResult(ERROR);
    }
  }
}
