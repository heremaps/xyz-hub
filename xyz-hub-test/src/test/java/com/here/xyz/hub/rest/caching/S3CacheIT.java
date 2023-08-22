/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

package com.here.xyz.hub.rest.caching;

import static org.junit.Assert.assertEquals;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.Service.Config;
import com.here.xyz.hub.cache.CacheClient;
import com.here.xyz.hub.cache.S3CacheClient;
import com.here.xyz.hub.rest.RestAssuredTest;
import io.vertx.core.Vertx;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class S3CacheIT extends RestAssuredTest {

  @BeforeClass
  public static void setupClass() throws IOException {
    Service.configuration = new Config();
    Service.configuration.XYZ_HUB_S3_BUCKET = "test-bucket";
    Service.configuration.LOCALSTACK_ENDPOINT = "http://localhost:4566";
    Service.vertx = Vertx.vertx();
  }

  @AfterClass
  public static void tearDownClass() throws IOException {
    Service.configuration = null;
  }

  @Test
  public void testSetAndGet() throws InterruptedException {
    CacheClient cacheClient = S3CacheClient.getInstance();
    final String key = "testKey";
    final String value = "testValue";

    cacheClient.set(key, value.getBytes(), 100);

    Thread.sleep(1000);

    AtomicReference<String> cachedValue = new AtomicReference<>();
    cacheClient.get(key)
        .onSuccess(v -> cachedValue.set(new String(v)))
        .onFailure(t -> cachedValue.set("Getting value from S3 cache failed: " + t.getMessage()));

    while (cachedValue.get() == null)
      Thread.sleep(10);

    assertEquals(value, cachedValue.get());
  }
}
