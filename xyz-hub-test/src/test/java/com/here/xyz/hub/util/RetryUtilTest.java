/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.xyz.hub.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.here.xyz.hub.Core;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class RetryUtilTest {

  private static volatile int callCount = 0;

  @BeforeClass
  public static void setupClass() {
    //Mock necessary configuration values
    Core.vertx = Vertx.vertx();
  }

  @Before
  public void setup() {
    callCount = 0;
  }

  @Test
  public void executeTaskPositive() throws InterruptedException {
    Future<String> f = RetryUtil.<String>executeWithRetry(task -> {
      Promise<String> p = Promise.promise();
      String result = "The result";
      p.complete(result);
      return p.future();
    }, t -> 1, 2, 1_000).future();

    //Wait for the asynchronous task to be completed
    Thread.sleep(100);

    assertTrue(f.isComplete());
    f.onSuccess(r -> assertEquals("The result", r));
  }

  @Test
  public void executeTaskWithRetryPositive() throws InterruptedException {
    Future<String> f = RetryUtil.<String>executeWithRetry(task -> {
      callCount++;
      if (callCount == 1) throw new RuntimeException("Some Exception which may be retried.");
      Promise<String> p = Promise.promise();
      String result = "The result";
      p.complete(result);
      return p.future();
    }, t -> 1, 2, 1_000).future();

    //Wait for the asynchronous task to be completed
    Thread.sleep(100);

    assertTrue(f.isComplete());
    assertEquals(2, callCount);
    f.onSuccess(r -> assertEquals("The result", r));
  }

  @Test
  public void executeTaskWithRetryNegative() throws InterruptedException {
    Future<String> f = RetryUtil.<String>executeWithRetry(task -> {
      callCount++;
      throw new RuntimeException("Some Exception which may be retried.");
    }, t -> 1, 2, 1_000).future();

    //Wait for the asynchronous task to be completed
    Thread.sleep(100);

    assertTrue(f.isComplete());
    assertEquals(3, callCount);
    f.onFailure(t -> assertTrue(t instanceof RuntimeException));
  }

}
