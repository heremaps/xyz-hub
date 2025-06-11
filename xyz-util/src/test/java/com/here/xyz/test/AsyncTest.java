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

package com.here.xyz.test;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.here.xyz.util.Async;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

public class AsyncTest {
  private static final Async ASYNC = new Async(5, AsyncTest.class);

  @Test
  public void runParallel() {
    long waitTime = 100;
    CompositeFuture f = Future.all(
        List.of(
          ASYNC.run(() -> {
            Thread.sleep(waitTime);
            return null;
          }),
          ASYNC.run(() -> {
            Thread.sleep(waitTime);
            return null;
          })
        )
    ).onFailure(e -> fail(e));

    assertTimeout(Duration.of(waitTime + waitTime / 2, MILLIS), () -> {
      while (!f.isComplete())
        Thread.sleep(10);
    });
  }

  @Test
  public void runWithTimeout() {
    long waitTime = 100;
    long startTime = System.currentTimeMillis();
    Future f = ASYNC.run(() -> {
      //Sleep for some time that is longer than the timeout for sure
      Thread.sleep(waitTime * 2);

      //Assure the task was actually interrupted and never reaches this point
      throw new Exception("The thread should never reach this point. It was not interrupted by a timeout.");
    }, waitTime, MILLISECONDS);

    //Assure the call to ASYNC#run() was not blocking
    long timeAfterStartingTask = System.currentTimeMillis();
    assertTrue(timeAfterStartingTask - startTime < waitTime / 2, "The main thread was not immediately continuing after "
        + "starting the task. It seems the call to Async#run() was blocking.");

    //Assure the timeout was fired at the right time
    long timoutGraceTime = waitTime + waitTime / 2;
    assertTimeout(Duration.of(timoutGraceTime, MILLIS), () -> {
      while (!f.isComplete())
        Thread.sleep(10);

      //Assure the future was set to fail in the correct way
      assertTrue(f.failed(), "The resulting future was not set to failed.");
      assertEquals(TimeoutException.class, f.cause().getClass(), "The resulting cause of the future should be a "
          + TimeoutException.class + " but is: " + f.cause());
    }, "The timeout was not firing after " + timoutGraceTime + "ms");
  }
}
