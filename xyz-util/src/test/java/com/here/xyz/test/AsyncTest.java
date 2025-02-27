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
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.fail;

import com.here.xyz.util.Async;
import com.here.xyz.util.service.Core;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

public class AsyncTest {
  private static final Async ASYNC = new Async(5, AsyncTest.class);

  static {
    Core.vertx = Vertx.vertx();
  }

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
}
