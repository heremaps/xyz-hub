/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.jobs.steps.resources;

import io.vertx.core.Future;

/**
 * Depicts the number of I/O bytes that have been used by the step.
 */
public class IOResource extends ExecutionResource {
  private static final IOResource INSTANCE = new IOResource();

  private IOResource() {}

  public static IOResource getInstance() {
    return INSTANCE;
  }

  @Override
  public Future<Double> getUtilizedUnits() {
    return Future.succeededFuture(0d);
  }

  @Override
  protected double getMaxUnits() {
    return Long.MAX_VALUE;
  }

  @Override
  protected double getMaxVirtualUnits() {
    return Long.MAX_VALUE;
  }

  @Override
  protected String getId() {
    return "io-execution-resource";
  }
}
