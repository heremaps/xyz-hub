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

package com.here.xyz.jobs.steps.execution.fusion;

import com.here.xyz.jobs.steps.resources.ExecutionResource;
import com.here.xyz.jobs.steps.resources.Load;
import io.vertx.core.Future;
import java.util.List;

public class SimpleTestStepWithLoad<T extends SimpleTestStepWithLoad> extends SimpleTestStep<T> {

  public SimpleTestStepWithLoad(String paramA) {
    super(paramA, null, null);
  }

  @Override
  public List<Load> getNeededResources() {
    return List.of(new Load().withResource(TestResource.getInstance()).withEstimatedVirtualUnits(5));
  }

  public static class TestResource extends ExecutionResource {

    private static final TestResource INSTANCE = new TestResource();

    private TestResource() {}

    public static TestResource getInstance() {
      return INSTANCE;
    }

    @Override
    public Future<Double> getUtilizedUnits() {
      return null;
    }

    @Override
    protected double getMaxUnits() {
      return 20;
    }

    @Override
    protected double getMaxVirtualUnits() {
      return 20;
    }

    @Override
    protected String getId() {
      return TestResource.class.getSimpleName();
    }
  }

}
