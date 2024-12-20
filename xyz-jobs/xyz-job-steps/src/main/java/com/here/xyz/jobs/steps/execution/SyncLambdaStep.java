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

package com.here.xyz.jobs.steps.execution;

import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.ExecutionMode.SYNC;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.xyz.jobs.steps.impl.CompressGeoObjects;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import java.util.List;

/**
 * A simplified synchronous version of the {@link LambdaBasedStep}.
 */
@JsonSubTypes({
        @JsonSubTypes.Type(value = CompressGeoObjects.class)
})
public abstract class SyncLambdaStep extends LambdaBasedStep<SyncLambdaStep> {

  @Override
  public AsyncExecutionState getExecutionState() throws UnknownStateException {
    return null;
  }

  @Override
  public ExecutionMode getExecutionMode() {
    return SYNC;
  }

  @Override
  public List<Load> getNeededResources() {
    return List.of();
  }

  @Override
  public int getTimeoutSeconds() {
    return 900 - 10; //Maximum lambda timeout with some buffer
  }

  @Override
  public void resume() throws Exception {
    execute();
  }

  @Override
  public void cancel() throws Exception {
    //Nothing to do here
  }

  @Override
  public boolean validate() throws ValidationException {
    return false;
  }
}
