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

package com.here.xyz.connectors.runtime;

import com.amazonaws.services.lambda.runtime.Context;

public class LambdaConnectorRuntime extends ConnectorRuntime {

  private Context context;
  private String streamId;

  public LambdaConnectorRuntime(Context context, String streamId) {
    this.context = context;
    this.streamId = streamId == null ? "no-stream-id" : streamId;
    ConnectorRuntime.setInstance(this);
  }

  @Override
  public int getRemainingTime() {
    return context.getRemainingTimeInMillis();
  }

  @Override
  public String getApplicationName() {
    return context.getFunctionName();
  }

  @Override
  public String getEnvironmentVariable(String variableName) {
    if (context instanceof SimulatedContext) {
      return ((SimulatedContext) context).getEnv(variableName);
    }
    return System.getenv(variableName);
  }

  @Override
  public boolean isRunningLocally() {
    return context instanceof SimulatedContext;
  }

  @Override
  public String getStreamId() {
    return streamId;
  }

  public String getInvokedFunctionArn() {
    return context.getInvokedFunctionArn();
  }
}