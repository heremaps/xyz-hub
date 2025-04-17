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

package com.here.xyz.util.runtime;

import com.amazonaws.services.lambda.runtime.Context;
import com.here.xyz.util.ARN;
import com.here.xyz.util.service.aws.SimulatedContext;
import java.util.List;

public class LambdaFunctionRuntime extends FunctionRuntime {

  private Context context;
  private String streamId;

  public LambdaFunctionRuntime(Context context, String streamId) {
    if (context == null)
      throw new NullPointerException("Context is missing for LambdaConnectorRuntime.");
    this.context = context;
    this.streamId = streamId == null ? "no-stream-id" : streamId;
    FunctionRuntime.setInstance(this);
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
    if (context instanceof SimulatedContext)
      return ((SimulatedContext) context).getEnv(variableName);
    return System.getenv(variableName);
  }

  @Override
  public boolean isRunningLocally() {
    return context instanceof SimulatedContext;
  }

  @Override
  public String getSoftwareVersion() {
    if (context instanceof SimulatedContext)
      return null;
    String version = getVersionFromArn(context.getInvokedFunctionArn());
    String functionVersion = "$LATEST".equals(context.getFunctionVersion()) ? "0" : context.getFunctionVersion();
    return isValidVersion(version) ? version : ("0.0." + functionVersion);
  }

  private static String getVersionFromArn(String arn) {
    String resourceWithAlias = new ARN(arn).getResourceWithoutType();
    if (!resourceWithAlias.contains(":"))
      return null;
    String[] aliasParts = resourceWithAlias.split("-");
    if (aliasParts.length < 3)
      return null;
    List<String> versionParts = List.of(aliasParts[aliasParts.length - 3], aliasParts[aliasParts.length - 2], aliasParts[aliasParts.length - 1]);
    return String.join(".", versionParts);
  }

  private boolean isValidVersion(String version) {
    try {
      if (version == null)
        return false;
      if (version.contains("SNAPSHOT"))
        return false;
      Integer.parseInt(version.substring(0, 1));
      return version.contains(".");
    }
    catch (NumberFormatException e) {
      return false;
    }
  }

  @Override
  public String getStreamId() {
    return streamId;
  }

  public String getInvokedFunctionArn() {
    return context.getInvokedFunctionArn();
  }
}