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

package com.here.xyz.util.runtime;

public abstract class FunctionRuntime {
  private static FunctionRuntime instance;
  private static final ThreadLocal<FunctionRuntime> threadLocalInstanceHolder = new ThreadLocal<>();

  public abstract int getRemainingTime();
  public abstract String getApplicationName();

  public abstract String getEnvironmentVariable(String variableName);

  /**
   * @deprecated Please do not use this method.
   * It's kept only as workaround for some deprecated implementations.
   * It must be transparent to the system where it's running.
   * @return
   */
  @Deprecated
  public abstract boolean isRunningLocally();

  /**
   * Provides the software version of the connector.
   * A return value of <code>null</code> means "latest".
   * @return The software version of the running connector
   */
  public String getSoftwareVersion() {
    return null;
  }

  public abstract String getStreamId();

  public static FunctionRuntime getInstance() {
    if (instance != null)
      return instance;
    //NOTE: Running locally there can be multiple instances of a connector running at the same time. See #setInstance(ConnectorRuntime)
    return threadLocalInstanceHolder.get();
  }

  static void setInstance(FunctionRuntime instance) {
    //NOTE: Running locally there can be multiple instances of a connector running at the same time.
    if (instance.isRunningLocally())
      FunctionRuntime.threadLocalInstanceHolder.set(instance);
    else
      FunctionRuntime.instance = instance;
  }
}