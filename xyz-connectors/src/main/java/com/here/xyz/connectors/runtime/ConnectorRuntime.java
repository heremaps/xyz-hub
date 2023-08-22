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

public abstract class ConnectorRuntime {

  private static ConnectorRuntime instance;

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

  public abstract String getStreamId();

  public static ConnectorRuntime getInstance() {
    return instance;
  }

  static void setInstance(ConnectorRuntime instance) {
    ConnectorRuntime.instance = instance;
  }

}
