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
package com.here.naksha.lib.psql;

import com.here.naksha.lib.core.util.json.JsonEnum;

/**
 * Special values to enable different debugging level.
 */
public class EPsqlLogLevel extends JsonEnum {

  /**
   * Disable logging.
   */
  public static final EPsqlLogLevel OFF = def(EPsqlLogLevel.class, 0).alias(EPsqlLogLevel.class, null);

  /**
   * Debug logging, without pg_hint, but everything else.
   */
  public static final EPsqlLogLevel DEBUG = def(EPsqlLogLevel.class, 5);

  /**
   * Verbose logging, most detailed.
   */
  public static final EPsqlLogLevel VERBOSE = def(EPsqlLogLevel.class, 6);

  @Override
  protected void init() {
    register(EPsqlLogLevel.class);
  }
}
