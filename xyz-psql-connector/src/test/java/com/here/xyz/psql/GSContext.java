/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

package com.here.xyz.psql;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.here.xyz.connectors.SimulatedContext;
import java.util.HashMap;
import java.util.Map;

import com.here.xyz.psql.config.DatabaseSettings;
import com.here.xyz.psql.config.PSQLConfig;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GSContext extends SimulatedContext implements LambdaLogger {

  private static final Logger logger = LogManager.getLogger();
  private static final Level LOG_LEVEL = Level.INFO;

  @SuppressWarnings("serial")
  private static final Map<String, String> vars_local = new HashMap<String, String>() {{
    put(DatabaseSettings.PSQL_HOST, "localhost");
    put(DatabaseSettings.PSQL_USER, "postgres");
    put(DatabaseSettings.PSQL_PASSWORD, "password");
    put(PSQLConfig.ECPS_PHRASE, "testing");
  }};

  public GSContext(String functionName, Map<String, String> environmentVariables) {
    super(functionName, environmentVariables);
  }

  public static GSContext newLocal() {
    return new GSContext("xyz-psql-local", vars_local);
  }

  @Override
  public void log(String string) {
    logger.log(LOG_LEVEL, string);
  }
}
