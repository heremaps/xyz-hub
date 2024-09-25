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

package com.here.xyz.jobs.steps.impl.transport;

import com.here.xyz.jobs.steps.Step;
import com.here.xyz.util.db.SQLQuery;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TransportTools {
  private static final Logger logger = LogManager.getLogger();

  private static final String JOB_DATA_PREFIX = "job_data_";
  private static final String TRIGGER_TABLE_SUFFIX = "_trigger_tbl";

  protected static String getTemporaryJobTableName(Step step) {
    return JOB_DATA_PREFIX + step.getId();
  }

  protected static String getTemporaryTriggerTableName(Step step) {
    return getTemporaryJobTableName(step)+TRIGGER_TABLE_SUFFIX;
  }

  protected static SQLQuery buildDropTemporaryTableQuery(String schema, String tableName) {
    return new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table};")
            .withVariable("table", tableName)
            .withVariable("schema", schema);
  }

  protected static void infoLog(String phase, String spaceId, String getGlobalStepId, String... messages) {
    logger.info("[{}@{}] ON '{}' {}", getGlobalStepId, phase, spaceId, messages.length > 0 ? messages : "");
  }

  protected static void errorLog(String phase, String spaceId, String getGlobalStepId, String message, Exception e) {
    logger.error("[{}@{}] ON '{}' {}", getGlobalStepId, phase, spaceId, message, e);
  }

  protected enum Phase {
    GRAPH_TRANSFORMER,
    JOB_EXECUTOR,
    STEP_EXECUTE,
    STEP_RESUME,
    STEP_CANCEL,
    STEP_ON_STATE_CHECK,
    STEP_ON_ASYNC_FAILURE,
    STEP_ON_ASYNC_SUCCESS,
    JOB_DELETE,
    JOB_VALIDATE
  }
}
