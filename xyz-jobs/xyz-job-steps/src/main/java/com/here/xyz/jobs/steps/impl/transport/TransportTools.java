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

package com.here.xyz.jobs.steps.impl.transport;

import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.util.db.SQLQuery;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//TODO: Move commonly used parts into the common parent class instead (once refactoring of import step was done)
@Deprecated
public class TransportTools {
  private static final Logger logger = LogManager.getLogger();

  private static final String JOB_DATA_PREFIX = "job_data_";

  public static String getTemporaryJobTableName(String stepId) {
    return JOB_DATA_PREFIX + stepId;
  }

  protected static SQLQuery buildTemporaryJobTableDropStatement(String schema, String tableName) {
    return new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table};")
        .withVariable("table", tableName)
        .withVariable("schema", schema);
  }

  //TODO: Introduce proper logging methods at parent Step class level

  @Deprecated
  protected static void infoLog(Phase phase, Step step, String... messages) {
    logger.info("{} [{}@{}] ON '{}' {}", step.getClass().getSimpleName(), step.getGlobalStepId(), phase.name(), getSpaceId(step),
        messages.length > 0 ? messages : "");
  }

  @Deprecated
  private static String getSpaceId(Step step) {
    return step instanceof SpaceBasedStep<?> spaceStep ? spaceStep.getSpaceId() : null;
  }

  @Deprecated
  protected static void errorLog(Phase phase, Step step, Exception e, String... message) {
    logger.error("{} [{}@{}] ON '{}' {}", step.getClass().getSimpleName(), step.getGlobalStepId(), phase.name(), getSpaceId(step),
        message, e);
  }

  @Deprecated
  protected enum Phase {
    @Deprecated
    GRAPH_TRANSFORMER,
    @Deprecated
    JOB_EXECUTOR,
    @Deprecated
    STEP_EXECUTE,
    @Deprecated
    STEP_RESUME,
    @Deprecated
    STEP_CANCEL,
    @Deprecated
    STEP_ON_STATE_CHECK,
    @Deprecated
    STEP_ON_ASYNC_FAILURE,
    @Deprecated
    STEP_ON_ASYNC_UPDATE,
    @Deprecated
    STEP_ON_ASYNC_SUCCESS,
    @Deprecated
    JOB_DELETE,
    @Deprecated
    JOB_VALIDATE
  }
}
