/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.jobs.steps.impl.transport.tools;

import com.here.xyz.XyzSerializable;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.db.SQLQuery;

import java.util.Set;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext;
import static com.here.xyz.jobs.steps.impl.transport.TaskedSpaceBasedStep.SpaceBasedTaskUpdate;

public class TaskedSpaceBasedQueryBuilder extends DatabaseStepQueryBuilder {

  public TaskedSpaceBasedQueryBuilder(Space space, SpaceContext context, String stepId,
                                      String schema, String rootTable, String superRootTable) {
    super(space, context, stepId, schema, rootTable, superRootTable);
  }

  public SQLQuery buildTaskTableStatement() {
    return new SQLQuery("""
            CREATE TABLE ${schema}.${table}
            (
            	task_id SERIAL,
            	task_input JSONB,
            	task_output JSONB,
            	started BOOLEAN DEFAULT false,
            	finalized BOOLEAN DEFAULT false,
            	unknown_query_state_occurrences INTEGER DEFAULT 0,
            	retry_attempts INTEGER DEFAULT 0,
              started_at TIMESTAMP DEFAULT NULL,
              updated_at TIMESTAMP DEFAULT NULL,
            	CONSTRAINT ${primaryKey} PRIMARY KEY (task_id)
            );
        """)
            .withVariable("table", getTemporaryJobTableName())
            .withVariable("schema", schema)
            .withVariable("primaryKey", getTemporaryJobTableName() + "_primKey");
  }

  public SQLQuery buildUpdateTaskItemOutputStatement(SpaceBasedTaskUpdate update) {
    return new SQLQuery("""
            UPDATE ${schema}.${table}
                  SET
                    updated_at = now(),
                    task_output = (
                      COALESCE(task_output, '{}'::JSONB) || #{taskUpdate}::JSONB
                  ) || jsonb_build_object(
                      'taskOutput',
                      COALESCE(task_output->'taskOutput', '{}'::JSONB)
                      || COALESCE((#{taskUpdate}::JSONB)->'taskOutput', '{}'::JSONB)
                  )
                  WHERE task_id = #{taskId};
            """)
            .withVariable("schema", schema)
            .withVariable("table", getTemporaryJobTableName())
            .withNamedParameter("taskId", update.taskId)
            .withNamedParameter("taskUpdate", XyzSerializable.serialize(update));
  }

  public SQLQuery buildResetTaskItemWhichAreNotFinalizedStatement() {
    return new SQLQuery("""
            UPDATE ${schema}.${table} t
                SET started = false,
                updated_at = now()
                WHERE started = true AND finalized = false;
        """)
            .withVariable("schema", schema)
            .withVariable("table", getTemporaryJobTableName(stepId));
  }

  public SQLQuery buildRetrieveTaskOutputsQuery() {
    return new SQLQuery("SELECT task_id, task_input, task_output->'taskOutput' as task_output FROM ${schema}.${tmpTable};")
            .withVariable("schema", schema)
            .withVariable("tmpTable", getTemporaryJobTableName());
  }

  public SQLQuery retrieveTaskStatisticsQuery() {
    return new SQLQuery("""
            SELECT COUNT(1) as total,
                SUM((started = true)::int) as started,
                SUM((finalized = true)::int) as finalized,
                ARRAY_AGG(task_id) FILTER (WHERE started = true AND finalized = false) as started_not_finalized_task_ids
                FROM ${schema}.${table};
        """)
            .withVariable("schema", schema)
            .withVariable("table", getTemporaryJobTableName(stepId));
  }

  public SQLQuery retrieveTaskItemAndStatisticsQuery() {
    return new SQLQuery("SELECT total, started, finalized, task_id, task_input from get_task_item_and_statistics();")
            .withContext(getQueryContext());
  }

  public SQLQuery buildRetrieveTaskItemAndStatisticsAfterUpdateQuery(SpaceBasedTaskUpdate update) {
    return new SQLQuery("""
        SELECT total, started, finalized, task_id, task_input
          FROM update_task_item_and_get_task_item_and_statistics(
            #{taskId},
            #{taskOutput}::JSONB,
            #{finalized}
        );
    """)
      .withNamedParameter("taskId", update.taskId)
      .withNamedParameter("taskOutput", XyzSerializable.serialize(update))
      .withNamedParameter("finalized", true)
      .withContext(getQueryContext());
  }

  public SQLQuery buildLoadOutputsQuery(int taskId) {
    return new SQLQuery("""
              SELECT task_output->'taskOutput' AS task_output
                FROM ${schema}.${tmpTable}
               WHERE task_id = #{taskId};
        """)
          .withVariable("schema", schema)
          .withVariable("tmpTable", getTemporaryJobTableName())
          .withNamedParameter("taskId", taskId);
  }

  public SQLQuery buildInsertTaskItemStatement(String serializedTaskItem){
    return new SQLQuery("""
            INSERT INTO  ${schema}.${table} AS t (task_input)
                VALUES (#{taskItem}::JSONB);
        """)
            .withVariable("schema", schema)
            .withVariable("table", getTemporaryJobTableName())
            .withNamedParameter("taskItem", serializedTaskItem)
            .withLoggingEnabled(false);
  }

  public SQLQuery buildTemporaryJobTableDropStatement() {
    return new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table};")
            .withVariable("table", getTemporaryJobTableName())
            .withVariable("schema", schema);
  }

  public SQLQuery buildIncrementUnknownQueryStateStatement(Set<Integer> unknownStateTaskIds, int maxUnknownTaskQueryChecks) {
    return new SQLQuery("""
            WITH updated AS (
                UPDATE ${schema}.${table}
                   SET unknown_query_state_occurrences = COALESCE(unknown_query_state_occurrences, 0) + 1,
                   updated_at = now()
                 WHERE task_id IN (
                     SELECT value::INT
                       FROM jsonb_array_elements_text(#{missingTaskIds}::JSONB)
                 )
                RETURNING task_id, unknown_query_state_occurrences
            )
            SELECT task_id
              FROM updated
             WHERE unknown_query_state_occurrences >= #{maxUnknownTaskQueryChecks};
        """)
            .withVariable("schema", schema)
            .withVariable("table", getTemporaryJobTableName())
            .withNamedParameter("missingTaskIds", XyzSerializable.serialize(unknownStateTaskIds))
            .withNamedParameter("maxUnknownTaskQueryChecks", maxUnknownTaskQueryChecks);
  }

  public SQLQuery buildResetTaskForRetryStatement(int taskId) {
    return new SQLQuery("""
                UPDATE ${schema}.${table}
                   SET unknown_query_state_occurrences = 0,
                       updated_at = now(),
                       retry_attempts = retry_attempts + 1
                 WHERE task_id = #{taskId}
                 RETURNING task_id, task_input, retry_attempts;
            """)
            .withVariable("schema", schema)
            .withVariable("table", getTemporaryJobTableName())
            .withNamedParameter("taskId", taskId);
  }

  private String getTemporaryJobTableName(String stepId) {
    return JOB_DATA_PREFIX + stepId;
  }
}
