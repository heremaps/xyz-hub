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

import com.here.xyz.jobs.steps.S3DataFile;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.util.db.SQLQuery;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportTools {
  private static final Logger logger = LogManager.getLogger();

  private static final String JOB_DATA_PREFIX = "job_data_";
  private static final String TRIGGER_TABLE_SUFFIX = "_trigger_tbl";

  protected static String getSpaceId(Step step) {
    if(step instanceof SpaceBasedStep<?> spaceStep)
      return spaceStep.getSpaceId();
    return null;
  }

  protected static String getTemporaryJobTableName(Step step) {
    return JOB_DATA_PREFIX + step.getId();
  }

  protected static String getTemporaryTriggerTableName(Step step) {
    return getTemporaryJobTableName(step) + TRIGGER_TABLE_SUFFIX;
  }

  protected static SQLQuery buildDropTemporaryTableQuery(String schema, String tableName) {
    return new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table};")
            .withVariable("table", tableName)
            .withVariable("schema", schema);
  }

  protected static SQLQuery buildTemporaryJobTableForImportQuery(String schema, Step step) {
    return new SQLQuery("""
        CREATE TABLE IF NOT EXISTS ${schema}.${table}
               (
                    s3_bucket text NOT NULL,
                    s3_path text NOT NULL,
                    s3_region text NOT NULL,
                    state text NOT NULL, --jobtype
                    execution_count int DEFAULT 0, --amount of retries
                    data jsonb COMPRESSION lz4, --statistic data
                    i SERIAL,
                    CONSTRAINT ${primaryKey} PRIMARY KEY (s3_path)
               );
        """)
            .withVariable("table", getTemporaryJobTableName(step))
            .withVariable("schema", schema)
            .withVariable("primaryKey", getTemporaryJobTableName(step) + "_primKey");
  }

  protected static List<SQLQuery> buildInitialInsertsForTemporaryJobTable(String schema, List<S3DataFile> inputs,
                                                                          String bucketRegion, Step step) {
    List<SQLQuery> queryList = new ArrayList<>();
    for (S3DataFile input : inputs) {
      if (input instanceof UploadUrl || input instanceof DownloadUrl) {
        JsonObject data = new JsonObject()
                .put("compressed", input.isCompressed())
                .put("filesize", input.getByteSize());

        queryList.add(
                new SQLQuery("""                
                    INSERT INTO  ${schema}.${table} (s3_bucket, s3_path, s3_region, state, data)
                        VALUES (#{bucketName}, #{s3Key}, #{bucketRegion}, #{state}, #{data}::jsonb)
                        ON CONFLICT (s3_path) DO NOTHING;
                """) //TODO: Why would we ever have a conflict here? Why to fill the table again on resume()?
                        .withVariable("schema", schema)
                        .withVariable("table", getTemporaryJobTableName(step))
                        .withNamedParameter("s3Key", input.getS3Key())
                        .withNamedParameter("bucketName", input.getS3Bucket())
                        .withNamedParameter("bucketRegion", bucketRegion)
                        .withNamedParameter("state", "SUBMITTED")
                        .withNamedParameter("data", data.toString())
        );
      }
    }
    //Add final entry
    queryList.add(
            new SQLQuery("""                
                INSERT INTO  ${schema}.${table} (s3_bucket, s3_path, s3_region, state, data)
                    VALUES (#{bucketName}, #{s3Key}, #{bucketRegion}, #{state}, #{data}::jsonb)
                    ON CONFLICT (s3_path) DO NOTHING;
            """) //TODO: Why would we ever have a conflict here? Why to fill the table again on resume()?
                    .withVariable("schema", schema)
                    .withVariable("table", getTemporaryJobTableName(step))
                    .withNamedParameter("s3Key", "SUCCESS_MARKER")
                    .withNamedParameter("bucketName", "SUCCESS_MARKER")
                    .withNamedParameter("state", "SUCCESS_MARKER")
                    .withNamedParameter("bucketRegion", "SUCCESS_MARKER")
                    .withNamedParameter("data", "{}"));
    return queryList;
  }

  protected static SQLQuery buildResetSuccessMarkerAndRunningOnes(String schema, Step step) {
    return new SQLQuery("""
        UPDATE ${schema}.${table}
          SET state =
            CASE
              WHEN state = 'SUCCESS_MARKER_RUNNING' THEN 'SUCCESS_MARKER'
              WHEN state = 'RUNNING' THEN 'SUBMITTED'
            END
          WHERE state IN ('SUCCESS_MARKER_RUNNING', 'RUNNING');
        """)
            .withVariable("schema", schema)
            .withVariable("table", getTemporaryJobTableName(step));
  }

  protected static SQLQuery buildProgressQuery(String schema, Step step) {
    return new SQLQuery("""
          SELECT 
          	COALESCE(processed_bytes/overall_bytes, 0) as progress,
          	COALESCE(processed_bytes,0) as processed_bytes,
            	COALESCE(finished_cnt,0) as finished_cnt,
            	COALESCE(failed_cnt,0) as failed_cnt
            FROM(
            	SELECT
                  (SELECT sum((data->'filesize')::bigint ) FROM ${schema}.${table}) as overall_bytes,
                  sum((data->'filesize')::bigint ) as processed_bytes,
                  sum((state = 'FINISHED')::int) as finished_cnt,
                  sum((state = 'FAILED')::int) as failed_cnt
                FROM ${schema}.${table}
              	 WHERE POSITION('SUCCESS_MARKER' in state) = 0
            	   AND state IN ('FINISHED','FAILED')
            )A          
        """)
            .withVariable("schema", schema)
            .withVariable("table", getTemporaryJobTableName(step));
  }

  protected static Map<String, Object> createQueryContext(String stepId, String schema, String table,
                                                          boolean historyEnabled, String superTable){

    final Map<String, Object> queryContext = new HashMap<>(Map.of(
            "stepId", stepId,
            "schema", schema,
            "table", table,
            "context", superTable != null ? "'DEFAULT'" : "NULL",
            "historyEnabled", historyEnabled
    ));

    if (superTable != null)
      queryContext.put("extendedTable", superTable);

    return queryContext;
  }

  protected static void infoLog(Phase phase, Step step, String... messages) {
    logger.info("{} [{}@{}] ON '{}' {}", step.getClass().getSimpleName(), step.getGlobalStepId(), phase.name(), getSpaceId(step), messages.length > 0 ? messages : "");
  }

  protected static void errorLog(Phase phase, Step step, Exception e,  String... message) {
    logger.error("{} [{}@{}] ON '{}' {}", step.getClass().getSimpleName(), step.getGlobalStepId(), phase.name(), getSpaceId(step), message, e);
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
