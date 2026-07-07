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

import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.db.SQLQuery;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext;

public class ExportQueryBuilder extends DatabaseStepQueryBuilder{

  public ExportQueryBuilder(Space space, SpaceContext context,
                               String stepId, String schema, String rootTable, String superRootTable) {
    super(space, context, stepId, schema, rootTable, superRootTable);
  }

  public SQLQuery buildIRangeQuery(String table) {
    return new SQLQuery("SELECT min(i) AS min_i, max(i) AS max_i FROM ${schema}.${table}")
            .withVariable("schema",schema)
            .withVariable("table", table);
  }

  public SQLQuery buildExportToS3PluginQuery(int taskId, DownloadUrl downloadUrl,
          String bucketRegion, String serializedStep, String lambda_function_arn, String lambda_region,
          String contentQuery, String failureCallback) {
    return new SQLQuery(
            "SELECT export_to_s3_perform(#{taskId},  #{s3_bucket}, #{s3_path}, #{s3_region}, #{step_payload}::JSON->'step', " +
                    "#{lambda_function_arn}, #{lambda_region}, #{contentQuery}, '${{failureCallback}}');")
            .withContext(getQueryContext())
            .withAsyncProcedure(false)
            .withNamedParameter("taskId", taskId)
            .withNamedParameter("s3_bucket", downloadUrl.getS3Bucket())
            .withNamedParameter("s3_path", downloadUrl.getS3Key())
            .withNamedParameter("s3_region", bucketRegion)
            .withNamedParameter("step_payload", serializedStep)
            .withNamedParameter("lambda_function_arn", lambda_function_arn)
            .withNamedParameter("lambda_region", lambda_region)
            .withNamedParameter("contentQuery", contentQuery)
            .withQueryFragment("failureCallback",  failureCallback);
  }
}
