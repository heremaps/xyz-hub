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

import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.jobs.steps.impl.transport.tasks.inputs.ImportInput;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.db.SQLQuery;

import java.util.List;

import static com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace.Format;

public class ImportQueryBuilder extends DatabaseStepQueryBuilder {
  private static final String TMP_TABLE_SUFFIX = "_tmp_tbl_";
  protected final double featureWriterBatchSizeInMb;

  public ImportQueryBuilder(Space space, ContextAwareEvent.SpaceContext context, String stepId,
                               String schema, String rootTable, String superRootTable, double featureWriterBatchSizeInMb) {
    super(space, context, stepId, schema, rootTable, superRootTable);
    this.featureWriterBatchSizeInMb = featureWriterBatchSizeInMb;
  }

  public SQLQuery buildNextVersionQuery(){
    return withRetryPolicy(new SQLQuery("SELECT nextval('${schema}.${sequence}')")
            .withVariable("schema", schema)
            .withVariable("sequence", rootTable + "_version_seq"));
  }

  public SQLQuery buildImportTaskQuery(Format format, Integer taskId, ImportInput taskInput, String serializedImportStep,
                                       String lambdaArn, String ownLambdaRegion,
                                       boolean useFeatureWriter, String failureCallback){

    String targetTable = useFeatureWriter ?
            schema+".\"" + getTemporaryDataTableName(taskId) + "\""
            : schema+".\"" + rootTable + "\"";

    return withRetryPolicy(new SQLQuery(
            "SELECT perform_import_from_s3_task(#{taskId}, #{schema}, to_regclass(#{targetTable}), #{format}, " +
                    "#{s3Bucket}, #{s3Key}, #{s3Region}, #{filesize}, #{stepPayload}::JSON->'step', " +
                    "#{lambdaFunctionArn}, #{lambdaRegion}, '${{failureCallback}}')")
            .withContext(getQueryContext())
            .withAsync(true)
            .withNamedParameter("taskId", taskId)
            .withNamedParameter("schema", schema)
            .withNamedParameter("targetTable", targetTable)
            .withNamedParameter("format", format.name())
            .withNamedParameter("s3Bucket", taskInput.s3Bucket())
            .withNamedParameter("s3Key", taskInput.s3Key())
            .withNamedParameter("s3Region", taskInput.s3Region())
            .withNamedParameter("filesize", taskInput.fileByteSize())
            .withNamedParameter("stepPayload", serializedImportStep)
            .withNamedParameter("lambdaFunctionArn", lambdaArn)
            .withNamedParameter("lambdaRegion", ownLambdaRegion)
            .withQueryFragment("failureCallback", failureCallback));
  }

  public SQLQuery buildCreateImportTriggerForEmptyLayers(String targetAuthor, long targetSpaceVersion, boolean retainMetadata){
    return withRetryPolicy(new SQLQuery("CREATE OR REPLACE TRIGGER insertTrigger BEFORE INSERT ON ${schema}.${table} "
            + "FOR EACH ROW EXECUTE PROCEDURE ${triggerFunction}('${{author}}', ${{spaceVersion}}, ${{retainMetadata}});")
            .withQueryFragment("spaceVersion", "" + targetSpaceVersion)
            .withQueryFragment("author", targetAuthor)
            .withQueryFragment("retainMetadata", "" + retainMetadata)
            .withVariable("triggerFunction", "tasked_import_from_s3_trigger_for_empty_layer")
            .withVariable("schema", schema)
            .withVariable("table", rootTable));
  }

  public SQLQuery buildTemporaryDataTableForImportQuery(int taskId){
    String tableFields ="  jsondata TEXT, i BIGSERIAL PRIMARY KEY";
    return withRetryPolicy(new SQLQuery("CREATE TABLE IF NOT EXISTS ${schema}.${table} (${{tableFields}} )")
            .withQueryFragment("tableFields", tableFields)
            .withVariable("schema", schema )
            .withVariable("table", getTemporaryDataTableName(taskId)));
  }

  public SQLQuery dropTemporaryDataTableForImportQuery(int taskId){
    return withRetryPolicy(new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table} ;")
            .withVariable("schema", schema )
            .withVariable("table", getTemporaryDataTableName(taskId)));
  }

  public SQLQuery buildTriggerCleanUpStatement(){
    //Delete trigger - if present
   return withRetryPolicy(new SQLQuery("DROP TRIGGER IF EXISTS insertTrigger ON ${schema}.${table};")
      .withVariable("schema", schema)
      .withVariable("table", rootTable));
  }

  public SQLQuery buildDropAllTemporaryTablesByTaskItemCount(int taskItemCount) {
    if (taskItemCount <= 0) {
      return SQLQuery.batchOf(List.of());
    }

    List<SQLQuery> dropQueries = new java.util.ArrayList<>();
    for (int taskId = 1; taskId <= taskItemCount; taskId++) {
      dropQueries.add(dropTemporaryDataTableForImportQuery(taskId));
    }
    return SQLQuery.batchOf(dropQueries);
  }

  public String getTemporaryDataTableName(int taskId) {
    return getTemporaryJobTableName() + TMP_TABLE_SUFFIX + taskId;
  }

  /**
   * Builds a task query which imports features from the temporary trigger table using i-range reads.
   */
  public SQLQuery buildImportFromTmpTableTaskQuery(Integer taskId, long rangeStart,
                                                   String author, long currentVersion, boolean isPartial, UpdateStrategy updateStrategy,
                                                   String serializedImportStep, String lambdaArn, String ownLambdaRegion,
                                                   String failureCallback) {

    return withRetryPolicy(new SQLQuery(
            "SELECT perform_import_from_tmp_table_task(#{taskId}, to_regclass(#{sourceTable}), #{rangeStart}, #{targetMb}, "
                    + "#{author}, #{currentVersion}, #{isPartial}, #{onExists}, #{onNotExists}, #{onVersionConflict}, "
                    + "#{onMergeConflict}, #{stepPayload}::JSON->'step', #{lambdaFunctionArn}, #{lambdaRegion}, '${{failureCallback}}')")
            .withContext(getQueryContext())
            .withAsync(true)
            .withNamedParameter("taskId", taskId)
            .withNamedParameter("sourceTable", getTemporaryDataTableName(taskId))
            .withNamedParameter("rangeStart", rangeStart)
            .withNamedParameter("targetMb", featureWriterBatchSizeInMb)
            .withNamedParameter("author", author)
            .withNamedParameter("currentVersion", currentVersion)
            .withNamedParameter("isPartial", isPartial)
            .withNamedParameter("onExists", updateStrategy.onExists() == null ? null : updateStrategy.onExists().name())
            .withNamedParameter("onNotExists", updateStrategy.onNotExists() == null ? null : updateStrategy.onNotExists().name())
            .withNamedParameter("onVersionConflict", updateStrategy.onVersionConflict() == null ? null : updateStrategy.onVersionConflict().name())
            .withNamedParameter("onMergeConflict", updateStrategy.onMergeConflict() == null ? null : updateStrategy.onMergeConflict().name())
            .withNamedParameter("stepPayload", serializedImportStep)
            .withNamedParameter("lambdaFunctionArn", lambdaArn)
            .withNamedParameter("lambdaRegion", ownLambdaRegion)
            .withQueryFragment("failureCallback", failureCallback));
  }
}
