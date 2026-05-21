package com.here.xyz.jobs.steps.impl.transport.tools;

import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.jobs.steps.impl.transport.tasks.inputs.ImportInput;
import com.here.xyz.util.db.SQLQuery;

import java.util.List;
import java.util.Map;

import static com.here.xyz.jobs.steps.impl.transport.TaskedSpaceBasedStep.getTemporaryJobTableName;
import static com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace.Format;

public class ImportQueryBuilder {
  private static final String TMP_TABLE_SUFFIX = "_tmp_tbl_";

  private final String stepId;
  private final String schema;
  private final double featureWriterBatchSizeInMb;

  private String rootTable;

  public static void main(String[] args) {}

  public ImportQueryBuilder(String stepId, String schema){
    this.stepId = stepId;
    this.schema = schema;
    this.featureWriterBatchSizeInMb = 10;
  }

  public ImportQueryBuilder(String stepId, String schema, String rootTable, long versionsToKeep, double featureWriterBatchSizeInMb) {
    this.stepId = stepId;
    this.schema = schema;
    this.rootTable = rootTable;
    this.featureWriterBatchSizeInMb = featureWriterBatchSizeInMb;
  }

  public SQLQuery buildNextVersionQuery(){
    return new SQLQuery("SELECT nextval('${schema}.${sequence}')")
            .withVariable("schema", schema)
            .withVariable("sequence", rootTable + "_version_seq");
  }

  public SQLQuery buildImportTaskQuery(Format format, Integer taskId, ImportInput taskInput, String serializedImportStep,
                                       String lambdaArn, String ownLambdaRegion, Map<String, Object> context,
                                       boolean useFeatureWriter, String failureCallback){

    String targetTable = useFeatureWriter ?
            schema+".\"" + getTemporaryDataTableName(taskId) + "\""
            : schema+".\"" + rootTable + "\"";

    return new SQLQuery(
            "SELECT perform_import_from_s3_task(#{taskId}, #{schema}, to_regclass(#{targetTable}), #{format}, " +
                    "#{s3Bucket}, #{s3Key}, #{s3Region}, #{filesize}, #{stepPayload}::JSON->'step', " +
                    "#{lambdaFunctionArn}, #{lambdaRegion}, '${{failureCallback}}')")
            .withContext(context)
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
            .withQueryFragment("failureCallback", failureCallback);
  }

  public SQLQuery buildCreateImportTriggerForEmptyLayers(String targetAuthor, long targetSpaceVersion, boolean retainMetadata){
    return new SQLQuery("CREATE OR REPLACE TRIGGER insertTrigger BEFORE INSERT ON ${schema}.${table} "
            + "FOR EACH ROW EXECUTE PROCEDURE ${triggerFunction}('${{author}}', ${{spaceVersion}}, ${{retainMetadata}});")
            .withQueryFragment("spaceVersion", "" + targetSpaceVersion)
            .withQueryFragment("author", targetAuthor)
            .withQueryFragment("retainMetadata", "" + retainMetadata)
            .withVariable("triggerFunction", "tasked_import_from_s3_trigger_for_empty_layer")
            .withVariable("schema", schema)
            .withVariable("table", rootTable);
  }

  public SQLQuery buildTemporaryDataTableForImportQuery(int taskId){
    String tableFields =
            "  jsondata TEXT," +
                    " i BIGSERIAL PRIMARY KEY";
    return new SQLQuery("CREATE TABLE IF NOT EXISTS ${schema}.${table} (${{tableFields}} )")
            .withQueryFragment("tableFields", tableFields)
            .withVariable("schema", schema )
            .withVariable("table", getTemporaryDataTableName(taskId));
  }

  public SQLQuery dropTemporaryDataTableForImportQuery(int taskId){
    return new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table} ;")
            .withVariable("schema", schema )
            .withVariable("table", getTemporaryDataTableName(taskId));
  }

  public SQLQuery buildTriggerCleanUpStatement(){
    //Delete trigger - if present
   return new SQLQuery("DROP TRIGGER IF EXISTS insertTrigger ON ${schema}.${table};")
      .withVariable("schema", schema)
      .withVariable("table", rootTable);
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

  public SQLQuery buildDropAllTemporaryTablesByStepPrefixQuery() {
    return new SQLQuery("""
            DO $$
            DECLARE
              r RECORD;
            BEGIN
              FOR r IN
                SELECT tablename
                  FROM pg_tables
                 WHERE schemaname = #{schema}
                   AND left(tablename, length(#{tablePrefix})) = #{tablePrefix}
              LOOP
                EXECUTE format('DROP TABLE IF EXISTS %I.%I;', #{schema}, r.tablename);
              END LOOP;
            END
            $$;
        """)
            .withNamedParameter("schema", schema)
            .withNamedParameter("tablePrefix", getTemporaryJobTableName(this.stepId));
  }


  public String getTemporaryDataTableName(int taskId) {
    return getTemporaryJobTableName(this.stepId) + TMP_TABLE_SUFFIX + taskId;
  }

  /**
   * Builds a task query which imports features from the temporary trigger table using i-range reads.
   */
  public SQLQuery buildImportFromTmpTableTaskQuery(Integer taskId, long rangeStart,
                                                   String author, long currentVersion, boolean isPartial, UpdateStrategy updateStrategy,
                                                   String serializedImportStep, String lambdaArn, String ownLambdaRegion,
                                                   Map<String, Object> context, String failureCallback) {

    return new SQLQuery(
            "SELECT perform_import_from_tmp_table_task(#{taskId}, to_regclass(#{sourceTable}), #{rangeStart}, #{targetMb}, "
                    + "#{author}, #{currentVersion}, #{isPartial}, #{onExists}, #{onNotExists}, #{onVersionConflict}, "
                    + "#{onMergeConflict}, #{stepPayload}::JSON->'step', #{lambdaFunctionArn}, #{lambdaRegion}, '${{failureCallback}}')")
            .withContext(context)
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
            .withQueryFragment("failureCallback", failureCallback);
  }
}
