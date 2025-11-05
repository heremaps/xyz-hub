package com.here.xyz.jobs.steps.impl.transport.tools;

import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.jobs.steps.impl.transport.tasks.inputs.ImportInput;
import com.here.xyz.util.db.SQLQuery;

import java.util.List;
import java.util.Map;

import static com.here.xyz.jobs.steps.impl.transport.TaskedSpaceBasedStep.getTemporaryJobTableName;
import static com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace.Format.GEOJSON;

public class ImportQueryBuilder {
  private static final String TRIGGER_TABLE_SUFFIX = "_trigger_tbl";

  private final String schema;
  private final String temporaryImportTable;
  private long versionsToKeep = 1;
  private String rootTable;

  public static void main(String[] args) {}

  public ImportQueryBuilder(String stepId, String schema, String rootTable, long versionsToKeep){
    this.schema = schema;
    this.rootTable = rootTable;
    this.versionsToKeep = versionsToKeep;
    this.temporaryImportTable = getTemporaryTriggerTableName(stepId);
  }

  public SQLQuery buildCleanUpStatement(){
    return SQLQuery.batchOf(
            new SQLQuery("DROP TRIGGER IF EXISTS insertTrigger ON ${schema}.${table};")
                    .withVariable("schema", schema)
                    .withVariable("table", rootTable),
            //Only present if FeatureWriter is used
            new SQLQuery("DROP TABLE IF EXISTS ${schema}.${triggerTable};")
                    .withVariable("schema", schema)
                    .withVariable("triggerTable", temporaryImportTable));
  }

  public SQLQuery buildTemporaryTriggerTableBlockForImportIntoEmpty(String targetAuthor, long newVersion, boolean retainMetadata){
    return SQLQuery.batchOf(buildCreateImportTriggerForEmptyLayers(targetAuthor, newVersion, retainMetadata));
  }

  public SQLQuery buildTemporaryTriggerTableBlockForImportWithFW(String author, long newVersion, String superRootTable,
                                                                 UpdateStrategy updateStrategy){
    return SQLQuery.batchOf(
            buildTemporaryTriggerTableForImportQuery(),
            buildCreateFeatureWriterImportTrigger(author, newVersion, superRootTable, updateStrategy)
    );
  }
  public SQLQuery buildNextVersionQuery(){
    return new SQLQuery("SELECT nextval('${schema}.${sequence}')")
            .withVariable("schema", schema)
            .withVariable("sequence", rootTable + "_version_seq");
  }

  public SQLQuery buildImportTaskQuery(Integer taskId, ImportInput taskInput, String serializedImportStep,
                                       String lambdaArn, String ownLambdaRegion, Map<String, Object> context,
                                       boolean useFeatureWriter, String failureCallback){

    String targetTable = useFeatureWriter ? schema+".\"" + temporaryImportTable + "\"" : schema+".\"" + rootTable + "\"";

    return new SQLQuery(
            "SELECT perform_import_from_s3_task(#{taskId}, #{schema}, to_regclass(#{targetTable}), #{format}, " +
                    "#{s3Bucket}, #{s3Key}, #{s3Region}, #{filesize}, #{stepPayload}::JSON->'step', " +
                    "#{lambdaFunctionArn}, #{lambdaRegion}, '${{failureCallback}}')")
            .withContext(context)
            .withAsync(true)
            .withNamedParameter("taskId", taskId)
            .withNamedParameter("schema", schema)
            .withNamedParameter("targetTable", targetTable)
            .withNamedParameter("format", GEOJSON.name())
            .withNamedParameter("s3Bucket", taskInput.s3Bucket())
            .withNamedParameter("s3Key", taskInput.s3Key())
            .withNamedParameter("s3Region", taskInput.s3Region())
            .withNamedParameter("filesize", taskInput.fileByteSize())
            .withNamedParameter("stepPayload", serializedImportStep)
            .withNamedParameter("lambdaFunctionArn", lambdaArn)
            .withNamedParameter("lambdaRegion", ownLambdaRegion)
            .withQueryFragment("failureCallback", failureCallback);
  }

  private SQLQuery buildTemporaryTriggerTableForImportQuery(){
    String tableFields =
            "jsondata TEXT, "
                    + "geo geometry(GeometryZ, 4326), "
                    + "count INT ";
    return new SQLQuery("CREATE TABLE IF NOT EXISTS ${schema}.${table} (${{tableFields}} )")
            .withQueryFragment("tableFields", tableFields)
            .withVariable("schema", schema )
            .withVariable("table", temporaryImportTable);
  }

  private String getTemporaryTriggerTableName(String stepId) {
    return getTemporaryJobTableName(stepId) + TRIGGER_TABLE_SUFFIX;
  }

  private SQLQuery buildCreateImportTriggerForEmptyLayers(String targetAuthor, long targetSpaceVersion, boolean retainMetadata){
    return new SQLQuery("CREATE OR REPLACE TRIGGER insertTrigger BEFORE INSERT ON ${schema}.${table} "
            + "FOR EACH ROW EXECUTE PROCEDURE ${triggerFunction}('${{author}}', ${{spaceVersion}}, ${{retainMetadata}});")
            .withQueryFragment("spaceVersion", "" + targetSpaceVersion)
            .withQueryFragment("author", targetAuthor)
            .withQueryFragment("retainMetadata", "" + retainMetadata)
            .withVariable("triggerFunction", "tasked_import_from_s3_trigger_for_empty_layer")
            .withVariable("schema", schema)
            .withVariable("table", rootTable);
  }

  private SQLQuery buildCreateFeatureWriterImportTrigger(String author, long newVersion, String superRootTable,
                                                         UpdateStrategy updateStrategy){
    List<String> tables = superRootTable == null ? List.of(rootTable) : List.of(superRootTable, rootTable);

    //TODO: Check if we can forward the whole transaction to the FeatureWriter rather than doing it for each row
    return new SQLQuery("""
        CREATE OR REPLACE TRIGGER insertTrigger BEFORE INSERT ON ${schema}.${table}
          FOR EACH ROW EXECUTE PROCEDURE ${triggerFunction}(
             ${{author}},
             ${{spaceVersion}},
             false, --isPartial
             ${{onExists}},
             ${{onNotExists}},
             ${{onVersionConflict}},
             ${{onMergeConflict}},
             ${{historyEnabled}},
             ${{context}},
             '${{tables}}',
             '${{format}}',
             '${{entityPerLine}}'
             )
        """)
            .withQueryFragment("spaceVersion", Long.toString(newVersion))
            .withQueryFragment("author", "'" + author + "'")
            .withQueryFragment("onExists", updateStrategy.onExists() == null ? "NULL" : "'" + updateStrategy.onExists() + "'")
            .withQueryFragment("onNotExists", updateStrategy.onNotExists() == null ? "NULL" : "'" + updateStrategy.onNotExists() + "'")
            .withQueryFragment("onVersionConflict",
                    updateStrategy.onVersionConflict() == null ? "NULL" : "'" + updateStrategy.onVersionConflict() + "'")
            .withQueryFragment("onMergeConflict",
                    updateStrategy.onMergeConflict() == null ? "NULL" : "'" + updateStrategy.onMergeConflict() + "'")
            .withQueryFragment("historyEnabled", "" + (versionsToKeep > 1))
            .withQueryFragment("context", superRootTable == null ? "NULL" : "'DEFAULT'")
            .withQueryFragment("tables", String.join(",", tables))
            //Maybe other formats will be supported in the future
            .withQueryFragment("format", GEOJSON.name())
            //If FeatureCollection is supported in the future, this needs to be adapted with EMR
            .withQueryFragment("entityPerLine", "Feature")
            .withVariable("schema", schema)
            .withVariable("triggerFunction", "import_from_s3_trigger_for_non_empty_layer")
            .withVariable("table", temporaryImportTable);
  }
}
