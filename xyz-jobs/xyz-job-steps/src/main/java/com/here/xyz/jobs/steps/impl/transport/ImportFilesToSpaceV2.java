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

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.jobs.steps.S3DataFile;
import com.here.xyz.jobs.steps.execution.StepException;
import com.here.xyz.jobs.steps.impl.transport.tasks.inputs.ImportInput;
import com.here.xyz.jobs.steps.impl.transport.tasks.outputs.ImportOutput;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.inputs.InputFromOutput;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.jobs.steps.outputs.CreatedVersion;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.psql.query.QueryBuilder;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.events.UpdateStrategy.DEFAULT_UPDATE_STRATEGY;
import static com.here.xyz.jobs.steps.Step.Visibility.USER;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.ExecutionMode.ASYNC;
import static com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpaceV2.EntityPerLine.Feature;
import static com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpaceV2.EntityPerLine.FeatureCollection;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_ON_ASYNC_SUCCESS;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.infoLog;
import static com.here.xyz.util.web.XyzWebClient.WebClientException;


/**
 * This step imports a set of user provided inputs and imports their data into a specified space. This step produces exactly one output of
 * type {@link FeatureStatistics}.
 */
public class ImportFilesToSpaceV2 extends TaskedSpaceBasedStep<ImportFilesToSpaceV2, ImportInput, ImportOutput> {
  public static final String STATISTICS = "statistics";

  {
    setOutputSets(List.of(new OutputSet(STATISTICS, USER, true)));
  }

  @JsonView({Internal.class, Static.class})
  private long targetTableFeatureCount = -1;

  @JsonView({Internal.class, Static.class})
  private UpdateStrategy updateStrategy = DEFAULT_UPDATE_STRATEGY;

  @JsonView({Internal.class, Static.class})
  private EntityPerLine entityPerLine = Feature;

  @JsonView({Internal.class, Static.class})
  private Format format = Format.GEOJSON;

  public UpdateStrategy getUpdateStrategy() {
    return updateStrategy;
  }

  public void setUpdateStrategy(UpdateStrategy updateStrategy) {
    this.updateStrategy = updateStrategy;
  }

  public ImportFilesToSpaceV2 withUpdateStrategy(UpdateStrategy updateStrategy) {
    setUpdateStrategy(updateStrategy);
    return this;
  }

  public enum Format {
    GEOJSON;
  }

  public enum EntityPerLine {
    Feature,
    FeatureCollection
  }

  public EntityPerLine getEntityPerLine() {
    return entityPerLine;
  }

  public void setEntityPerLine(EntityPerLine entityPerLine) {
    this.entityPerLine = entityPerLine;
  }

  @JsonView({Internal.class, Static.class})
  private boolean retainMetadata = false;

  public ImportFilesToSpaceV2 withEntityPerLine(EntityPerLine entityPerLine) {
    setEntityPerLine(entityPerLine);
    return this;
  }

  public boolean isRetainMetadata() {
    return retainMetadata;
  }

  public void setRetainMetadata(boolean retainMetadata) {
    this.retainMetadata = retainMetadata;
  }

  public ImportFilesToSpaceV2 withRetainMetadata(boolean retainMetadata) {
    setRetainMetadata(retainMetadata);
    return this;
  }

  public ImportFilesToSpaceV2 withVersionRef(Ref versionRef) {
    setVersionRef(versionRef);
    return this;
  }

  @Override
  protected boolean queryRunsOnWriter() throws WebClientException, SQLException, TooManyResourcesClaimed {
    return true;
  }

  @Override
  protected int setInitialThreadCount(String schema) throws WebClientException, SQLException, TooManyResourcesClaimed {
    return 15;
  }

  @Override
  protected void initialSetup() throws SQLException, TooManyResourcesClaimed, WebClientException {
    long newVersion = getOrIncreaseVersionSequence();
    runWriteQuerySync(buildCreateImportTriggerStatement(space().getOwner(), newVersion), db(), 0);
  }

  @Override
  protected void finalCleanUp() throws WebClientException, SQLException, TooManyResourcesClaimed {
    runWriteQuerySync(buildDropImportTriggerStatement(), db(), 0);
  }

  @Override
  protected List<ImportInput> createTaskItems(String schema) throws WebClientException, SQLException, TooManyResourcesClaimed, QueryBuilder.QueryBuildingException {
    List<?> inputs = loadInputs(UploadUrl.class);
    if (inputs.isEmpty()) {
      throw new StepException("No valid inputs of type 'UploadUrl' found.");
    }
    List<ImportInput> taskItems = new ArrayList<>();
    for (Input input : (List<Input>) inputs) {
      if (input instanceof S3DataFile) {
        taskItems.add(
                new ImportInput(input.getS3Bucket(), input.getS3Key(), bucketRegion(), input.getByteSize())
        );
      }
    }

    /*
     * Old Tables should work with trigger and enrichment
     * New Tables should work with enrichedFiles
    */

    return taskItems;
  }

  @Override
  public List<Load> getNeededResources() {
    return List.of();
  }

  @Override
  public int getTimeoutSeconds() {
    return 3 * 24 * 3600;
  }

  @Override
  public String getDescription() {
    return "ImportsV2 the data to space " + getSpaceId();
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    //TBD
    return 100;
  }

  @Override
  public ExecutionMode getExecutionMode() {
    return ASYNC;
  }

  @Override
  public boolean validate() throws ValidationException {
    super.validate();
    //TBD
    return true;
  }
  
  @Override
  protected SQLQuery buildTaskQuery(String schema, Integer taskId, ImportInput taskInput)
          throws WebClientException {

    return new SQLQuery(
            "PERFORM import_from_s3_perform_v2(#{taskId}, #{schema}, to_regclass(#{targetTable}), #{format}, " +
                    "#{s3Bucket}, #{s3Key}, #{s3Region}, #{filesize}, #{stepPayload}::JSON->'step', " +
                    "#{lambdaFunctionArn}, #{lambdaRegion})")
            .withContext(getQueryContext(schema))
            .withAsync(true)
//            .withAsyncProcedure(false)
            .withNamedParameter("taskId", taskId)
            .withNamedParameter("schema", schema)
            .withNamedParameter("targetTable", schema+".\"" + getRootTableName(space()) + "\"")
            .withNamedParameter("format", "GEOJSON")
            .withNamedParameter("s3Bucket", taskInput.s3Bucket())
            .withNamedParameter("s3Key", taskInput.s3Key())
            .withNamedParameter("s3Region", taskInput.s3Region())
            .withNamedParameter("filesize", taskInput.fileByteSize())
            .withNamedParameter("stepPayload", new LambdaStepRequest().withStep(this).serialize())
            .withNamedParameter("lambdaFunctionArn", getwOwnLambdaArn().toString())
            .withNamedParameter("lambdaRegion", getwOwnLambdaArn().getRegion());
  }

  @Override
  protected void processOutputs(List<ImportOutput> taskOutputs) throws IOException {
    FeatureStatistics statistics = new FeatureStatistics();

    if(!taskOutputs.isEmpty()){
      long totalImportedRows = taskOutputs.stream().mapToLong(ImportOutput::extractRowCount).sum();
      long totalImportedBytes = taskOutputs.stream().mapToLong(ImportOutput::fileBytes).sum();

      statistics = new FeatureStatistics()
              .withFeatureCount(totalImportedRows)
              .withByteSize(totalImportedBytes);
    }

    infoLog(STEP_ON_ASYNC_SUCCESS, this,"Job Statistics: bytes=" + statistics.getByteSize() + " rows=" + statistics.getFeatureCount());
    registerOutputs(List.of(statistics), STATISTICS);
  }

  /**
   * This method returns the next space version in either of two ways:
   * <ol><li>By Fetching the next version from {@link CreatedVersion} provided as step input</li>
   * <li>By Incrementing the space version sequence</li></ol>
   */
  private long getOrIncreaseVersionSequence() throws SQLException, TooManyResourcesClaimed, WebClientException {

    Optional<Input> versionInput = loadInputs(InputFromOutput.class)
            .stream()
            .filter(input -> ((InputFromOutput) input).getDelegate() instanceof CreatedVersion)
            .findFirst();
    if(versionInput.isPresent()) {
      CreatedVersion version = (CreatedVersion) ((InputFromOutput) versionInput.get()).getDelegate();
      return version.getVersion();
    }

    return runReadQuerySync(new SQLQuery("SELECT nextval('${schema}.${sequence}')")
            .withVariable("schema", getSchema(db()))
            .withVariable("sequence", getRootTableName(space()) + "_version_seq"), db(), 0, rs -> {
      rs.next();
      return rs.getLong(1);
    });
  }

  private SQLQuery buildDropImportTriggerStatement() throws WebClientException {
    return  new SQLQuery("DROP TRIGGER if exists insertTrigger ON ${schema}.${table};")
            .withVariable("schema", getSchema(db()))
            .withVariable("table", getRootTableName(space()));
  }

  private String getTriggerFunctionName(boolean targetLayerIsEmpty) {
    if(!targetLayerIsEmpty)
      return "import_from_s3_trigger_for_non_empty_layer";
    else {
      String triggerFunction = "import_from_s3_trigger_for_empty_layer_v2";
      return triggerFunction + (entityPerLine == FeatureCollection ? "_geojsonfc" : "");
    }
  }

  /** copied over functions */
  private SQLQuery buildCreateImportTriggerStatement(String targetAuthor, long newVersion) throws WebClientException {
    if(useFeatureWriter())
      return buildCreateImportTrigger(targetAuthor, newVersion);
    return buildCreateFeatureWriterImportTrigger(targetAuthor, newVersion);
  }

  private SQLQuery buildCreateFeatureWriterImportTrigger(String targetAuthor, long targetSpaceVersion) throws WebClientException {
    String triggerFunction = getTriggerFunctionName(true);

    return new SQLQuery("CREATE OR REPLACE TRIGGER insertTrigger BEFORE INSERT ON ${schema}.${table} "
            + "FOR EACH ROW EXECUTE PROCEDURE ${triggerFunction}('${{author}}', ${{spaceVersion}}, ${{retainMetadata}});")
            .withQueryFragment("spaceVersion", "" + targetSpaceVersion)
            .withQueryFragment("author", targetAuthor)
            .withQueryFragment("retainMetadata", "" + isRetainMetadata())
            .withVariable("triggerFunction", triggerFunction)
            .withVariable("schema", getSchema(db()))
            .withVariable("table", getRootTableName(space()));
  }

  private SQLQuery buildCreateImportTrigger(String author, long newVersion) throws WebClientException {
    String triggerFunction = getTriggerFunctionName(false);
    String superTable = space().getExtension() != null ? getRootTableName(superSpace()) : null;

    List<String> tables = superTable == null ? List.of(getRootTableName(space())) : List.of(superTable, getRootTableName(space()));

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
            .withQueryFragment("historyEnabled", "" + (space().getVersionsToKeep() > 1))
            .withQueryFragment("context", superTable == null ? "NULL" : "'DEFAULT'")
            .withQueryFragment("tables", String.join(",", tables))
            .withQueryFragment("format", format.toString())
            .withQueryFragment("entityPerLine", entityPerLine.toString())
            .withVariable("schema", getSchema(db()))
            .withVariable("triggerFunction", triggerFunction)
            .withVariable("table", getRootTableName(space()));
  }

  /*
   * Use FeatureWriter if either is true
   * - the target space is not empty
   * - space is composite
   */
  public boolean useFeatureWriter() throws WebClientException {
    return loadTargetSpaceFeatureCount() > 0 || space().getExtension() != null;
  }

  private long loadTargetSpaceFeatureCount() {
    if (targetTableFeatureCount == -1 && getSpaceId() != null) {
      StatisticsResponse statistics;
      try {
        statistics = loadSpaceStatistics(getSpaceId(), EXTENSION, true);
        targetTableFeatureCount = statistics.getCount().getValue();
      }
      catch (WebClientException e) {
        throw new RuntimeException(e);
      }
    }
    return targetTableFeatureCount;
  }

}
