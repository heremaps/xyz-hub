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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.jobs.steps.S3DataFile;
import com.here.xyz.jobs.steps.execution.StepException;
import com.here.xyz.jobs.steps.impl.tools.ResourceAndTimeCalculator;
import com.here.xyz.jobs.steps.impl.transport.tasks.inputs.ImportInput;
import com.here.xyz.jobs.steps.impl.transport.tasks.outputs.ImportOutput;
import com.here.xyz.jobs.steps.impl.transport.tools.ImportQueryBuilder;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.inputs.InputFromOutput;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.jobs.steps.outputs.CreatedVersion;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.resources.IOResource;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.service.Core;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.events.UpdateStrategy.DEFAULT_UPDATE_STRATEGY;
import static com.here.xyz.jobs.steps.Step.Visibility.USER;
import static com.here.xyz.jobs.steps.impl.SpaceBasedStep.LogPhase.JOB_EXECUTOR;
import static com.here.xyz.jobs.steps.impl.SpaceBasedStep.LogPhase.JOB_VALIDATE;
import static com.here.xyz.jobs.steps.impl.SpaceBasedStep.LogPhase.STEP_ON_ASYNC_SUCCESS;
import static com.here.xyz.util.web.XyzWebClient.WebClientException;
import static com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace.Format.GEOJSON;
import static com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace.Format.FAST_IMPORT_INTO_EMPTY;
/**
 * TODO:
 *  - implement retry mechanism for failed imports
 *  - take over missing parts from ImportFilesToSpace (like resource calculation)
 * This step imports a set of user provided inputs and imports their data into a specified space. This step produces exactly one output of
 * type {@link FeatureStatistics}.
 */
public class TaskedImportFilesToSpace extends TaskedSpaceBasedStep<TaskedImportFilesToSpace, ImportInput, ImportOutput> {
  private static final int IMPORT_THREAD_COUNT = 15;
  private static final long MAX_INPUT_BYTES_FOR_KEEP_INDICES = 1l * 1024 * 1024 * 1024;
  private static final int MIN_FEATURE_COUNT_IN_TARGET_TABLE_FOR_KEEP_INDICES = 5_000_000;

  public static final String STATISTICS = "statistics";

  {
    setOutputSets(List.of(new OutputSet(STATISTICS, USER, true)));
  }

  //TODO: Work with enriched files which have a custom format. Here we plan to import into empty layers without using Triggers.
  public enum Format {
    GEOJSON, FAST_IMPORT_INTO_EMPTY
  }

  private ImportQueryBuilder importQueryBuilder;

  @JsonView({Internal.class, Static.class})
  private Format format = GEOJSON;

  @JsonView({Internal.class, Static.class})
  private int fileCount = -1;

  @JsonView({Internal.class, Static.class})
  private long targetTableFeatureCount = -1;

  @JsonView({Internal.class, Static.class})
  private UpdateStrategy updateStrategy = DEFAULT_UPDATE_STRATEGY;

  @JsonView({Internal.class, Static.class})
  private boolean retainMetadata = false;

  @JsonView({Internal.class, Static.class})
  private int estimatedSeconds = -1;

  //Compilers can decide max allowed import size. Set default to 10G for normal use-case
  @JsonIgnore
  private long maxInputBytesForNonEmptyImport = 10l * 1024 * 1024 * 1024;

  public UpdateStrategy getUpdateStrategy() {
    return updateStrategy;
  }

  public void setUpdateStrategy(UpdateStrategy updateStrategy) {
    this.updateStrategy = updateStrategy;
  }

  public TaskedImportFilesToSpace withUpdateStrategy(UpdateStrategy updateStrategy) {
    setUpdateStrategy(updateStrategy);
    return this;
  }

  public boolean isRetainMetadata() {
    return retainMetadata;
  }

  public void setRetainMetadata(boolean retainMetadata) {
    this.retainMetadata = retainMetadata;
  }

  public TaskedImportFilesToSpace withRetainMetadata(boolean retainMetadata) {
    setRetainMetadata(retainMetadata);
    return this;
  }

  public TaskedImportFilesToSpace withVersionRef(Ref versionRef) {
    setVersionRef(versionRef);
    return this;
  }

  public void setFormat(Format format) {
    this.format = format;
  }

  public Format getFormat() {
    return format;
  }

  public TaskedImportFilesToSpace withFormat(Format format) {
    setFormat(format);
    return this;
  }

  public long getMaxInputBytesForNonEmptyImport() {
    return maxInputBytesForNonEmptyImport;
  }

  public void setMaxInputBytesForNonEmptyImport(long maxInputBytesForNonEmptyImport) {
    this.maxInputBytesForNonEmptyImport = maxInputBytesForNonEmptyImport;
  }

  public TaskedImportFilesToSpace withMaxInputBytesForNonEmptyImport(long maxInputBytesForNonEmptyImport) {
    setMaxInputBytesForNonEmptyImport(maxInputBytesForNonEmptyImport);
    return this;
  }

  @Override
  protected boolean queryRunsOnWriter(){
    return true;
  }

  @Override
  protected int setInitialThreadCount(){
    return IMPORT_THREAD_COUNT;
  }

  @Override
  protected void initialSetup() throws SQLException, TooManyResourcesClaimed, WebClientException {
    long newVersion = getOrIncreaseVersionSequence();

    if(useFeatureWriter()) {
      String superRootTable = space().getExtension() != null ? getRootTableName(superSpace()) : null;
      runBatchWriteQuerySync(getQueryBuilder().buildTemporaryTriggerTableBlockForImportWithFW(space().getOwner(),
             newVersion, superRootTable, updateStrategy), db(), 0);
    }else{
      if(format.equals(FAST_IMPORT_INTO_EMPTY))
        return;
      //import into an empty, non-composite, layer
      runBatchWriteQuerySync(getQueryBuilder().buildTemporaryTriggerTableBlockForImportIntoEmpty(space().getOwner(), newVersion, retainMetadata),
              db(), 0);
    }
  }

  @Override
  protected void finalCleanUp() throws WebClientException, SQLException, TooManyResourcesClaimed {
    runBatchWriteQuerySync(getQueryBuilder().buildCleanUpStatement(), db(), 0);
  }

  @Override
  protected List<ImportInput> createTaskItems() {
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
    return taskItems;
  }

  @Override
  public List<Load> getNeededResources() {
    try {
      //Calculate estimation for ACUs for all parallel running threads
      overallNeededAcus = overallNeededAcus != -1 ? overallNeededAcus : calculateNeededAcus(IMPORT_THREAD_COUNT);
      return List.of(new Load().withResource(db()).withEstimatedVirtualUnits(overallNeededAcus),
              new Load().withResource(IOResource.getInstance()).withEstimatedVirtualUnits(getUncompressedUploadBytesEstimation()));
    }
    catch (WebClientException e) {
      throw new RuntimeException(e);
    }
  }

  private double calculateNeededAcus(int threadCount) {
    double neededACUs;

    neededACUs = ResourceAndTimeCalculator.getInstance().calculateNeededImportAcus(
            getUncompressedUploadBytesEstimation(), countFiles(), threadCount);
    neededACUs /= 4d; //TODO: Remove workaround once GraphSequentializer was implemented

    infoLog(JOB_EXECUTOR,  "Calculated ACUS: expectedMemoryConsumption: "
            + getUncompressedUploadBytesEstimation() + " => neededACUs:" + neededACUs);
    return neededACUs;
  }

  private int countFiles() {
    return fileCount = fileCount != -1 ? fileCount : currentInputsCount(UploadUrl.class);
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
    if (estimatedSeconds == -1 && getSpaceId() != null) {
      estimatedSeconds = ResourceAndTimeCalculator.getInstance()
              .calculateImportTimeInSeconds(getSpaceId(), getUncompressedUploadBytesEstimation(), getExecutionMode());
      infoLog(JOB_EXECUTOR,  "Calculated estimatedSeconds: "+estimatedSeconds );
    }
    return estimatedSeconds;
  }

  @Override
  public boolean validate() throws ValidationException {
    super.validate();
    try {
      infoLog(JOB_VALIDATE);
      //Check if the space is actually existing
      Space space = space();
      if (!space.isActive())
        throw new ValidationException("Data can not be written to target " + space.getId() + " as it is inactive.");

      if (space.isReadOnly())
        throw new ValidationException("Data can not be written to target " + space.getId() + " as it is in read-only mode.");

      if (loadTargetSpaceFeatureCount() > 0 && getUncompressedUploadBytesEstimation() > getMaxInputBytesForNonEmptyImport())
        throw new ValidationException("An import into a non empty space is not possible. "
                + "The uncompressed size of the provided files exceeds the limit of " + getMaxInputBytesForNonEmptyImport() + " bytes.");
    }
    catch (WebClientException e) {
      throw new ValidationException("Error loading resource " + getSpaceId(), e);
    }

    return !isUserInputsExpected() || isUserInputsPresent(UploadUrl.class);
  }
  
  @Override
  protected SQLQuery buildTaskQuery(Integer taskId, ImportInput taskInput, String failureCallback)
          throws WebClientException {

    return getQueryBuilder().buildImportTaskQuery(format, taskId, taskInput, new LambdaStepRequest().withStep(this).serialize(),
                    getwOwnLambdaArn().toString(), getwOwnLambdaArn().getRegion(), getQueryContext(getSchema(db())),
                    useFeatureWriter(), failureCallback);
  }

  @Override
  protected void processOutputs(List<ImportOutput> taskOutputs) throws IOException, WebClientException {
    FeatureStatistics statistics = new FeatureStatistics();

    if(!taskOutputs.isEmpty()){
      long totalImportedRows = taskOutputs.stream().mapToLong(ImportOutput::extractRowCount).sum();
      long totalImportedBytes = taskOutputs.stream().mapToLong(ImportOutput::fileBytes).sum();

      statistics = new FeatureStatistics()
              .withFeatureCount(totalImportedRows)
              .withByteSize(totalImportedBytes);
    }

    infoLog(STEP_ON_ASYNC_SUCCESS, "Job Statistics: bytes=" + statistics.getByteSize() + " rows=" + statistics.getFeatureCount());
    registerOutputs(List.of(statistics), STATISTICS);

    infoLog(STEP_ON_ASYNC_SUCCESS, "Set contentUpdatedAt on target space");
    hubWebClient().patchSpace(getSpaceId(), Map.of("contentUpdatedAt", Core.currentTimeMillis()));
  }

  public boolean keepIndices() {
    /*
     * The targetSpace needs to have more than MIN_FEATURE_COUNT_IN_TARGET_TABLE_FOR_KEEP_INDICES features
     * Reason: For tables with not that many records in its always faster to remove and recreate indices
     * +
     * Incoming bytes have to be smaller than MAX_INPUT_BYTES_FOR_KEEP_INDICES
     * Reason: if we write not that much, it's also with indices fast enough
     */
    return loadTargetSpaceFeatureCount() > MIN_FEATURE_COUNT_IN_TARGET_TABLE_FOR_KEEP_INDICES
            || getUncompressedUploadBytesEstimation() < MAX_INPUT_BYTES_FOR_KEEP_INDICES;
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

    return runReadQuerySync(getQueryBuilder().buildNextVersionQuery(), db(), 0, rs -> {
      rs.next();
      return rs.getLong(1);
    });
  }

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

  private ImportQueryBuilder getQueryBuilder() throws WebClientException {
    if(importQueryBuilder == null){
      importQueryBuilder = new ImportQueryBuilder(getId(), getSchema(db()), getRootTableName(space()), space().getVersionsToKeep());
    }
    return importQueryBuilder;
  }
}
