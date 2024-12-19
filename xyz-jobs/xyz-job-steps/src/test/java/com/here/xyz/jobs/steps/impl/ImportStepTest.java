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

package com.here.xyz.jobs.steps.impl;

import static com.here.xyz.events.UpdateStrategy.DEFAULT_UPDATE_STRATEGY;
import static com.here.xyz.jobs.steps.Step.InputSet.USER_INPUTS;

import com.google.common.io.ByteStreams;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.EntityPerLine;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.Format;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ImportStepTest extends StepTest {
  /**
    Test Format`s
      * CSV_JSON_WKB
      * CSV_GEOSJON
      * GEOJSON

    Test EntityPerLine`s
      * Feature
      * FeatureCollection
   *
    Test UpdateStrategy`s
      * UpdateStrategy.OnExists
      * UpdateStrategy.OnNotExists
      * UpdateStrategy.OnMergeConflict
      * UpdateStrategy.OnVersionConflict

    Test ThreadCount-Settings
      * Check ThreadCount Calculation

    Test START_EXECUTION
      * Check Result
      * Check Temp-Table and its content
      * Check Temp-Trigger-Table
      * Check Trigger-Creation
      * Check set of ReadOnlyMode

    Test SUCCESS_CALLBACK
      * Check Statistics
      * Check cleanup of Temp-Table
      * Check cleanup Temp-Trigger-Table
      * Check release of ReadOnlyMode

    Test FAILURE_CALLBACK
      * Retryable: Check persistence of temp-tables
      * NonRetryable: Check clean-up of tables

    Test STATE_CHECK
      * Check Statistics
      * Check Result (running queries)

    Test ExecutionModes
      * ASYCN
      * SYNC

    Test Import in Empty Layer (all Formats / all execution modes )
      * Check Trigger-Type
      * Check result (Operation, author, etc)
      * Check if expected Features are present

    Test Import in NonEmpty Layer (all Formats / all execution modes )
      * Check Trigger-Type
      * Check result (Operation, author, etc)
      * Check if expected Features are present
      * Check if existing id got updated

    Test Cancel

    Test Resume
   */

  /** Import in Empty Layer + Entity: Feature */
  @Test
  public void testSyncImport_with_many_files() throws Exception {
    executeImportStepWithManyFiles(Format.GEOJSON, 10, 2 , false);
  }

  //@Test //temporary deactivation
  public void testAsyncSyncImport_with_many_files() throws Exception {
    executeImportStepWithManyFiles(Format.GEOJSON, 10, 2 , true);
  }

  @Test
  public void testImport_inEmpty_GEOJSON_Entity_Feature() throws Exception {
    //Gets executed SYNC
    executeImportStep(Format.GEOJSON, 0, EntityPerLine.Feature, false);
  }

  @Test
  public void testImport_inEmpty_CSV_JSON_WKB_Entity_Feature() throws Exception {
    //CSV_JSON_WKB gets always executed ASYNC
    executeImportStep(Format.CSV_JSON_WKB, 0, EntityPerLine.Feature, true);
  }

  @Test
  public void testImport_inEmpty_CSV_GEOJSON_Entity_Feature() throws Exception {
    //CSV_GEOJSON gets always executed ASYNC
    executeImportStep(Format.CSV_GEOJSON, 0, EntityPerLine.Feature, true);
  }

  /** Import in NON-Empty Layer + Entity: Feature */
  @Test
  public void testImport_inNonEmpty_GEOJSON_Entity_Feature() throws Exception {
    int existingFeatureCount = 10;
    //CSV_JSON_WKB gets always executed ASYNC
    putRandomFeatureCollectionToSpace(SPACE_ID, existingFeatureCount);
    executeImportStep(Format.GEOJSON, existingFeatureCount, EntityPerLine.Feature, true);
  }

  @Test
  public void testImport_inNonEmpty_CSV_JSON_WKB_Entity_Feature() throws Exception {
    int existingFeatureCount = 10;
    //Gets executed ASYNC
    putRandomFeatureCollectionToSpace(SPACE_ID, existingFeatureCount);
    executeImportStep(Format.CSV_JSON_WKB, existingFeatureCount, EntityPerLine.Feature, true);
  }

  /** Import in NON-Empty Layer + Entity: FeatureCollection */
  @Test
  public void testImport_inNonEmpty_GEOJSON_Entity_FeatureCollection() throws Exception {
    int existingFeatureCount = 10;
    putRandomFeatureCollectionToSpace(SPACE_ID, existingFeatureCount);
    executeImportStep(Format.GEOJSON, existingFeatureCount, EntityPerLine.FeatureCollection, true);
  }

  @Test
  public void testImport_inNonEmpty_CSV_GEOJSON_Entity_FeatureCollection() throws Exception {
    Assertions.assertThrows(BaseHttpServerVerticle.ValidationException.class, () -> new ImportFilesToSpace()
            .withFormat(Format.CSV_JSON_WKB)
            .withEntityPerLine(EntityPerLine.FeatureCollection)
            .withSpaceId(SPACE_ID).validate());
  }

  private void executeImportStep(Format format, int featureCountSource,
                                 ImportFilesToSpace.EntityPerLine entityPerLine, boolean runAsync) throws IOException, InterruptedException {
    StatisticsResponse statsBefore = getStatistics(SPACE_ID);
    if(featureCountSource == 0)
      Assertions.assertEquals(0L, statsBefore.getCount().getValue());
    else
      Assertions.assertEquals(Long.valueOf(featureCountSource), statsBefore.getCount().getValue());

    String fileExtension = switch(format) {
      case GEOJSON -> ".geojson";
      case CSV_JSON_WKB -> ".csvwkb";
      case CSV_GEOJSON ->  ".csvgeojson";
    };

    if (entityPerLine == EntityPerLine.FeatureCollection)
      fileExtension += "fc";

    S3ContentType contentType = format == Format.GEOJSON ? S3ContentType.APPLICATION_JSON : S3ContentType.TEXT_CSV;
    uploadInputFile(JOB_ID, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file1" + fileExtension)), contentType);
    uploadInputFile(JOB_ID, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file2" + fileExtension)), contentType);

    LambdaBasedStep step = new ImportFilesToSpace()
        .withJobId(JOB_ID)
        .withFormat(format)
        .withEntityPerLine(entityPerLine)
        .withUpdateStrategy(DEFAULT_UPDATE_STRATEGY)
        .withSpaceId(SPACE_ID)
        .withInputSets(List.of(USER_INPUTS.get()));

    if(runAsync)
      step.setUncompressedUploadBytesEstimation(1024 * 1024 * 1024);

    sendLambdaStepRequestBlock(step, true);

    StatisticsResponse statsAfter = getStatistics(SPACE_ID);

    //We have 2 files with 20 features each.
    Assertions.assertEquals(Long.valueOf(40 + featureCountSource), statsAfter.getCount().getValue());
    checkStatistics(40, step.loadUserOutputs());
  }

  private void executeImportStepWithManyFiles(Format format, int fileCount, int featureCountPerFile, boolean runAsync) throws IOException, InterruptedException {
    uploadFiles(JOB_ID, fileCount, featureCountPerFile, format);
    LambdaBasedStep step = new ImportFilesToSpace()
        .withJobId(JOB_ID)
        .withFormat(format)
        .withSpaceId(SPACE_ID)
        .withInputSets(List.of(USER_INPUTS.get()));

    if(runAsync)
      //Triggers async execution with max threads
      step.setUncompressedUploadBytesEstimation(1024 * 1024 * 1024);

    sendLambdaStepRequestBlock(step ,true);

    StatisticsResponse statsAfter = getStatistics(SPACE_ID);
    Assertions.assertEquals(Long.valueOf(fileCount * featureCountPerFile), statsAfter.getCount().getValue());
    checkStatistics(fileCount * featureCountPerFile, step.loadUserOutputs());
  }

  protected void checkStatistics(int expectedFeatureCount, List<Output> outputs) throws IOException {
    for (Object output : outputs) {
       if(output instanceof FeatureStatistics statistics) {
         Assertions.assertEquals(expectedFeatureCount, statistics.getFeatureCount());
      }
    }
  }
}
