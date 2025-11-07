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

package com.here.xyz.jobs.steps.impl;

import com.google.common.io.ByteStreams;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.EntityPerLine;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.Format;
import com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.here.xyz.jobs.steps.Step.InputSet.USER_INPUTS;

@Disabled
public class TaskedImportStepTest extends ImportStepTest {

  @Test
  public void testSyncImport_with_many_files() throws Exception {
    executeImportStepWithManyFiles(Format.GEOJSON, 10, 2 , false);
  }

  //@Test
  @Disabled("Takes extra 6 minutes of execution time, disabled by default")
  public void testSyncImport_with_more_than_default_pagination_size_files() throws Exception {
    executeImportStepWithManyFiles(Format.GEOJSON, 1010, 2 , false);
  }

  @Test
  public void testImport_inEmpty_GEOJSON_Entity_Feature() throws Exception {
    //Gets executed SYNC
    executeImportStep(Format.GEOJSON, 0, EntityPerLine.Feature, false);
  }

  @Test
  public void testImport_inEmpty_CSV_JSON_WKB_Entity_Feature() throws Exception {
    //NOT IMPLEMENTED
  }

  @Test
  public void testImport_inEmpty_CSV_GEOJSON_Entity_Feature() throws Exception {
    //NOT IMPLEMENTED
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
    //NOT IMPLEMENTED
  }

  @Test
  public void testImport_inNonEmpty_GEOJSON_Entity_FeatureCollection() throws Exception {
    //NOT IMPLEMENTED
  }

  @Test
  public void testImport_inNonEmpty_CSV_GEOJSON_Entity_FeatureCollection() throws Exception {
    Assertions.assertThrows(BaseHttpServerVerticle.ValidationException.class, () -> new ImportFilesToSpace()
            .withFormat(Format.CSV_JSON_WKB)
            .withEntityPerLine(EntityPerLine.FeatureCollection)
            .withSpaceId(SPACE_ID)
            .validate());
  }

  private void executeImportStepWithManyFiles(Format format, int fileCount, int featureCountPerFile, boolean runAsync) throws IOException, InterruptedException {
    uploadFiles(JOB_ID, fileCount, featureCountPerFile, format);
    LambdaBasedStep step = new TaskedImportFilesToSpace()
            .withVersionRef(new Ref(Ref.HEAD))
            .withJobId(JOB_ID)
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

  @Override
  protected void executeImportStep(Format format, int featureCountSource,
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

    //* Only files with enriched features are allowd */
    uploadInputFile(JOB_ID, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file1" + fileExtension)), contentType);
    uploadInputFile(JOB_ID, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file2" + fileExtension)), contentType);

    TaskedImportFilesToSpace step = new TaskedImportFilesToSpace()
            .withJobId(JOB_ID)
            .withVersionRef(new Ref(Ref.HEAD))
            //.withFormat(format)
            //.withEntityPerLine(entityPerLine)
            //.withUpdateStrategy(DEFAULT_UPDATE_STRATEGY)
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
}
