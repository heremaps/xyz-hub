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

package com.here.xyz.jobs.steps.impl;

import com.google.common.io.ByteStreams;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace;
import com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace.EntityPerLine;
import com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace.Format;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.responses.StatisticsResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import static com.here.xyz.jobs.steps.Step.InputSet.USER_INPUTS;

public class TaskedImportStepTest extends StepTest {

  @BeforeEach
  public void setup() throws SQLException {
    cleanup();
    createSpace(new Space().withId(SPACE_ID).withVersionsToKeep(1000).withStorage(new Space.ConnectorRef().withId("psql")), false);
  }

  //@Test
  public void write(){
    createTag(SPACE_ID, new Tag().withId("tag").withVersion(0));
    for (int i = 0; i < 999; i++) {

      putFeatureCollectionToSpace(SPACE_ID,
              new FeatureCollection().withFeatures(List.of(
        new Feature().withGeometry(new Point().withCoordinates(new PointCoordinates(10, 10)))))
      );
    }
    System.out.println("done");
  }

  @Disabled
  @Test
  public void testNewFormat() throws Exception {
    executeImportStep(TaskedImportFilesToSpace.Format.FAST_IMPORT_INTO_EMPTY,0, EntityPerLine.Feature);
  }

  @Test
  public void testSyncImport_with_many_files() throws Exception {
    executeImportStepWithManyFiles(Format.GEOJSON, 100, 2 , false);
  }

  //TODO: fix versionRef -1 issue
  @Disabled
  @Test
  public void testEmptyImportWithEmptyUserInput() throws Exception {
    TaskedImportFilesToSpace step = new TaskedImportFilesToSpace()
            .withVersionRef(new Ref(0))
            .withJobId(JOB_ID)
            .withSpaceId(SPACE_ID)
            .withInputSets(List.of(USER_INPUTS.get()));
    //validation should fail because no user input got provided
    Assertions.assertFalse(step.validate());
  }

  //TODO: fix versionRef -1 issue
  @Disabled
  @Test
  public void testEmptyImportWithoutUserInput() throws Exception {
    TaskedImportFilesToSpace step = new TaskedImportFilesToSpace()
            .withVersionRef(new Ref(0))
            .withJobId(JOB_ID)
            .withSpaceId(SPACE_ID);
    step.validate();
    sendLambdaStepRequestBlock(step ,true);
  }

  //@Test
  @Disabled("Takes extra 6 minutes of execution time, disabled by default")
  public void testSyncImport_with_more_than_default_pagination_size_files() throws Exception {
    executeImportStepWithManyFiles(Format.GEOJSON, 1010, 2, false );
  }

  @Test
  public void testImport_inEmpty_GEOJSON_Entity_Feature() throws Exception {
    //Gets executed SYNC
    executeImportStep(TaskedImportFilesToSpace.Format.GEOJSON, 0, EntityPerLine.Feature);
  }

  /** Import in NON-Empty Layer + Entity: Feature */
  @Test
  public void testImport_inNonEmpty_GEOJSON_Entity_Feature() throws Exception {
    int existingFeatureCount = 10;
    //CSV_JSON_WKB gets always executed ASYNC
    putRandomFeatureCollectionToSpace(SPACE_ID, existingFeatureCount);
    executeImportStep(TaskedImportFilesToSpace.Format.GEOJSON, existingFeatureCount, EntityPerLine.Feature);
  }

  //TODO: fix statistics
  @Disabled
  public void testImport_inNonEmpty_GEOJSON_Entity_FeatureCollection() throws Exception {
    int existingFeatureCount = 10;
    putRandomFeatureCollectionToSpace(SPACE_ID, existingFeatureCount);
    executeImportStep(Format.GEOJSON, existingFeatureCount, EntityPerLine.FeatureCollection);
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

/****  */
  @Test
  public void testImport_inEmpty_GEOJSON_with_deleted_features() throws Exception {
    StatisticsResponse statsBefore = getStatistics(SPACE_ID);
    Assertions.assertEquals(0L, statsBefore.getCount().getValue());

    uploadInputFile(JOB_ID,
        ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file_with_deleted.geojson")),
        S3ContentType.APPLICATION_JSON);

    TaskedImportFilesToSpace step = new TaskedImportFilesToSpace()
            .withJobId(JOB_ID)
            .withVersionRef(new Ref(Ref.HEAD))
            .withFormat(TaskedImportFilesToSpace.Format.GEOJSON)
            .withSpaceId(SPACE_ID)
            .withInputSets(List.of(USER_INPUTS.get()));

    sendLambdaStepRequestBlock(step, true);

    StatisticsResponse statsAfter = getStatistics(SPACE_ID);

    // The file contains 10 features total: 5 marked as deleted, 5 normal.
    // When importing into an empty space, the deleted features should not appear as visible features.
    // Only the 5 non-deleted features should be counted.
    Assertions.assertEquals(Long.valueOf(5), statsAfter.getCount().getValue());
    checkStatistics(5, step.loadUserOutputs());
  }

  @Test
  public void testImport_inNonEmpty_GEOJSON_with_deleted_features() throws Exception {
    // Pre-populate the space with features that overlap with IDs in the import file.
    // Use IDs that match some deleted (del1, del2) and some normal (norm1, norm2) features from the file,
    // plus additional features (existing1, existing2) that are not in the import file.
    putFeatureCollectionToSpace(SPACE_ID, new FeatureCollection().withFeatures(List.of(
        simpleFeature("del1"),      // exists in file as deleted
        simpleFeature("del2"),      // exists in file as deleted
        simpleFeature("norm1"),     // exists in file as normal (will be updated)
        simpleFeature("norm2"),     // exists in file as normal (will be updated)
        simpleFeature("existing1"), // not in file, should remain untouched
        simpleFeature("existing2")  // not in file, should remain untouched
    )));

    StatisticsResponse statsBefore = getStatistics(SPACE_ID);
    Assertions.assertEquals(Long.valueOf(6), statsBefore.getCount().getValue());

    uploadInputFile(JOB_ID,
        ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file_with_deleted.geojson")),
        S3ContentType.APPLICATION_JSON);

    TaskedImportFilesToSpace step = new TaskedImportFilesToSpace()
            .withJobId(JOB_ID)
            .withVersionRef(new Ref(Ref.HEAD))
            .withFormat(TaskedImportFilesToSpace.Format.GEOJSON)
            .withSpaceId(SPACE_ID)
            .withInputSets(List.of(USER_INPUTS.get()));

    sendLambdaStepRequestBlock(step, true);

    StatisticsResponse statsAfter = getStatistics(SPACE_ID);

    // Import file: 5 deleted (del1-del5), 5 normal (norm1-norm5)
    // Pre-existing: del1, del2, norm1, norm2, existing1, existing2
    //
    // After import:
    //   del1, del2  -> deleted by import (removed from visible features)
    //   del3, del4, del5 -> deleted but didn't exist, no visible effect
    //   norm1, norm2 -> updated by import (still visible)
    //   norm3, norm4, norm5 -> inserted by import (new visible features)
    //   existing1, existing2 -> untouched (still visible)
    //
    // Expected visible count: norm1 + norm2 + norm3 + norm4 + norm5 + existing1 + existing2 = 7
    Assertions.assertEquals(Long.valueOf(7), statsAfter.getCount().getValue());
    checkStatistics(10, step.loadUserOutputs()); // todo: clarify should (del3, del4, del5) be within the count ?
  }

/****  */

  protected void executeImportStep(Format format, int featureCountSource,
                                   EntityPerLine entityPerLine) throws IOException, InterruptedException {

    StatisticsResponse statsBefore = getStatistics(SPACE_ID);
    if(featureCountSource == 0)
      Assertions.assertEquals(0L, statsBefore.getCount().getValue());
    else
      Assertions.assertEquals(Long.valueOf(featureCountSource), statsBefore.getCount().getValue());

    String fileExtension = switch(format) {
      case GEOJSON -> ".geojson";
      case FAST_IMPORT_INTO_EMPTY ->  ".new";
    };

    if (entityPerLine == EntityPerLine.FeatureCollection)
      fileExtension += "fc";

    S3ContentType contentType = format == Format.GEOJSON ? S3ContentType.APPLICATION_JSON : S3ContentType.TEXT_CSV;

    //* Only files with enriched features are allowed */
    uploadInputFile(JOB_ID, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file1" + fileExtension)), contentType);
    uploadInputFile(JOB_ID, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file2" + fileExtension)), contentType);

    TaskedImportFilesToSpace step = new TaskedImportFilesToSpace()
            .withJobId(JOB_ID)
            .withVersionRef(new Ref(Ref.HEAD))
            .withFormat(format)
            .withEntityPerLine(entityPerLine)
            .withSpaceId(SPACE_ID)
            .withInputSets(List.of(USER_INPUTS.get()));

    sendLambdaStepRequestBlock(step, true);

    StatisticsResponse statsAfter = getStatistics(SPACE_ID);

    //We have 2 files with 20 features each.
    Assertions.assertEquals(Long.valueOf(40 + featureCountSource), statsAfter.getCount().getValue());
    checkStatistics(40, step.loadUserOutputs());
  }

  protected void checkStatistics(int expectedFeatureCount, List<Output> outputs) throws IOException {
    for (Object output : outputs) {
      if(output instanceof FeatureStatistics statistics) {
        Assertions.assertEquals(expectedFeatureCount, statistics.getFeatureCount());
      }
    }
  }
}
