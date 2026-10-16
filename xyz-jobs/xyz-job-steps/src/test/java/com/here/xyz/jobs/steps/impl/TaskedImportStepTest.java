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
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
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
import com.here.xyz.util.ARN;
import com.here.xyz.util.runtime.LambdaFunctionRuntime;
import com.here.xyz.util.service.aws.lambda.SimulatedContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static com.here.xyz.jobs.steps.Step.InputSet.USER_INPUTS;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.START_EXECUTION;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.SUCCESS_CALLBACK;

public class TaskedImportStepTest extends StepTest {

  private final String SPACE_ID_EXT = SPACE_ID + "_ext";

  @BeforeEach
  public void setup() throws SQLException {
    cleanup();
    createSpace(new Space().withId(SPACE_ID).withVersionsToKeep(1000).withStorage(new Space.ConnectorRef().withId("psql")), false);
  }

  @AfterEach
  public void cleanup() throws SQLException {
    deleteSpace(SPACE_ID_EXT);
    super.cleanup();
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

  @Test
  public void testEmptyImportWithEmptyUserInput() throws Exception {
    TaskedImportFilesToSpace step = new TaskedImportFilesToSpace()
            .withJobId(JOB_ID)
            .withSpaceId(SPACE_ID)
            .withInputSets(List.of(USER_INPUTS.get()));
    //validation should fail because no user input got provided
    Assertions.assertFalse(step.validate());
  }

  @Test
  public void testEmptyImportWithoutUserInput() throws Exception {
    TaskedImportFilesToSpace step = new TaskedImportFilesToSpace()
            .withVersionRef(new Ref(Ref.HEAD))
            .withJobId(JOB_ID)
            .withSpaceId(SPACE_ID);
    //required for resolving versionRef
    step.prepare(null, null);
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

  /**
   * Import into a composite (extension) space while the target carries the filter context=EXTENSION.
   * The imported features must be written into the extension layer only and must NOT leak into the base
   * (SUPER) layer. The composite (DEFAULT context) view must show the base features plus the imported ones.
   */
  @Test
  public void testImport_intoExtension_with_contextEXTENSION() throws Exception {
    //Pre-populate the base/super space with 3 features that are not part of the import files
    putFeatureCollectionToSpace(SPACE_ID, new FeatureCollection().withFeatures(List.of(
        simpleFeature("base1"),
        simpleFeature("base2"),
        simpleFeature("base3")
    )));

    //Create the composite space extending the base space
    createSpace(new Space().withId(SPACE_ID_EXT).withVersionsToKeep(1000)
        .withStorage(new Space.ConnectorRef().withId("psql"))
        .withExtension(new Space.Extension().withSpaceId(SPACE_ID)), false);

        putFeatureCollectionToSpace(SPACE_ID_EXT, new FeatureCollection().withFeatures(List.of(
        simpleFeature("some-delta-1"),
        simpleFeature("base2"),
        simpleFeature("some-delta-2")
    )));



    long baseCountBefore = getStatistics(SPACE_ID).getCount().getValue();
    Assertions.assertEquals(3L, baseCountBefore);

    //Upload the two import files (40 features in total)
    uploadInputFile(JOB_ID, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/fileCtxTest1.geojson")),
        S3ContentType.APPLICATION_JSON);
//    uploadInputFile(JOB_ID, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/fileCtxTest2.geojson")),
//        S3ContentType.APPLICATION_JSON);

    TaskedImportFilesToSpace step = new TaskedImportFilesToSpace()
            .withJobId(JOB_ID)
            .withVersionRef(new Ref(Ref.HEAD))
            .withFormat(TaskedImportFilesToSpace.Format.GEOJSON)
            .withSpaceId(SPACE_ID_EXT)
            .withContext(SpaceContext.EXTENSION)
            .withInputSets(List.of(USER_INPUTS.get()));

    //The provided context has to be carried by the step so that it gets forwarded to the FeatureWriter
    Assertions.assertEquals(SpaceContext.EXTENSION, step.getContext());
    //Importing into a composite space always uses the FeatureWriter
    Assertions.assertTrue(step.useFeatureWriter());

    sendLambdaStepRequestBlock(step, true);

    //The base/super space must remain untouched - the import only wrote into the extension layer
    Assertions.assertEquals(3L, getStatistics(SPACE_ID).getCount().getValue());

    //The extension layer must contain exactly the 40 imported features
    Assertions.assertEquals(2, getFeaturesFromSmallSpace(SPACE_ID_EXT, SpaceContext.EXTENSION, null, false)
        .getFeatures().size());

    //The composite (DEFAULT) view must show the base features merged with the imported ones (3 + 40)
    Assertions.assertEquals(5, getFeaturesFromSmallSpace(SPACE_ID_EXT, SpaceContext.DEFAULT, null, false)
        .getFeatures().size());

    checkStatistics(1, step.loadUserOutputs());
  }

  /**
   * A resumed import into an empty layer must recreate its import trigger, even if it was dropped while the step was
   * suspended (e.g., together with an outdated SQL script version). Without the trigger, the features get written
   * without an id and the import fails with a not-null violation (23502).
   */
  @Test
  public void testResume_inEmpty_recreatesDroppedImportTrigger() throws Exception {
    uploadInputFile(JOB_ID, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file1.geojson")),
            S3ContentType.APPLICATION_JSON);
    uploadInputFile(JOB_ID, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file2.geojson")),
            S3ContentType.APPLICATION_JSON);

    TaskedImportFilesToSpace step = new TaskedImportFilesToSpace()
            .withJobId(JOB_ID)
            .withVersionRef(new Ref(Ref.HEAD))
            .withFormat(Format.GEOJSON)
            .withSpaceId(SPACE_ID)
            .withInputSets(List.of(USER_INPUTS.get()));
    step.prepare(null, null);
    Assertions.assertTrue(step.validate());
    //Like the definition a resume is started with, this one does not carry any state of the previous attempt
    Map<String, Object> stepDefinition = step.toMap();

    String rootTable = getRootTableName(SPACE_ID);
    String jobTable = "job_data_" + step.getId();
    try {
      //First attempt: creates the task items and the import trigger
      sendLambdaStepRequest(step, START_EXECUTION, true);
      waitTillTaskItemsAreFinalized(step);
      long targetVersion = loadTargetVersionOfTaskItems(jobTable);

      //Rewind to the state of an attempt which failed before its tasks were finalized and whose trigger got dropped
      //in between, as it happens when the script version providing the trigger function gets cleaned up
      runStatements(
              "DROP TRIGGER insertTrigger ON public.\"" + rootTable + "\"",
              "DELETE FROM public.\"" + rootTable + "\"",
              "UPDATE public.\"" + jobTable + "\" SET finalized = false, task_output = NULL");
      Assertions.assertNull(loadImportTriggerDefinition(rootTable));

      //Resume
      TaskedImportFilesToSpace resumedStep = XyzSerializable.fromMap(stepDefinition, TaskedImportFilesToSpace.class);
      executeResumeSimulated(resumedStep);

      String triggerDefinition = loadImportTriggerDefinition(rootTable);
      Assertions.assertNotNull(triggerDefinition, "The resume has to recreate the dropped import trigger");
      Assertions.assertTrue(triggerDefinition.endsWith("tasked_import_from_s3_trigger_for_empty_layer('ANONYMOUS', '"
              + targetVersion + "', 'false')"), "The import trigger has to use the version persisted in the task items, but is: "
              + triggerDefinition);

      waitTillTaskItemsAreFinalized(resumedStep);
      sendLambdaStepRequest(resumedStep, SUCCESS_CALLBACK, true);

      StatisticsResponse statsAfter = getStatistics(SPACE_ID);
      Assertions.assertEquals(40L, statsAfter.getCount().getValue());
      Assertions.assertEquals(targetVersion, statsAfter.getMaxVersion().getValue());
      checkStatistics(40, resumedStep.loadUserOutputs());
      Assertions.assertNull(loadImportTriggerDefinition(rootTable), "The import trigger has to be dropped after the import");
    }
    finally {
      deleteAllJobTables(List.of(step.getId()));
    }
  }

  /**
   * A repeated (non-resume) start of an import into an empty layer, which finds the task table of a previous attempt
   * with all task items finalized, finalizes the step directly. It must not leave an import trigger behind, otherwise
   * every later write into the space would be rewritten by it.
   */
  @Test
  public void testRepeatedStart_inEmpty_afterAllTasksFinalized_leavesNoImportTrigger() throws Exception {
    uploadInputFile(JOB_ID, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file1.geojson")),
            S3ContentType.APPLICATION_JSON);
    uploadInputFile(JOB_ID, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file2.geojson")),
            S3ContentType.APPLICATION_JSON);

    TaskedImportFilesToSpace step = new TaskedImportFilesToSpace()
            .withJobId(JOB_ID)
            .withVersionRef(new Ref(Ref.HEAD))
            .withFormat(Format.GEOJSON)
            .withSpaceId(SPACE_ID)
            .withInputSets(List.of(USER_INPUTS.get()));
    step.prepare(null, null);
    Assertions.assertTrue(step.validate());

    String rootTable = getRootTableName(SPACE_ID);
    try {
      //First attempt: all tasks get finalized, but the step itself does not (no success callback in the simulation)
      sendLambdaStepRequest(step, START_EXECUTION, true);
      waitTillTaskItemsAreFinalized(step);
      Assertions.assertNotNull(loadImportTriggerDefinition(rootTable));

      //Start again without a resume: The task table of the first attempt exists, all of its items are finalized
      sendLambdaStepRequest(step, START_EXECUTION, true);

      Assertions.assertNull(loadImportTriggerDefinition(rootTable),
              "A step which got finalized while resetting its task items must not recreate the import trigger");
      Assertions.assertEquals(40L, getStatistics(SPACE_ID).getCount().getValue());
    }
    finally {
      deleteAllJobTables(List.of(step.getId()));
    }
  }

  /**
   * Starts the execution of the given step in-process with {@code resume = true}, like
   * {@link LambdaBasedStep.LambdaBasedStepExecutor} does for a simulated START_EXECUTION request.
   * NOTE: A real resume can not be triggered in a simulation, because its detection requires the execution history of
   * the step function.
   */
  private void executeResumeSimulated(LambdaBasedStep step) throws Exception {
    new LambdaFunctionRuntime(new SimulatedContext("localLambda", null), step.getGlobalStepId());
    setField(step, "ownLambdaArn", new ARN(LAMBDA_ARN));
    setField(step, "isSimulation", true);
    step.execute(true);
  }

  private static void setField(LambdaBasedStep step, String fieldName, Object value) throws ReflectiveOperationException {
    Field field = LambdaBasedStep.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(step, value);
  }

  private long loadTargetVersionOfTaskItems(String jobTable) throws SQLException {
    try (Connection connection = getTestDatabaseConnection();
         Statement statement = connection.createStatement();
         ResultSet rs = statement.executeQuery("SELECT DISTINCT (task_input->>'targetVersion')::BIGINT FROM public.\""
                 + jobTable + "\"")) {
      Assertions.assertTrue(rs.next());
      long targetVersion = rs.getLong(1);
      Assertions.assertFalse(rs.next(), "All task items are expected to share the same targetVersion");
      return targetVersion;
    }
  }

  /**
   * @return The definition of the import trigger on the root table, or {@code null} if it does not exist.
   */
  private String loadImportTriggerDefinition(String rootTable) throws SQLException {
    try (Connection connection = getTestDatabaseConnection();
         PreparedStatement statement = connection.prepareStatement(
                 "SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname = 'inserttrigger' AND tgrelid = to_regclass(?)")) {
      statement.setString(1, "public.\"" + rootTable + "\"");
      try (ResultSet rs = statement.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    }
  }

  private void runStatements(String... sqlStatements) throws SQLException {
    try (Connection connection = getTestDatabaseConnection(); Statement statement = connection.createStatement()) {
      for (String sql : sqlStatements)
        statement.execute(sql);
    }
  }

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
