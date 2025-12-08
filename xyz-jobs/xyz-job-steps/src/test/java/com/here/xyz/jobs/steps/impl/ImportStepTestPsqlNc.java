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

import static com.here.xyz.util.Random.randomAlpha;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.io.ByteStreams;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.EntityPerLine;
import com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace;
import static com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace.Format.FAST_IMPORT_INTO_EMPTY;
import static com.here.xyz.jobs.steps.Step.InputSet.USER_INPUTS;

import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.responses.StatisticsResponse;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ImportStepTestPsqlNc extends ImportStepTest {

  @Override
  protected String createSpaceId()
  { return getClass().getSimpleName() + "-drgnstn" + "_" + randomAlpha(5); }

  @Override
  protected Space createSpace(String spaceId) {
    return createSpace(new Space().withId(spaceId)
                                  .withVersionsToKeep(1000)
                                  .withStorage( new ConnectorRef().withId("psql-nl-connector") )
                                  .withSearchableProperties(Map.of("$testAlias:[$.properties.test]::scalar", true )),
                        false);
  }


  /** Import in Empty Layer + Entity: Feature */
  @Override
  @Disabled(" -- overwrite -- disabled -- psql-nl-connector ")
  @Test
  public void testSyncImport_with_many_files() throws Exception {
    super.testSyncImport_with_many_files();
  }

  @Override
  @Disabled(" -- overwrite -- disabled -- psql-nl-connector - Takes extra 6 minutes of execution time, disabled by default")
  @Test
  public void testSyncImport_with_more_than_default_pagination_size_files() throws Exception {
    super.testSyncImport_with_more_than_default_pagination_size_files();
  }

  //@Test //temporary deactivation
  @Override
  @Disabled(" -- overwrite -- disabled -- psql-nl-connector ")
  //@Test //temporary deactivation
  public void testAsyncSyncImport_with_many_files() throws Exception {
    super.testAsyncSyncImport_with_many_files();
  }

  @Override
  @Disabled(" -- overwrite -- disabled -- psql-nl-connector ")
  @Test
  public void testImport_inEmpty_GEOJSON_Entity_Feature() throws Exception {
    //super.testImport_inEmpty_GEOJSON_Entity_Feature();
    executeImportStep(TaskedImportFilesToSpace.Format.GEOJSON, 0, EntityPerLine.Feature);
  }

  @Override
  @Disabled(" -- overwrite -- disabled -- psql-nl-connector ")
  @Test
  public void testImport_inEmpty_CSV_JSON_WKB_Entity_Feature() throws Exception {
    super.testImport_inEmpty_CSV_JSON_WKB_Entity_Feature();
  }

  @Override
  @Disabled(" -- overwrite -- disabled -- psql-nl-connector ")
  @Test
  public void testImport_inEmpty_CSV_GEOJSON_Entity_Feature() throws Exception {
    super.testImport_inEmpty_CSV_GEOJSON_Entity_Feature();
  }

  @Override
  @Test
  public void testImport_inNonEmpty_CSV_JSON_WKB_Entity_Feature() throws Exception {
    super.testImport_inNonEmpty_CSV_JSON_WKB_Entity_Feature();
  }

  /** Import in NON-Empty Layer + Entity: FeatureCollection */
  @Override
  @Test
  public void testImport_inNonEmpty_GEOJSON_Entity_FeatureCollection() throws Exception {
    //super.testImport_inNonEmpty_GEOJSON_Entity_FeatureCollection();

     int existingFeatureCount = 10;
    //CSV_JSON_WKB gets always executed ASYNC
    putRandomFeatureCollectionToSpace(SPACE_ID, existingFeatureCount);
    executeImportStep(TaskedImportFilesToSpace.Format.GEOJSON, existingFeatureCount, EntityPerLine.Feature);

  }


  @Override
  @Disabled(" -- overwrite -- disabled -- psql-nl-connector ")
  @Test
  public void testImport_inNonEmpty_CSV_GEOJSON_Entity_FeatureCollection() throws Exception {
    super.testImport_inNonEmpty_CSV_GEOJSON_Entity_FeatureCollection();
  }

  @Disabled(" -- originates from TaskedImportStepTest ")
  @Test
  public void testNewFormat() throws Exception {
    executeImportStep(TaskedImportFilesToSpace.Format.FAST_IMPORT_INTO_EMPTY,0, EntityPerLine.Feature);
  }

  protected void executeImportStep(TaskedImportFilesToSpace.Format format, int featureCountSource,
                                   ImportFilesToSpace.EntityPerLine entityPerLine) throws IOException, InterruptedException {

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

    S3ContentType contentType = format == TaskedImportFilesToSpace.Format.GEOJSON ? S3ContentType.APPLICATION_JSON : S3ContentType.TEXT_CSV;

    //* Only files with enriched features are allowed */
    uploadInputFile(JOB_ID, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file1" + fileExtension)), contentType);
    uploadInputFile(JOB_ID, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file2" + fileExtension)), contentType);

    TaskedImportFilesToSpace step = new TaskedImportFilesToSpace()
            .withJobId(JOB_ID)
            .withVersionRef(new Ref(Ref.HEAD))
            .withFormat(format)
            .withSpaceId(SPACE_ID)
            .withInputSets(List.of(USER_INPUTS.get()));

    sendLambdaStepRequestBlock(step, true);

    StatisticsResponse statsAfter = getStatistics(SPACE_ID);

    //We have 2 files with 20 features each.
    Assertions.assertEquals(Long.valueOf(40 + featureCountSource), statsAfter.getCount().getValue());
    checkStatistics(40, step.loadUserOutputs());
  }

}
