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

package com.here.xyz.jobs.steps.impl.export;

import com.here.xyz.FeatureChange;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.hub.Ref;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.here.xyz.FeatureChange.Operation.DELETE;
import static com.here.xyz.FeatureChange.Operation.INSERT;
import static com.here.xyz.FeatureChange.Operation.UPDATE;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext;
import static com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles.OutputType;
import static com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles.OutputType.FOLDER_PATCH;

public class VersionRangeExportStepTest extends ExportTestBase {

  @BeforeEach
  public void setUp() throws Exception {
    createTestData(100,false);
  }

  @Test
  public void exportWithVersionRangeTest() throws IOException, InterruptedException {
    Ref versionRef = new Ref("0..HEAD");

    executeExportStepAndCheckResults(null, versionRef,
            "changesets?versionRef=" + versionRef + "&squashed=true", FOLDER_PATCH);
  }

  protected void executeExportStepAndCheckResults(SpaceContext context, Ref versionRef, String hubPathAndQuery, OutputType outputType)
          throws IOException, InterruptedException {

    //Create Step definition
    ExportSpaceToFiles step = new ExportSpaceToFiles()
            .withSpaceId(SPACE_ID)
            .withJobId(JOB_ID);
    if (context != null)
      step.setContext(context);
    if (versionRef != null)
      step.setVersionRef(versionRef);
    if(outputType != null)
      step.setOutputType(outputType);

    //Send Lambda Requests
    sendLambdaStepRequestBlock(step, true);
    checkOutputs(step.loadOutputs(), hubPathAndQuery);
  }

  protected void checkOutputs(List<Output> allOutputs, String hubPathAndQuery)
          throws IOException {

    //Retrieve all Features from Space
    Set<Feature> allExpectedInsertedFeatures = new HashSet<>(customReadFeaturesQuery(SPACE_ID, hubPathAndQuery + "&operation="+ FeatureChange.Operation.INSERT).getFeatures());
    Set<Feature> allExpectedUpdatedFeatures = new HashSet<>(customReadFeaturesQuery(SPACE_ID, hubPathAndQuery  + "&operation="+ FeatureChange.Operation.UPDATE).getFeatures());
    Set<Feature> allExpectedDeletedFeatures = new HashSet<>(customReadFeaturesQuery(SPACE_ID, hubPathAndQuery  + "&operation="+ FeatureChange.Operation.DELETE).getFeatures());

    Assertions.assertNotEquals(0, allOutputs.size());
    Set<Feature> exportedInsertedFeatures = new HashSet<>();
    Set<Feature> exportedUpdatedFeatures = new HashSet<>();
    Set<Feature> exportedDeletedFeatures = new HashSet<>();

    Set<FeatureStatistics> statistics = new HashSet<>();

    for (Output output : allOutputs) {
      if (output instanceof DownloadUrl downloadUrl) {
        if(downloadUrl.getUrl().toString().contains(INSERT.toString())) {
          exportedInsertedFeatures.addAll(downloadFileAndDeserializeFeatures(downloadUrl));
        }
        if(downloadUrl.getUrl().toString().contains(UPDATE.toString())) {
          exportedUpdatedFeatures.addAll(downloadFileAndDeserializeFeatures(downloadUrl));
        }
        if(downloadUrl.getUrl().toString().contains(DELETE.toString())) {
          exportedDeletedFeatures.addAll(downloadFileAndDeserializeFeatures(downloadUrl));
        }
      }
    }

    checkFeatures(exportedInsertedFeatures, allExpectedInsertedFeatures);
    checkFeatures(exportedUpdatedFeatures, allExpectedUpdatedFeatures);
    checkFeatures(exportedDeletedFeatures, allExpectedDeletedFeatures);

    checkStatistics(statistics, allExpectedInsertedFeatures.size() + allExpectedUpdatedFeatures.size() + allExpectedDeletedFeatures.size());
  }
}
