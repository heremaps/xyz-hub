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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.jobs.steps.impl.StepTest;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.models.filters.SpatialFilter;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Tag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExportTestBase extends StepTest {
  private static final Logger logger = LogManager.getLogger();

  protected void executeExportStepAndCheckResults(String spaceId, ContextAwareEvent.SpaceContext context,
                                                  SpatialFilter spatialFilter, PropertiesQuery propertiesQuery,
                                                  Ref versionRef,
                                                  String hubPathAndQuery)
          throws IOException, InterruptedException {
    //Retrieve all Features from Space
    FeatureCollection allExpectedFeatures = customReadFeaturesQuery(spaceId, hubPathAndQuery);

    //Create Step definition
    ExportSpaceToFiles step = new ExportSpaceToFiles()
            .withThreadCount(10)
            .withSpaceId(spaceId)
            .withJobId(JOB_ID);

    if (context != null)
      step.setContext(context);
    if (propertiesQuery != null)
      step.setPropertyFilter(propertiesQuery);
    if (spaceId != null)
      step.setSpatialFilter(spatialFilter);
    if (versionRef != null)
      step.setVersionRef(versionRef);

    //Send Lambda Requests
    sendLambdaStepRequestBlock(step, true);
    checkOutputs(new HashSet<>(allExpectedFeatures.getFeatures()), step.loadOutputs());
  }

  protected void checkOutputs(Set<Feature> expectedFeatures, List<Output> allOutputs)
          throws IOException {
    Assertions.assertNotEquals(0, allOutputs.size());

    Set<Feature> exportedFeatures = new HashSet<>();
    Set<FeatureStatistics> statistics = new HashSet<>();

    for (Output output : allOutputs) {
      if (output instanceof DownloadUrl downloadUrl)
        exportedFeatures.addAll(downloadFileAndDeserializeFeatures(downloadUrl));
        //TODO: FeatureStatistics could get only checked if we also support during simulation "UPDATE_CALLBACK"
      else if (output instanceof FeatureStatistics s)
        statistics.add(s);
    }

    checkFeatures(exportedFeatures, expectedFeatures);
    checkStatistics(statistics, expectedFeatures.size());
  }

  protected static void checkFeatures(Set<Feature> exportedFeatures, Set<Feature> expectedFeatures) {
    assertEquals(expectedFeatures.size(), exportedFeatures.size(), "Expected features count should match exported features");

    for (Feature f : expectedFeatures)
      assertEquals(f, exportedFeatures.stream().filter(feature -> feature.getId().equals(f.getId())).findFirst().orElseThrow(() -> new NoSuchElementException("Expected feature with id \"" + f.getId() + "\" was not exported.")));

    for (Feature f : exportedFeatures)
      assertEquals(expectedFeatures.stream().filter(feature -> feature.getId().equals(f.getId())).findFirst().orElseThrow(() -> new NoSuchElementException("Exported feature with id \"" + f.getId() + "\" was not expected")), f);
  }

  protected static void checkStatistics(Set<FeatureStatistics> statistics, int expectedCount) {
    for (FeatureStatistics s : statistics)
      assertEquals(s.getFeatureCount(), expectedCount);
  }

  void createTestData(int v2k, boolean createTag) throws JsonProcessingException {
    createSpace(new Space().withId(SPACE_ID).withVersionsToKeep(v2k), false);
    //Add two new Features //TODO: Do not create FeatureCollections out of a String, create them using the Model instead
    FeatureCollection fc1 = XyzSerializable.deserialize("""
            {
                 "type": "FeatureCollection",
                 "features": [
                     {
                         "type": "Feature",
                         "id": "point1",
                         "properties": {
                            "value" : "1"
                         },
                         "geometry": {
                             "coordinates": [
                                 8.43,
                                 50.06
                             ],
                             "type": "Point"
                         }
                     },
                     {
                         "type": "Feature",
                         "id": "point2",
                         "properties": {
                            "value" : "2"
                         },
                         "geometry": {
                             "coordinates": [
                                 8.49,
                                 50.07
                             ],
                             "type": "Point"
                         }
                     }
                 ]
             }
            """, FeatureCollection.class);

    FeatureCollection fc2 = XyzSerializable.deserialize("""
            {
                 "type": "FeatureCollection",
                 "features": [
                     {
                         "type": "Feature",
                         "id": "point1",
                         "properties": {
                            "value" : "new"
                         },
                         "geometry": {
                             "coordinates": [
                                 8.43,
                                 50.06
                             ],
                             "type": "Point"
                         }
                     },
                     {
                         "type": "Feature",
                         "id": "point3",
                         "properties": {
                            "value" : "3"
                         },
                         "geometry": {
                             "coordinates": [
                                 8.49,
                                 50.07
                             ],
                             "type": "Point"
                         }
                     }
                 ]
             }
            """, FeatureCollection.class);

    putFeatureCollectionToSpace(SPACE_ID, fc1);
    //=> Version 1
    deleteFeaturesInSpace(SPACE_ID, List.of("point2"));
    //=> Version 2
    putFeatureCollectionToSpace(SPACE_ID, fc2);
    //=> Version 3
    for (int i = 0; i < 10 ; i++) {
      putRandomFeatureCollectionToSpace(SPACE_ID, 2);
    }
    //=> Version 4-13

    if(createTag)
      //Tag prevents deletion of versions >=2
      createTag(SPACE_ID, new Tag().withId("tag1").withVersion(2));
  }
}
