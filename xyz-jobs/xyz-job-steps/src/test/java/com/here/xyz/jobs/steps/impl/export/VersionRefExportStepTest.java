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

package com.here.xyz.jobs.steps.impl.export;

import com.here.xyz.XyzSerializable;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;

import java.io.IOException;
import java.util.List;

import com.here.xyz.models.hub.Tag;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class VersionRefExportStepTest extends ExportTestBase {

  @BeforeEach
  public void setUp() throws Exception {
    createSpace(new Space().withId(SPACE_ID).withVersionsToKeep(2), false);
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

    //Tag prevents deletion of versions >=2
    createTag(SPACE_ID, new Tag().withId("tag1").withVersion(2));
  }

  @Test
  public void exportWithSingleVersion() throws IOException, InterruptedException {
    //below version 11 hub delivers no features because currently hub-service respects v2k. In future hub-service will
    //take only the minVersion into account.
    Ref versionRef = new Ref("11");

    executeExportStepAndCheckResults(SPACE_ID, null, null, null,
            versionRef, "search?versionRef=" + versionRef);
  }

  @Test
  public void exportWithVersionRange1_3() throws IOException, InterruptedException {
    Ref versionRef = new Ref("1..3");

    RuntimeException runtimeException = Assertions.assertThrowsExactly(RuntimeException.class, () ->
            executeExportStepAndCheckResults(SPACE_ID, null, null, null,
                    versionRef, "search?versionRef=" + versionRef)
    );
    Assertions.assertEquals("Validation exception: Invalid VersionRef! StartVersion is smaller than min available version '2'!", runtimeException.getMessage());
  }

  @Disabled //Enable if xyz-hub also delivers features starting from minVersion - without ignoring v2k
  @Test
  public void exportWithVersionRange3_5() throws IOException, InterruptedException {
    Ref versionRef = new Ref("3..5");

    //Fails currently, because we are exporting 4 features (2 from version 3 and 2 from version 4)
    //export now ignores v2k if a minVersion is set - hub will take v2k into account and will not return features.
    executeExportStepAndCheckResults(SPACE_ID, null, null, null,
            versionRef, "search?versionRef=" + versionRef);
  }
}
