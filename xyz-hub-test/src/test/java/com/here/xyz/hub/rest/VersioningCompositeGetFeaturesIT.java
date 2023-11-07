/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

package com.here.xyz.hub.rest;

import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(RestTests.class)
public class VersioningCompositeGetFeaturesIT extends VersioningGetFeaturesIT {

  private static final String BASE = "base";
  private static final String DELTA = SPACE_ID;

  @Before
  public void before() {
    removeSpace(BASE);
    removeSpace(DELTA);

    createSpaceWithId(BASE);
    createSpaceWithVersionsToKeep(DELTA, 1000);
    makeComposite(DELTA, BASE);

    postFeature(BASE, newFeature(), AuthProfile.ACCESS_OWNER_1_ADMIN);

    postFeature(DELTA, newFeature(), AuthProfile.ACCESS_OWNER_1_ADMIN, true);
    postFeature(DELTA, newFeature()
            .withGeometry(new Point().withCoordinates(new PointCoordinates(50,50)))
            .withProperties(new Properties().with("key2", "value2")),
        AuthProfile.ACCESS_OWNER_1_ADMIN,
        true
    );
  }

  @After
  public void after() {
    removeSpace(BASE);
    removeSpace(DELTA);
  }

  @Test
  public void testFeatureDeletion() {
    deleteFeature(DELTA, "f1");
    getFeature(DELTA, "f1", "SUPER", 200);
    getFeature(DELTA, "f1", "EXTENSION", 200);
    getFeature(DELTA, "f1", "DEFAULT", 404);
    getFeature(DELTA, "f1", 404);
  }

  @Test
  public void testGetFeatureByIdAfterDeletionAndRecovery() {
    //Delete the feature in the composite space
    deleteFeature(DELTA, "f1");
    //"Recover" the feature in the composite space (by hardly deleting it from the EXTENSION)
    deleteFeature(DELTA, "f1", "EXTENSION");
    getFeature(DELTA, "f1", "EXTENSION", 404);
    getFeature(DELTA, "f1", "DEFAULT", 200);
    getFeature(DELTA, "f1", 200);
  }

  @Test
  public void testGetFeaturesByIdAfterDeletionAndRecoveryWithSpecificVersion() {
    //Delete the feature in the composite space (Creates version 2, see #before() above)
    deleteFeature(DELTA, "f1");
    //Check that versions 0 & 1 are still accessible correctly
    getFeature(DELTA, "f1", 0, "EXTENSION", 200);
    getFeature(DELTA, "f1", 0, "DEFAULT", 200);
    getFeature(DELTA, "f1", 1, "EXTENSION", 200);
    getFeature(DELTA, "f1", 1, "DEFAULT", 200);
    //Check that version 2 (the deletion marker) is only accessible in context EXTENSION
    getFeature(DELTA, "f1", 2, "EXTENSION", 200);
    getFeature(DELTA, "f1", 2, "DEFAULT", 404);
    //"Recover" the feature in the composite space (by hardly deleting it from the EXTENSION, creates version 3)
    deleteFeature(DELTA, "f1", "EXTENSION");
    //Check that version 3 is only accessible through context DEFAULT
    getFeature(DELTA, "f1", 3, "EXTENSION", 404);
    getFeature(DELTA, "f1", 3, "DEFAULT", 200);
    getFeature(DELTA, "f1", 3, 200);
    //Write another new version of the feature (creates version 4)
    postFeature(DELTA, newFeature().withProperties(new Properties().with("key3", "value3")), AuthProfile.ACCESS_OWNER_1_ADMIN,
        true);
    //Check that version 3 is still only accessible through context DEFAULT
    getFeature(DELTA, "f1", 3, "EXTENSION", 404);
    getFeature(DELTA, "f1", 3, "DEFAULT", 200);
    getFeature(DELTA, "f1", 3, 200);
    //Check that version 4 is accessible through both contexts
    getFeature(DELTA, "f1", 4, "EXTENSION", 200);
    getFeature(DELTA, "f1", 4, "DEFAULT", 200);
    getFeature(DELTA, "f1", 4, 200);
  }
}
