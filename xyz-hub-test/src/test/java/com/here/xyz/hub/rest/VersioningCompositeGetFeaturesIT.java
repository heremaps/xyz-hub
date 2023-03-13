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
    modifyComposite(DELTA, BASE);

    postFeature(BASE, newFeature(), AuthProfile.ACCESS_OWNER_1_ADMIN);

    postFeature(DELTA, newFeature(), AuthProfile.ACCESS_OWNER_1_ADMIN);
    postFeature(DELTA, newFeature()
            .withGeometry(new Point().withCoordinates(new PointCoordinates(50,50)))
            .withProperties(new Properties().with("key2", "value2")),
        AuthProfile.ACCESS_OWNER_1_ADMIN
    );
  }

  @After
  public void after() {
    removeSpace(BASE);
    removeSpace(DELTA);
  }
}
