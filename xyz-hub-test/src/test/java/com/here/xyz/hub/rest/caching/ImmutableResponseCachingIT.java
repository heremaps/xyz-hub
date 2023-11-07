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

package com.here.xyz.hub.rest.caching;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;

import com.here.xyz.hub.rest.TestSpaceWithFeature;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ImmutableResponseCachingIT extends TestSpaceWithFeature {

  private static String cleanUpId;

  public static final String F1 = "f1";
  final static Feature TEST_FEATURE =  new Feature().withId(F1)
      .withGeometry(new Point().withCoordinates(new PointCoordinates(0,0)))
      .withProperties(new Properties().with("key1", "value1"));

  @Before
  public void setup() {
    cleanUpId = createSpaceWithRandomId();
    addFeature(cleanUpId, TEST_FEATURE);
  }

  @After
  public void tearDown() {
    removeSpace(cleanUpId);
  }

  @Test
  public void testStaticCachePositive() throws InterruptedException {
    getFeature(cleanUpId, F1, 0)
        .statusCode(200)
        //Item should not come from cache
        .header("stream-info",  not(containsString("CH=1")));
    Thread.sleep(1000);
    getFeature(cleanUpId, F1, 0)
        .statusCode(200)
        //Item should come from cache
        .header("stream-info",  containsString("CH=1"))
        //Cache type should be "static"
        .header("stream-info", containsString("CT=S"));
  }

  private void testFeatureHeadRequest(boolean expectToBeCached) throws InterruptedException {
    getFeature(cleanUpId, F1)
        .statusCode(200)
        //Item should not come from cache
        .header("stream-info",  not(containsString("CH=1")));
    Thread.sleep(1000);
    getFeature(cleanUpId, F1)
        .statusCode(200)
        //Item should still not come from cache
        .header("stream-info",  expectToBeCached ? containsString("CH=1") : not(containsString("CH=1")));
  }

  @Test
  public void testStaticCacheNegative() throws InterruptedException {
    testFeatureHeadRequest(false);
  }

  @Test
  public void testStaticCacheForReadOnlySpace() throws InterruptedException {
    setReadOnly(cleanUpId);
    testFeatureHeadRequest(true);
  }
}
