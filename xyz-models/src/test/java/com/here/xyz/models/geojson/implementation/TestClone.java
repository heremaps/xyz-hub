/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.models.geojson.implementation;

import static org.junit.Assert.assertNotEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

public class TestClone {

  public static FeatureCollection generateRandomFeatures(int featureCount, int propertyCount) throws JsonProcessingException {
    FeatureCollection collection = new FeatureCollection();
    Random random = new Random();

    List<String> pKeys = Stream.generate(() ->
        RandomStringUtils.randomAlphanumeric(10)).limit(propertyCount).collect(Collectors.toList());
    collection.setFeatures(new ArrayList<>());
    collection.getFeatures().addAll(
        Stream.generate(() -> {
          Feature f = new Feature()
              .withGeometry(
                  new Point().withCoordinates(new PointCoordinates(360d * random.nextDouble() - 180d, 180d * random.nextDouble() - 90d)))
              .withProperties(new Properties());
          pKeys.forEach(p -> f.getProperties().put(p, RandomStringUtils.randomAlphanumeric(8)));
          return f;
        }).limit(featureCount).collect(Collectors.toList()));
    return collection;
  }

  @Test
  public void testAsMap() throws JsonProcessingException {
    FeatureCollection collection = generateRandomFeatures(1, 1);
    Feature feature = collection.getFeatures().get(0);
    feature.toMap();
  }

  @Test
  public void testClone() throws JsonProcessingException {
    FeatureCollection collection = generateRandomFeatures(1, 1);
    Feature feature = collection.getFeatures().get(0);
    feature.getProperties().setXyzNamespace(new XyzNamespace().withSpace("test").withTags(Arrays.asList("a", "b", "c")));
    Feature copy = feature.copy();
    copy.setId("changed");
    assertNotEquals(copy.getId(), feature.getId());
  }
}
