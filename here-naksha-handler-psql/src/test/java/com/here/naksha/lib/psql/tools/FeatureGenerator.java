/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.naksha.lib.psql.tools;

import com.here.naksha.lib.core.models.geojson.coordinates.PointCoordinates;
import com.here.naksha.lib.core.models.geojson.implementation.*;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;

public class FeatureGenerator {

  protected static Random RANDOM = new Random();

  public static Feature generateFeature(XyzNamespace xyzNamespace, List<String> propertyKeys) {
    propertyKeys = propertyKeys == null ? Collections.emptyList() : propertyKeys;

    final Feature f = new Feature(null);
    f.setGeometry(new Point()
        .withCoordinates(
            new PointCoordinates(360d * RANDOM.nextDouble() - 180d, 180d * RANDOM.nextDouble() - 90d)));
    propertyKeys.stream()
        .reduce(
            new Properties(),
            (properties, k) -> {
              properties.put(k, RandomStringUtils.randomAlphanumeric(3));
              return properties;
            },
            (a, b) -> a);
    return f;
  }

  public static FeatureCollection get11kFeatureCollection() throws Exception {
    final XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);
    final List<String> propertyKeys = Stream.generate(() -> RandomStringUtils.randomAlphanumeric(10))
        .limit(3)
        .collect(Collectors.toList());

    FeatureCollection collection = new FeatureCollection();
    collection.setFeatures(new ArrayList<>());
    collection
        .getFeatures()
        .addAll(Stream.generate(() -> generateFeature(xyzNamespace, propertyKeys))
            .limit(11000)
            .collect(Collectors.toList()));

    /** This property does not get auto-indexed */
    for (int i = 0; i < 11000; i++) {
      if (i % 5 == 0) {
        collection.getFeatures().get(i).getProperties().put("test", 1);
      }
    }

    return collection;
  }
}
