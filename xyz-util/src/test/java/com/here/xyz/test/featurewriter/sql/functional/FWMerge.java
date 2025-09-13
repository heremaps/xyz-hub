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

package com.here.xyz.test.featurewriter.sql.functional;

import static com.here.xyz.test.featurewriter.SpaceWriter.DEFAULT_AUTHOR;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.xyz.events.UpdateStrategy.OnExists;
import com.here.xyz.events.UpdateStrategy.OnMergeConflict;
import com.here.xyz.events.UpdateStrategy.OnNotExists;
import com.here.xyz.events.UpdateStrategy.OnVersionConflict;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.test.featurewriter.sql.SQLTestSuite;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class FWMerge extends SQLTestSuite {

  private static List<Arguments> mergeGeometryChangeArgs() {
    return List.of(
        Arguments.of(new PointCoordinates(0, 0), new PointCoordinates(10, 0)),
        Arguments.of(new PointCoordinates(0, 0), new PointCoordinates(0, 10)),
        Arguments.of(new PointCoordinates(0, 0, 0), new PointCoordinates(10, 0, 0))
    );
  }

  @ParameterizedTest
  @MethodSource("mergeGeometryChangeArgs")
  public void mergeGeometryChange(PointCoordinates coordsBefore, PointCoordinates coordsAfter) throws Exception {
    //Create f1 (v1)
    spaceWriter().writeFeature(
        new Feature()
            .withId(TEST_FEATURE_ID)
            .withProperties(new Properties().with("prop1", "value1"))
            .withGeometry(new Point().withCoordinates(coordsBefore)),
        DEFAULT_AUTHOR,
        OnExists.ERROR,
        OnNotExists.CREATE,
        null,
        null,
        false,
        null,
        true);

    //Update f1 with different property value (v2)
    spaceWriter().writeFeature(
        new Feature()
            .withId(TEST_FEATURE_ID)
            .withProperties(new Properties().with("prop1", "value2"))
            .withGeometry(new Point().withCoordinates(coordsBefore)),
        DEFAULT_AUTHOR,
        OnExists.REPLACE,
        OnNotExists.ERROR,
        null,
        null,
        false,
        null,
        true);

    //Update f1 with different geometry but use v1 as base (v3)
    spaceWriter().writeFeature(
        new Feature()
            .withId(TEST_FEATURE_ID)
            .withProperties(new Properties()
                .with("prop1", "value1")
                .withXyzNamespace(new XyzNamespace()
                    .withVersion(1)))
            .withGeometry(new Point().withCoordinates(coordsAfter)),
        DEFAULT_AUTHOR,
        OnExists.REPLACE,
        OnNotExists.ERROR,
        OnVersionConflict.MERGE,
        OnMergeConflict.ERROR,
        false,
        null,
        true);

    Feature writtenFeature = spaceWriter().getFeature(null);

    assertEquals(3, writtenFeature.getProperties().getXyzNamespace().getVersion());
    assertEquals(coordsAfter.get(0), ((Point) writtenFeature.getGeometry()).getCoordinates().get(0));
    assertEquals(coordsAfter.get(1), ((Point) writtenFeature.getGeometry()).getCoordinates().get(1));
  }
}
