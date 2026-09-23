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

package com.here.xyz.test.util.geo;

import com.here.xyz.models.geojson.coordinates.LineStringCoordinates;
import com.here.xyz.models.geojson.coordinates.LinearRingCoordinates;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.geojson.implementation.LineString;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Polygon;
import com.here.xyz.util.geo.GeometryValidator;
import com.here.xyz.util.geo.GeometryValidator.GeometryException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class GeometryValidatorIT {

    @Disabled("Geometry validation temporarily disabled - see GeometryValidator.validateGeometry TODO")
    @Test
    public void testValidateNullGeometry(){
        //Geometry cant be null!
        Assertions.assertThrows(GeometryException.class, () -> GeometryValidator.validateGeometry(null, 0));
    }

    @Disabled("Geometry validation temporarily disabled - see GeometryValidator.validateGeometry TODO")
    @Test
    public void testValidateIntersectsWithAntimeridian() throws InvalidGeometryException {
        LineStringCoordinates coordinates = new LineStringCoordinates();
        coordinates.add(new Position(179.999999, 0));
        coordinates.add(new Position(179.999999, 1));

        //Geometry filter intersects with anti meridian!
        Assertions.assertThrows(GeometryException.class, () -> GeometryValidator.validateGeometry(new LineString().withCoordinates(coordinates), 100));
    }

    @Disabled("Geometry validation temporarily disabled - see GeometryValidator.validateGeometry TODO")
    @Test
    public void testValidateMaxNumberOfCoordinates() throws InvalidGeometryException {
        LineStringCoordinates coordinates = new LineStringCoordinates();
        for (int i = 0; i <= GeometryValidator.MAX_NUMBER_OF_COORDNIATES ; i++) { coordinates.add(new Position(0.1 + i * 0.00001, 0.01 + i * 0.00001)) ;}

        //Geometry exceeds number of coordinates!
        Assertions.assertThrows(GeometryException.class, () -> GeometryValidator.validateGeometry(new LineString().withCoordinates(coordinates), 0));
    }

    @Disabled("Geometry validation temporarily disabled - see GeometryValidator.validateGeometry TODO")
    @Test
    public void testInvalidGeometry() throws InvalidGeometryException {
        Point point = new Point().withCoordinates(new PointCoordinates(190,1));
        //Invalid filter geometry!"
        Assertions.assertThrows(GeometryException.class, () -> GeometryValidator.validateGeometry(point, 0));
    }

    @Test
    public void testIsWorldBoundingBox_withValidBBoxes() {
        PolygonCoordinates expectedCoordinates = new PolygonCoordinates();

        // Expected coordinates of a world bounding box
        LinearRingCoordinates lrc = new LinearRingCoordinates();
        lrc.add(new Position(-180.0, -90.0));
        lrc.add(new Position(180.0, -90.0));
        lrc.add(new Position(180.0, 90.0));
        lrc.add(new Position(-180.0, 90.0));
        lrc.add(new Position(-180.0, -90.0));

        expectedCoordinates.add(lrc);
        Polygon polygon = new Polygon().withCoordinates(expectedCoordinates);
        Assertions.assertTrue(GeometryValidator.isWorldBoundingBox(polygon), "Should detect valid world bbox");

        //other valid permutation
        lrc = new LinearRingCoordinates();
        lrc.add(new Position(-180.0, 90.0));
        lrc.add(new Position(-180.0, -90.0));
        lrc.add(new Position(180.0, -90.0));
        lrc.add(new Position(180.0, 90.0));
        lrc.add(new Position(-180.0, 90.0));
        expectedCoordinates.add(0, lrc);

        Assertions.assertTrue(GeometryValidator.isWorldBoundingBox(polygon), "Should detect valid world bbox");
    }

    @Test
    public void testIsWorldBoundingBox_withInvalidBBoxes() {
        PolygonCoordinates expectedCoordinates = new PolygonCoordinates();
        LinearRingCoordinates lrc = new LinearRingCoordinates();
        //invalid WWBOX
        lrc.add(new Position(-180.0, -90.0));
        lrc.add(new Position(180.0, -90.0));
        lrc.add(new Position(180.0, 90.0));
        lrc.add(new Position(-180.0, 90.0));
        lrc.add(new Position(-180.0, 90.0));
        expectedCoordinates.add(lrc);
        Polygon polygon = new Polygon().withCoordinates(expectedCoordinates);

        lrc = new LinearRingCoordinates();
        //no WWBOX
        lrc.add(new Position(8.228788827111089, 50.14263202814135));
        lrc.add(new Position(8.228788827111089, 49.76343646294657));
        lrc.add(new Position(8.728858987763317, 49.76343646294657));
        lrc.add(new Position(8.728858987763317, 50.14263202814135));
        lrc.add(new Position(8.228788827111089, 50.14263202814135));
        expectedCoordinates.add(0, lrc);

        Assertions.assertFalse(GeometryValidator.isWorldBoundingBox(polygon), "Should detect invalid world bbox");
    }
}
