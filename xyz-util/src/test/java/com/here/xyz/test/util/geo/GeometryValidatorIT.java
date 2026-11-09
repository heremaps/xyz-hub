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

import static com.here.xyz.util.geo.GeometryValidator.MAX_NUMBER_OF_COORDNIATES;

import com.here.xyz.models.geojson.coordinates.LineStringCoordinates;
import com.here.xyz.models.geojson.coordinates.LinearRingCoordinates;
import com.here.xyz.models.geojson.coordinates.MultiPolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.geojson.implementation.GeometryCollection;
import com.here.xyz.models.geojson.implementation.LineString;
import com.here.xyz.models.geojson.implementation.MultiPolygon;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Polygon;
import com.here.xyz.util.geo.GeometryValidator;
import com.here.xyz.util.geo.GeometryValidator.GeometryException;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GeometryValidatorIT {

    @Test
    public void testValidateNullGeometry(){
        //Geometry cant be null!
        Assertions.assertThrows(GeometryException.class, () -> GeometryValidator.validateGeometry(null, 0));
    }

    @Test
    public void testValidateIntersectsWithAntimeridian() throws InvalidGeometryException {
        LineStringCoordinates coordinates = new LineStringCoordinates();
        coordinates.add(new Position(179.999999, 0));
        coordinates.add(new Position(179.999999, 1));

        //Geometry filter intersects with anti meridian!
        Assertions.assertThrows(GeometryException.class, () -> GeometryValidator.validateGeometry(new LineString().withCoordinates(coordinates), 100));

        //Far enough from the antimeridian
        LineStringCoordinates farAway = new LineStringCoordinates();
        farAway.add(new Position(170, 0));
        farAway.add(new Position(170, 1));
        Assertions.assertDoesNotThrow(() -> GeometryValidator.validateGeometry(new LineString().withCoordinates(farAway), 100));

        //Points are exempt
        Point point = new Point().withCoordinates(new PointCoordinates(179.999999, 0));
        Assertions.assertDoesNotThrow(() -> GeometryValidator.validateGeometry(point, 100));
    }

    @Test
    public void testValidateMaxNumberOfCoordinates() throws InvalidGeometryException {
        LineStringCoordinates coordinates = new LineStringCoordinates();
        for (int i = 0; i <= MAX_NUMBER_OF_COORDNIATES ; i++) { coordinates.add(new Position(0.1 + i * 0.00001, 0.01 + i * 0.00001)) ;}

        //Geometry exceeds number of coordinates!
        GeometryException e = Assertions.assertThrows(GeometryException.class,
            () -> GeometryValidator.validateGeometry(new LineString().withCoordinates(coordinates), 0));
        Assertions.assertTrue(e.getMessage().contains(String.valueOf(MAX_NUMBER_OF_COORDNIATES + 1)), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("maximum of " + MAX_NUMBER_OF_COORDNIATES), e.getMessage());
    }

    @Test
    public void testValidateAtMaxNumberOfCoordinates() {
        LineStringCoordinates coordinates = new LineStringCoordinates();
        for (int i = 0; i < MAX_NUMBER_OF_COORDNIATES; i++)
            coordinates.add(new Position(0.1 + i * 0.00001, 0.01 + i * 0.00001));
        Assertions.assertDoesNotThrow(() -> GeometryValidator.validateGeometry(new LineString().withCoordinates(coordinates), 0));

        //An explicit limit
        LineStringCoordinates eleven = new LineStringCoordinates();
        for (int i = 0; i < 11; i++)
            eleven.add(new Position(0.1 + i * 0.001, 0.01));
        Assertions.assertThrows(GeometryException.class,
            () -> GeometryValidator.validateGeometry(new LineString().withCoordinates(eleven), 0, 10));
        eleven.remove(10);
        Assertions.assertDoesNotThrow(() -> GeometryValidator.validateGeometry(new LineString().withCoordinates(eleven), 0, 10));
    }

    @Test
    public void testCoordinateCountRunsFirst() {
        LineStringCoordinates coordinates = new LineStringCoordinates();
        for (int i = 0; i < MAX_NUMBER_OF_COORDNIATES; i++)
            coordinates.add(new Position(0.1 + i * 0.00001, 0.01 + i * 0.00001));
        //Out of bounds, which validate() would reject
        coordinates.add(new Position(190, 1));

        GeometryException e = Assertions.assertThrows(GeometryException.class,
            () -> GeometryValidator.validateGeometry(new LineString().withCoordinates(coordinates), 100));
        Assertions.assertTrue(e.getMessage().contains("maximum of " + MAX_NUMBER_OF_COORDNIATES), e.getMessage());
        Assertions.assertFalse(e.getMessage().contains("Invalid filter geometry"), e.getMessage());
    }

    @Test
    public void testCountCoordinates() {
        Assertions.assertEquals(1, GeometryValidator.countCoordinates(new Point().withCoordinates(new PointCoordinates(1, 1))));

        Polygon polygon = new Polygon().withCoordinates(polygonWithHole());
        Assertions.assertEquals(10, GeometryValidator.countCoordinates(polygon));

        MultiPolygonCoordinates multiPolygonCoordinates = new MultiPolygonCoordinates();
        multiPolygonCoordinates.add(polygonWithHole());
        multiPolygonCoordinates.add(polygonWithHole());
        Assertions.assertEquals(20, GeometryValidator.countCoordinates(new MultiPolygon().withCoordinates(multiPolygonCoordinates)));

        LineStringCoordinates lineStringCoordinates = new LineStringCoordinates();
        lineStringCoordinates.add(new Position(0, 0));
        lineStringCoordinates.add(new Position(1, 0));
        lineStringCoordinates.add(new Position(1, 1));
        GeometryCollection collection = new GeometryCollection().withGeometries(List.of(
            new Point().withCoordinates(new PointCoordinates(1, 1)),
            new LineString().withCoordinates(lineStringCoordinates)));
        Assertions.assertEquals(4, GeometryValidator.countCoordinates(collection));

        Polygon empty = new Polygon();
        empty.setCoordinates(null);
        Assertions.assertEquals(0, GeometryValidator.countCoordinates(empty));
    }

    @Test
    public void testValidateGeometryWithoutJtsConversion() {
        //A ring of MAX_NUMBER_OF_COORDNIATES positions around (8.5, 50.1), the last one closes the ring
        LinearRingCoordinates ring = new LinearRingCoordinates();
        for (int i = 0; i < MAX_NUMBER_OF_COORDNIATES - 1; i++) {
            double angle = 2 * Math.PI * i / (MAX_NUMBER_OF_COORDNIATES - 1);
            ring.add(new Position(8.5 + 0.1 * Math.cos(angle), 50.1 + 0.1 * Math.sin(angle)));
        }
        ring.add(new Position(ring.get(0).getLongitude(), ring.get(0).getLatitude()));
        PolygonCoordinates coordinates = new PolygonCoordinates();
        coordinates.add(ring);

        //The validation must not do any of the heavy work that needs a JTS geometry (e.g. a projection or a buffer)
        Polygon polygon = new Polygon() {
            @Override
            protected org.locationtech.jts.geom.Geometry convertToJTSGeometry() {
                throw new AssertionError("validateGeometry must not convert to JTS");
            }
        };
        polygon.setCoordinates(coordinates);
        Assertions.assertDoesNotThrow(() -> GeometryValidator.validateGeometry(polygon, 100));
    }

    @Test
    public void testInvalidGeometry() throws InvalidGeometryException {
        Point point = new Point().withCoordinates(new PointCoordinates(190,1));
        //Invalid filter geometry!"
        Assertions.assertThrows(GeometryException.class, () -> GeometryValidator.validateGeometry(point, 0));
    }

    //A Polygon with a shell and a hole of 5 positions each
    private static PolygonCoordinates polygonWithHole() {
        PolygonCoordinates coordinates = new PolygonCoordinates();
        coordinates.add(square(0, 0, 10));
        coordinates.add(square(2, 2, 2));
        return coordinates;
    }

    private static LinearRingCoordinates square(double lon, double lat, double side) {
        LinearRingCoordinates ring = new LinearRingCoordinates();
        ring.add(new Position(lon, lat));
        ring.add(new Position(lon + side, lat));
        ring.add(new Position(lon + side, lat + side));
        ring.add(new Position(lon, lat + side));
        ring.add(new Position(lon, lat));
        return ring;
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
