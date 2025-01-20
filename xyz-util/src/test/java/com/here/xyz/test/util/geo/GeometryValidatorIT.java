package com.here.xyz.test.util.geo;

import com.here.xyz.models.geojson.coordinates.LineStringCoordinates;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.geojson.implementation.LineString;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.util.geo.GeometryValidator;
import com.here.xyz.util.geo.GeometryValidator.GeometryException;
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
    }

    @Test
    public void testValidateMaxNumberOfCoordinates() throws InvalidGeometryException {
        LineStringCoordinates coordinates = new LineStringCoordinates();
        for (int i = 0; i <= GeometryValidator.MAX_NUMBER_OF_COORDNIATES ; i++) { coordinates.add(new Position(0.1 + i * 0.00001, 0.01 + i * 0.00001)) ;}

        //Geometry exceeds number of coordinates!
        Assertions.assertThrows(GeometryException.class, () -> GeometryValidator.validateGeometry(new LineString().withCoordinates(coordinates), 0));
    }

    @Test
    public void testInvalidGeometry() throws InvalidGeometryException {
        Point point = new Point().withCoordinates(new PointCoordinates(190,1));
        //Invalid filter geometry!"
        Assertions.assertThrows(GeometryException.class, () -> GeometryValidator.validateGeometry(point, 0));
    }

}
