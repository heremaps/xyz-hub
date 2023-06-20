package com.here.xyz.models.geojson;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.here.xyz.models.geojson.coordinates.BBox;
import org.junit.jupiter.api.Test;

public class HQuadTest {

    BBox bbox = new BBox()
            .withEast(13.38134765625)
            .withNorth(52.53662109375)
            .withWest(13.359375)
            .withSouth(52.5146484375);
    String base4QK = "12201203120220";
    String base10QK = "377894440";

    @Test
    public void testBase4Quadkey() {
        HQuad hQuad = new HQuad(base4QK, true);

        assertEquals(bbox, hQuad.getBoundingBox());
        assertEquals(14, hQuad.level);
        assertEquals(8800, hQuad.x);
        assertEquals(6486, hQuad.y);
        assertEquals(base4QK, hQuad.quadkey);
    }

    @Test
    public void testBase10Quadkey() {
        HQuad hQuad = new HQuad(base10QK, false);

        assertEquals(bbox, hQuad.getBoundingBox());
        assertEquals(14, hQuad.level);
        assertEquals(8800, hQuad.x);
        assertEquals(6486, hQuad.y);
        assertEquals(base4QK, hQuad.quadkey);
    }

    @Test
    public void testLRC() {
        HQuad hQuad = new HQuad(8800, 6486, 14);

        assertEquals(bbox, hQuad.getBoundingBox());
        assertEquals(14, hQuad.level);
        assertEquals(8800, hQuad.x);
        assertEquals(6486, hQuad.y);
        assertEquals(base4QK, hQuad.quadkey);
    }

    @Test
    public void testInvalidBase4QK() {
        assertThrows(IllegalArgumentException.class, () -> new HQuad("5031", true));
    }

    @Test
    public void testInvalidBase10QK() {
        assertThrows(IllegalArgumentException.class, () -> new HQuad("12s", false));
    }

    @Test
    public void testInvalidLRC() {
        assertThrows(IllegalArgumentException.class, () -> new HQuad(10, 10, 1));
    }
}
