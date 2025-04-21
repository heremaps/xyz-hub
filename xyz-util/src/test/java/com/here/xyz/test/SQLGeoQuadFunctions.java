package com.here.xyz.test;

import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SQLGeoQuadFunctions extends SQLITBase{

    //MERCATOR_QUAD TESTS
    @Test
    public void mercator_quad_lrc_to_qk() throws Exception {
        try (DataSourceProvider dsp = getDataSourceProvider()) {
            SQLQuery query = new SQLQuery("SELECT mercator_quad(550,335,10);");
            Assertions.assertEquals("1202102332", query.run(dsp, rs -> rs.next() ? rs.getString(1) : null));
        }
    }

    @Test
    public void mercator_quad_point_to_qk() throws Exception {
        try (DataSourceProvider dsp = getDataSourceProvider()) {
            SQLQuery query = new SQLQuery("SELECT mercator_quad(13.4050, 52.5200, 10);");
            Assertions.assertEquals("1202102332", query.run(dsp, rs -> rs.next() ? rs.getString(1) : null));
        }
    }

    @Test
    public void mercator_quad_point_to_lrc() throws Exception {
        try (DataSourceProvider dsp = getDataSourceProvider()) {
            SQLQuery query = new SQLQuery("SELECT colX, rowY, level FROM " +
                    "mercator_quad_crl(3.4050, 52.5200, 10);");
            Assertions.assertEquals("521,335,10", query.run(dsp, rs -> rs.next() ? rs.getInt(1)
                    + "," + rs.getInt(2) + "," + rs.getInt(3) : null));
        }
    }

    @Test
    public void mercator_quad_qk_to_lrc() throws Exception {
        try (DataSourceProvider dsp = getDataSourceProvider()) {
            SQLQuery query = new SQLQuery("SELECT colX, rowY, level FROM mercator_quad_crl(mercator_quad(335,550,10));");
            Assertions.assertEquals("335,550,10", query.run(dsp, rs -> rs.next() ? rs.getInt(1)
                    + "," + rs.getInt(2) + "," + rs.getInt(3) : null));
        }
    }

    @Test
    public void mercator_quad_to_bbox() throws Exception {
        try (DataSourceProvider dsp = getDataSourceProvider()) {
            SQLQuery query = new SQLQuery("SELECT ST_AsGeojson(mercator_quad_to_bbox(550,335,10));");
            Assertions.assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[13.359375,52.482780222]," +
                    "[13.359375,52.696361078],[13.7109375,52.696361078],[13.7109375,52.482780222],[13.359375,52.482780222]]]}",
                    query.run(dsp, rs -> rs.next() ? rs.getString(1) : null));
        }
    }

    @Test
    public void mercator_quad_qk_to_bbox() throws Exception {
        try (DataSourceProvider dsp = getDataSourceProvider()) {
            SQLQuery query = new SQLQuery("SELECT ST_AsGeojson(mercator_quad_to_bbox(mercator_quad(550,335,10)));");
            Assertions.assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[13.359375,52.482780222],[13.359375,52.696361078]," +
                    "[13.7109375,52.696361078],[13.7109375,52.482780222],[13.359375,52.482780222]]]}",
                    query.run(dsp, rs -> rs.next() ? rs.getString(1) : null));
        }
    }

    @Test
    public void mercator_quad_zoom_level_from_bbox() throws Exception {
        try (DataSourceProvider dsp = getDataSourceProvider()) {
            SQLQuery query = new SQLQuery("SELECT mercator_quad_zoom_level_from_bbox(" +
                    "ST_GeomFromText('LINESTRING(9.102719236998638 49.22677286363091,7.011126411595683 " +
                    "51.32741493065305)',4326));");
            Assertions.assertEquals(Integer.valueOf(7), query.run(dsp, rs -> rs.next() ? rs.getInt(1) : null));
        }
    }

    //HERE_QUAD TESTS Base10
    @Test
    public void here_quad_base10() throws Exception {
        try (DataSourceProvider dsp = getDataSourceProvider()) {
            SQLQuery query = new SQLQuery("SELECT here_quad(8800,6486,14);");
            Assertions.assertEquals("377894440", query.run(dsp, rs -> rs.next() ? rs.getString(1) : null));
        }
    }

    @Test
    public void here_quad_point_base10() throws Exception {
        try (DataSourceProvider dsp = getDataSourceProvider()) {
            SQLQuery query = new SQLQuery("SELECT here_quad(13.4050::FLOAT, 52.5200::FLOAT, 14);");
            Assertions.assertEquals("377894444", query.run(dsp, rs -> rs.next() ? rs.getString(1) : null));
        }
    }

    @Test
    public void here_quad_to_lrc_base10() throws Exception {
        try (DataSourceProvider dsp = getDataSourceProvider()) {
            SQLQuery query = new SQLQuery("SELECT rowY, colX, level, hkey FROM here_quad_crl('377894440');");
            Assertions.assertEquals("6486,8800,14,377894440", query.run(dsp,
                    rs -> rs.next() ? rs.getInt(1) + "," + rs.getInt(2) + "," + rs.getInt(3) + "," + rs.getLong(4) : null));
        }
    }

    @Test
    public void here_quad_to_bbox_base10() throws Exception {
        try (DataSourceProvider dsp = getDataSourceProvider()) {
            SQLQuery query = new SQLQuery("SELECT ST_AsGeojson(here_quad_to_bbox(550,405,10));");
            Assertions.assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[13.359375,52.3828125]," +
                    "[13.359375,52.734375],[13.7109375,52.734375],[13.7109375,52.3828125],[13.359375,52.3828125]]]}",
                    query.run(dsp, rs -> rs.next() ? rs.getString(1) : null));
        }
    }

    //HERE_QUAD TESTS Base4
    @Test
    public void here_quad_to_long_key_base4() throws Exception {
        try (DataSourceProvider dsp = getDataSourceProvider()) {
            SQLQuery query = new SQLQuery("SELECT here_quad_base4_to_base10('12201203120220');");
            Assertions.assertEquals("377894440", query.run(dsp, rs -> rs.next() ? rs.getString(1) : null));
        }
    }

    @Test
    public void here_quad_to_lrc_base4() throws Exception {
        try (DataSourceProvider dsp = getDataSourceProvider()) {
            SQLQuery query = new SQLQuery("SELECT colX, rowY, level, hkey FROM " +
                    "here_quad_crl('12201203120220', true);");
            Assertions.assertEquals("8800,6486,14,377894440",
                    query.run(dsp, rs -> rs.next() ? rs.getInt(1)
                            + "," + rs.getInt(2) + "," + rs.getInt(3) + "," + rs.getLong(4) : null));
        }
    }

    @Test
    public void here_quad_to_bbox_base4() throws Exception {
        try (DataSourceProvider dsp = getDataSourceProvider()) {
            SQLQuery query = new SQLQuery("SELECT ST_AsGeojson(here_quad_to_bbox('12201203120220',true));");
            Assertions.assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[13.359375,52.514648438]," +
                    "[13.359375,52.536621094],[13.381347656,52.536621094],[13.381347656,52.514648438]," +
                    "[13.359375,52.514648438]]]}",
                    query.run(dsp, rs -> rs.next() ? rs.getString(1) : null));
        }
    }

    //GENERIC QUAD FUNCTIONS
    @Test
    public void for_coordinate() throws Exception {
        try (DataSourceProvider dsp = getDataSourceProvider()) {
            SQLQuery query = new SQLQuery(
                    "SELECT 'DEFAULT' AS source, * FROM for_coordinate(3.4050, 52.5200, 10) A " +
                            "UNION ALL " +
                            "SELECT 'MERCATOR_QUAD' AS source, * FROM for_coordinate(3.4050, 52.5200, 10, 'MERCATOR_QUAD') B " +
                            "UNION ALL " +
                            "SELECT 'HERE_QUAD' AS source, * FROM for_coordinate(3.4050, 52.5200, 10, 'HERE_QUAD') C;"
            );
            query.run(dsp, rs -> {
                Assertions.assertTrue(rs.next());
                Assertions.assertEquals("DEFAULT", rs.getString(1));
                Assertions.assertEquals(521, rs.getInt(2));
                Assertions.assertEquals(335, rs.getInt(3));

                Assertions.assertTrue(rs.next());
                Assertions.assertEquals("MERCATOR_QUAD", rs.getString(1));
                Assertions.assertEquals(521, rs.getInt(2));
                Assertions.assertEquals(335, rs.getInt(3));

                Assertions.assertTrue(rs.next());
                Assertions.assertEquals("HERE_QUAD", rs.getString(1));
                Assertions.assertEquals(521, rs.getInt(2));
                Assertions.assertEquals(405, rs.getInt(3));

                return null;
            });
        }
    }

    @Test
    public void tile_to_bbox() throws Exception {
        try (DataSourceProvider dsp = getDataSourceProvider()) {
            SQLQuery query = new SQLQuery(
                    "SELECT 'DEFAULT' AS source, ST_AsGeojson(A.geometry) FROM tile_to_bbox(550,335,10) A " +
                            "UNION ALL " +
                            "SELECT 'MERCATOR_QUAD' AS source, ST_AsGeojson(B.geometry) FROM tile_to_bbox(550,335,10, 'MERCATOR_QUAD') B " +
                            "UNION ALL " +
                            "SELECT 'HERE_QUAD' AS source, ST_AsGeojson(C.geometry) FROM tile_to_bbox(550,405,10, 'HERE_QUAD') C;"
            );
            query.run(dsp, rs -> {
                Assertions.assertTrue(rs.next());
                Assertions.assertEquals("DEFAULT", rs.getString(1));
                Assertions.assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[13.359375,52.482780222],[13.359375,52.696361078],[13.7109375,52.696361078],[13.7109375,52.482780222],[13.359375,52.482780222]]]}", rs.getString(2));

                Assertions.assertTrue(rs.next());
                Assertions.assertEquals("MERCATOR_QUAD", rs.getString(1));
                Assertions.assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[13.359375,52.482780222],[13.359375,52.696361078],[13.7109375,52.696361078],[13.7109375,52.482780222],[13.359375,52.482780222]]]}", rs.getString(2));

                Assertions.assertTrue(rs.next());
                Assertions.assertEquals("HERE_QUAD", rs.getString(1));
                Assertions.assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[13.359375,52.3828125],[13.359375,52.734375],[13.7109375,52.734375],[13.7109375,52.3828125],[13.359375,52.3828125]]]}", rs.getString(2));

                return null;
            });
        }
    }
}
