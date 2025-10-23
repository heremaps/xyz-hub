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

package com.here.xyz.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class SQLGeoTileCalculationsFunctions extends SQLITBase {
  private static boolean DEBUG_MODE = false;

  @Test
  public void get_tiles_for_range() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery query = new SQLQuery("SELECT * FROM get_tiles_for_range(ROW(5,32,16,16," +
          "10,10,1,1)::map_quad_range);");
      query.run(dsp, rs -> {
        assertTrue(rs.next());
        assertEquals(16, rs.getInt(1));
        assertEquals(10, rs.getInt(2));
        assertEquals(5, rs.getInt(3));
        return null;
      });
    }
  }

  @Test
  public void get_tile_range_here_quad() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery query = new SQLQuery(
          "SELECT * FROM get_tile_range(ST_GeomFromText('LINESTRING(9.102719236998638 49.22677286363091," +
              "6.915544638457192 50.034583803271744,8.303202059707132 50.85309237628701,9.5950997450405 " +
              "51.29120801758333,8.156119246513924 51.69371318719752,7.011126411595683 51.32741493065305)'," +
              "4326), 11,'HERE_QUAD');"
      );
      query.run(dsp, rs -> {
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
        assertEquals(2048, rs.getInt(2));
        assertEquals(1063, rs.getInt(3));
        assertEquals(1078, rs.getInt(4));
        assertEquals(792, rs.getInt(5));
        assertEquals(806, rs.getInt(6));
        assertEquals(16, rs.getInt(7));
        assertEquals(15, rs.getInt(8));
        return null;
      });
    }
  }

  @Test
  public void get_tile_range_mercator_quad() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery query = new SQLQuery(
          "SELECT * FROM get_tile_range(ST_GeomFromText('LINESTRING(9.102719236998638 49.22677286363091," +
              "6.915544638457192 50.034583803271744,8.303202059707132 50.85309237628701," +
              "9.5950997450405 51.29120801758333,8.156119246513924 51.69371318719752,7.011126411595683 " +
              "51.32741493065305)',4326), 11,'MERCATOR_QUAD');"
      );
      query.run(dsp, rs -> {
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
        assertEquals(2048, rs.getInt(2));
        assertEquals(1063, rs.getInt(3));
        assertEquals(1078, rs.getInt(4));
        assertEquals(679, rs.getInt(5));
        assertEquals(701, rs.getInt(6));
        assertEquals(16, rs.getInt(7));
        assertEquals(23, rs.getInt(8));
        return null;
      });
    }
  }

  @Test
  public void get_bounding_box_for_range_here_quad() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery query = new SQLQuery(
          "SELECT ST_AsGeojson(get_bounding_box_for_range(1063,1078,792,806,11,'HERE_QUAD'));"
      );
      query.run(dsp, rs -> {
        assertTrue(rs.next());
        assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[6.85546875,49.21875]," +
            "[6.85546875,51.85546875],[9.66796875,51.85546875],[9.66796875,49.21875]," +
            "[6.85546875,49.21875]]]}", rs.getString(1));
        return null;
      });
    }
  }

  @Test
  public void get_bounding_box_for_range_mercator_quad() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery query = new SQLQuery(
          "SELECT ST_AsGeojson(get_bounding_box_for_range(1063,1078,792,806,11,'MERCATOR_QUAD'));"
      );
      query.run(dsp, rs -> {
        assertTrue(rs.next());
        assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[6.85546875,35.603718741]," +
            "[6.85546875,37.718590326],[9.66796875,37.718590326],[9.66796875,35.603718741]," +
            "[6.85546875,35.603718741]]]}", rs.getString(1));
        return null;
      });
    }
  }

  @Test
  public void for_geometry_here_quad_level_6() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery query = new SQLQuery(
          "SELECT here_quad(colX,rowY,level),colX,rowY,level " +
              "FROM for_geometry(ST_GeomFromText('LINESTRING(9.102719236998638 49.22677286363091," +
              "6.915544638457192 50.034583803271744,8.303202059707132 50.85309237628701,9.5950997450405 " +
              "51.29120801758333,8.156119246513924 51.69371318719752,7.011126411595683 51.32741493065305)'" +
              ",4326), 6, 'HERE_QUAD');"
      );
      query.run(dsp, rs -> {
        assertTrue(rs.next());
        assertEquals(5761, rs.getInt(1));
        assertEquals(33, rs.getInt(2));
        assertEquals(24, rs.getInt(3));
        assertEquals(6, rs.getInt(4));

        assertTrue(rs.next());
        assertEquals(5763, rs.getInt(1));
        assertEquals(33, rs.getInt(2));
        assertEquals(25, rs.getInt(3));
        assertEquals(6, rs.getInt(4));

        return null;
      });
    }
  }

  @Test
  public void for_geometry_here_quad_level_11() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery query = new SQLQuery(
          "SELECT here_quad(colX,rowY,level),colX,rowY,level " +
              "FROM for_geometry(ST_GeomFromText('LINESTRING(9.102719236998638 49.22677286363091," +
              "6.915544638457192 50.034583803271744,8.303202059707132 50.85309237628701,9.5950997450405 " +
              "51.29120801758333,8.156119246513924 51.69371318719752,7.011126411595683 51.32741493065305)'" +
              ",4326), 11, 'HERE_QUAD');"
      );
      query.run(dsp, rs -> {
        List<int[]> expectedResults = List.of(
            new int[]{5899979, 1065, 795, 11},
            new int[]{5899982, 1066, 795, 11},
            new int[]{5899990, 1070, 793, 11},
            new int[]{5899981, 1067, 794, 11},
            new int[]{5899992, 1068, 794, 11},
            new int[]{5899993, 1069, 794, 11},
            new int[]{5899996, 1070, 794, 11},
            new int[]{5899983, 1067, 795, 11},
            new int[]{5900161, 1073, 792, 11},
            new int[]{5900164, 1074, 792, 11},
            new int[]{5899991, 1071, 793, 11},
            new int[]{5900162, 1072, 793, 11},
            new int[]{5900163, 1073, 793, 11},
            new int[]{5900165, 1075, 792, 11},
            new int[]{5899957, 1063, 796, 11},
            new int[]{5900000, 1064, 796, 11},
            new int[]{5900001, 1065, 796, 11},
            new int[]{5899959, 1063, 797, 11},
            new int[]{5900002, 1064, 797, 11},
            new int[]{5900003, 1065, 797, 11},
            new int[]{5900009, 1065, 798, 11},
            new int[]{5900012, 1066, 798, 11},
            new int[]{5900013, 1067, 798, 11},
            new int[]{5900015, 1067, 799, 11},
            new int[]{5900026, 1068, 799, 11},
            new int[]{5900027, 1069, 799, 11},
            new int[]{5901343, 1063, 803, 11},
            new int[]{5901393, 1069, 800, 11},
            new int[]{5901396, 1070, 800, 11},
            new int[]{5901398, 1070, 801, 11},
            new int[]{5901399, 1071, 801, 11},
            new int[]{5901570, 1072, 801, 11},
            new int[]{5901571, 1073, 801, 11},
            new int[]{5901577, 1073, 802, 11},
            new int[]{5901580, 1074, 802, 11},
            new int[]{5901581, 1075, 802, 11},
            new int[]{5901592, 1076, 802, 11},
            new int[]{5901594, 1076, 803, 11},
            new int[]{5901595, 1077, 803, 11},
            new int[]{5901598, 1078, 803, 11},
            new int[]{5901365, 1063, 804, 11},
            new int[]{5901408, 1064, 804, 11},
            new int[]{5901409, 1065, 804, 11},
            new int[]{5901412, 1066, 804, 11},
            new int[]{5901413, 1067, 804, 11},
            new int[]{5901415, 1067, 805, 11},
            new int[]{5901426, 1068, 805, 11},
            new int[]{5901427, 1069, 805, 11},
            new int[]{5901430, 1070, 805, 11},
            new int[]{5901436, 1070, 806, 11},
            new int[]{5901604, 1074, 804, 11},
            new int[]{5901431, 1071, 805, 11},
            new int[]{5901602, 1072, 805, 11},
            new int[]{5901603, 1073, 805, 11},
            new int[]{5901606, 1074, 805, 11},
            new int[]{5901605, 1075, 804, 11},
            new int[]{5901616, 1076, 804, 11},
            new int[]{5901617, 1077, 804, 11}
        );
        int i = 0;
        while (rs.next()) {
          assertEquals(expectedResults.get(i)[0], rs.getInt(1));
          assertEquals(expectedResults.get(i)[1], rs.getInt(2));
          assertEquals(expectedResults.get(i)[2], rs.getInt(3));
          assertEquals(expectedResults.get(i)[3], rs.getInt(4));
          i++;
        }
        return null;
      });
    }
  }

  @Test
  public void for_geometry_mercator_quad_level_6() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery query = new SQLQuery(
          "SELECT mercator_quad(colX,rowY,level),colX,rowY,level " +
              "FROM for_geometry(ST_GeomFromText('LINESTRING(9.102719236998638 49.22677286363091,6.915544638457192 " +
              "50.034583803271744,8.303202059707132 50.85309237628701,9.5950997450405 51.29120801758333,8.156119246513924 " +
              "51.69371318719752,7.011126411595683 51.32741493065305)',4326), 6);"
      );
      query.run(dsp, rs -> {
        rs.next();
        assertEquals("120203", rs.getString(1));
        assertEquals(33, rs.getInt(2));
        assertEquals(21, rs.getInt(3));
        assertEquals(6, rs.getInt(4));

        return null;
      });
    }
  }

  @Test
  public void for_geometry_mercator_quad_level_11() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery query = new SQLQuery(
          "SELECT mercator_quad(colX,rowY,level),colX,rowY,level " +
              "FROM for_geometry(ST_GeomFromText('LINESTRING(9.102719236998638 49.22677286363091,6.915544638457192 " +
              "50.034583803271744,8.303202059707132 50.85309237628701,9.5950997450405 51.29120801758333,8.156119246513924 " +
              "51.69371318719752,7.011126411595683 51.32741493065305)',4326), 11);"
      );
      query.run(dsp, rs -> {
        List<Object[]> expectedResults = List.of(
            new Object[]{"12020303003", 1065, 681, 11},
            new Object[]{"12020303012", 1066, 681, 11},
            new Object[]{"12020302131", 1063, 682, 11},
            new Object[]{"12020303020", 1064, 682, 11},
            new Object[]{"12020303021", 1065, 682, 11},
            new Object[]{"12020301323", 1069, 679, 11},
            new Object[]{"12020301332", 1070, 679, 11},
            new Object[]{"12020303011", 1067, 680, 11},
            new Object[]{"12020303100", 1068, 680, 11},
            new Object[]{"12020303101", 1069, 680, 11},
            new Object[]{"12020303013", 1067, 681, 11},
            new Object[]{"12020301333", 1071, 679, 11},
            new Object[]{"12020303111", 1071, 680, 11},
            new Object[]{"12020312000", 1072, 680, 11},
            new Object[]{"12020312001", 1073, 680, 11},
            new Object[]{"12020312010", 1074, 680, 11},
            new Object[]{"12020312012", 1074, 681, 11},
            new Object[]{"12020312013", 1075, 681, 11},
            new Object[]{"12020312102", 1076, 681, 11},
            new Object[]{"12020312120", 1076, 682, 11},
            new Object[]{"12020312121", 1077, 682, 11},
            new Object[]{"12020312130", 1078, 682, 11},
            new Object[]{"12020312210", 1074, 684, 11},
            new Object[]{"12020312203", 1073, 685, 11},
            new Object[]{"12020312212", 1074, 685, 11},
            new Object[]{"12020303331", 1071, 686, 11},
            new Object[]{"12020312220", 1072, 686, 11},
            new Object[]{"12020312221", 1073, 686, 11},
            new Object[]{"12020312122", 1076, 683, 11},
            new Object[]{"12020312123", 1077, 683, 11},
            new Object[]{"12020312132", 1078, 683, 11},
            new Object[]{"12020312211", 1075, 684, 11},
            new Object[]{"12020312300", 1076, 684, 11},
            new Object[]{"12020321030", 1066, 690, 11},
            new Object[]{"12020303332", 1070, 687, 11},
            new Object[]{"12020321101", 1069, 688, 11},
            new Object[]{"12020321110", 1070, 688, 11},
            new Object[]{"12020321013", 1067, 689, 11},
            new Object[]{"12020321102", 1068, 689, 11},
            new Object[]{"12020321103", 1069, 689, 11},
            new Object[]{"12020321031", 1067, 690, 11},
            new Object[]{"12020303333", 1071, 687, 11},
            new Object[]{"12020321023", 1065, 691, 11},
            new Object[]{"12020321032", 1066, 691, 11},
            new Object[]{"12020321200", 1064, 692, 11},
            new Object[]{"12020321201", 1065, 692, 11},
            new Object[]{"12020320313", 1063, 693, 11},
            new Object[]{"12020321202", 1064, 693, 11},
            new Object[]{"12020320331", 1063, 694, 11},
            new Object[]{"12020321220", 1064, 694, 11},
            new Object[]{"12020321222", 1064, 695, 11},
            new Object[]{"12020321223", 1065, 695, 11},
            new Object[]{"12020321232", 1066, 695, 11},
            new Object[]{"12020323010", 1066, 696, 11},
            new Object[]{"12020323011", 1067, 696, 11},
            new Object[]{"12020323100", 1068, 696, 11},
            new Object[]{"12020323102", 1068, 697, 11},
            new Object[]{"12020323103", 1069, 697, 11},
            new Object[]{"12020323121", 1069, 698, 11},
            new Object[]{"12020323130", 1070, 698, 11},
            new Object[]{"12020323131", 1071, 698, 11},
            new Object[]{"12020323133", 1071, 699, 11},
            new Object[]{"12020332022", 1072, 699, 11},
            new Object[]{"12020332023", 1073, 699, 11},
            new Object[]{"12020332201", 1073, 700, 11},
            new Object[]{"12020332210", 1074, 700, 11},
            new Object[]{"12020332211", 1075, 700, 11},
            new Object[]{"12020332213", 1075, 701, 11}
        );
        int i = 0;
        while (rs.next()) {
          assertEquals(expectedResults.get(i)[0], rs.getString(1));
          assertEquals(expectedResults.get(i)[1], rs.getInt(2));
          assertEquals(expectedResults.get(i)[2], rs.getInt(3));
          assertEquals(expectedResults.get(i)[3], rs.getInt(4));
          i++;
        }
        return null;
      });
    }
  }

  private static Stream<Arguments> testSetup() {
    return Stream.of(
        //MERCATOR_QUAD
        Arguments.of("POINT(13.4050 52.5200)", 6, "MERCATOR_QUAD" ,
            List.of("120210")),
        Arguments.of("MULTIPOINT((10 40), (40 30), (20 20), (30 10))", 6, "MERCATOR_QUAD",
            List.of("122211", "122001", "122321", "122131")),
        Arguments.of("LINESTRING(30 10, 10 30, 40 40)", 6, "MERCATOR_QUAD",
            List.of("122012", "122013", "122102", "122021", "122030", "122032", "122033", "122101", "122110", "122111", "122103", "122211", "122300", "122302", "122303", "122321")),
        Arguments.of("MULTILINESTRING((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))", 6, "MERCATOR_QUAD",
            List.of("122001", "122010", "122012", "122030", "122032", "122033", "122110", "122111", "122103", "122112", "122121", "122123", "122132", "122210", "122211", "122203", "122212", "122221", "122310", "122311", "122303", "122312", "122321")),
        Arguments.of("POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))", 6, "MERCATOR_QUAD",
            List.of("122011", "122100", "122012", "122013", "122102", "122030", "122031", "122120", "122023", "122032", "122033", "122122", "122101", "122110", "122111", "122103", "122112", "122121", "122130", "122123", "122132", "122201", "122210", "122211", "122300", "122212", "122213", "122302", "122320", "122301", "122310", "122303", "122321")),
        //Polygon with hole
        Arguments.of("POLYGON((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))", 6, "MERCATOR_QUAD",
            List.of("120233", "120322", "122010", "122011", "122100", "122012", "122013", "122102", "122030", "122031", "122120", "120323", "120332", "120333", "122101", "122110", "122111", "122103", "122112", "122113", "122121", "122130", "122131", "122023", "122032", "122033", "122122", "122201", "122210", "122211", "122300", "122213", "122302", "122123", "122132", "122133", "122301", "122310", "122303", "122312", "122321", "122330")),
        Arguments.of("MULTIPOLYGON(((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))", 6, "MERCATOR_QUAD",
            List.of("122001", "122010", "122011", "122012", "122013", "122031", "122100", "122101", "122110", "122111", "122102", "122103", "122112", "122113", "122120", "122121", "122130", "122122", "122123", "122132", "122201", "122210", "122211", "122202", "122203", "122212", "122213", "122220", "122221", "122230", "122231", "122232", "122233", "122300", "122301", "122302", "122303", "122312", "122320", "122321", "122330", "122331")),
        //1 Polygon with hole
        Arguments.of("MULTIPOLYGON(((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))", 6, "MERCATOR_QUAD",
            List.of("120233", "120322", "122100", "122012", "122013", "122102", "122021", "122030", "122031", "122120", "120323", "120332", "122101", "122110", "122111", "122112", "122113", "122121", "122131", "122023", "122032", "122033", "122122", "122201", "122210", "122211", "122300", "122203", "122212", "122213", "122302", "122221", "122230", "122231", "122320", "122123", "122132", "122133", "122301", "122310", "122311", "122303", "122312", "122313", "122321", "122330", "122322", "122323")),

        //HERE_QUAD
        Arguments.of("POINT(13.4050 52.5200)", 6, "HERE_QUAD", List.of(5766)),
        Arguments.of("MULTIPOINT((10 40), (40 30), (20 20), (30 10))", 6, "HERE_QUAD",
            List.of(5647, 5675, 5651, 5687)),
        Arguments.of("LINESTRING(30 10, 10 30, 40 40)", 6, "HERE_QUAD",
            List.of(5656, 5647, 5658, 5668, 5669, 5651, 5657, 5667, 5670, 5671, 5677, 5688, 5689, 5692, 5694, 5695)),
        Arguments.of("MULTILINESTRING((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))", 6, "HERE_QUAD",
            List.of(5635, 5638, 5641, 5644, 5645, 5646, 5647, 5668, 5669, 5651, 5657, 5660, 5662, 5663, 5681, 5684, 5670, 5673, 5676, 5675, 5683, 5686, 5689, 5692, 5693, 5694, 5695)),
        Arguments.of("POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))", 6, "HERE_QUAD",
            List.of(5650, 5644, 5645, 5656, 5643, 5646, 5647, 5658, 5665, 5668, 5669, 5680, 5651, 5657, 5659, 5662, 5681, 5684, 5670, 5671, 5682, 5676, 5677, 5688, 5679, 5690, 5683, 5686, 5689, 5692, 5693, 5691, 5694, 5695)),
        //Polygon with hole
        Arguments.of("POLYGON((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))", 6, "HERE_QUAD",
            List.of(5645, 5656, 5643, 5646, 5647, 5658, 5665, 5668, 5669, 5680, 5651, 5654, 5657, 5660, 5659, 5662, 5681, 5684, 5685, 5670, 5671, 5682, 5676, 5677, 5688, 5678, 5679, 5690, 5683, 5686, 5687, 5689, 5692, 5693, 5691, 5694, 5695, 5738, 5781, 5824)),
        Arguments.of("MULTIPOLYGON(((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))", 6, "HERE_QUAD",
            List.of(5636, 5637, 5634, 5635, 5638, 5639, 5640, 5641, 5644, 5645, 5643, 5646, 5647, 5650, 5651, 5654, 5655, 5656, 5657, 5660, 5658, 5659, 5669, 5670, 5671, 5673, 5676, 5677, 5675, 5678, 5679, 5680, 5681, 5684, 5682, 5683, 5686, 5687, 5688, 5689, 5692, 5693, 5690, 5691, 5694, 5695, 5738)),
        //1 Polygon with hole
        Arguments.of("MULTIPOLYGON(((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))", 6, "HERE_QUAD",
            List.of(5648, 5635, 5638, 5639, 5650, 5641, 5644, 5645, 5656, 5643, 5646, 5647, 5658, 5649, 5651, 5654, 5657, 5660, 5661, 5659, 5662, 5663, 5706, 5665, 5668, 5669, 5680, 5667, 5670, 5671, 5682, 5677, 5679, 5690, 5681, 5684, 5685, 5683, 5686, 5687, 5730, 5689, 5692, 5693, 5691, 5694, 5695, 5765))
    );
  }

  @ParameterizedTest
  @MethodSource("testSetup")
  public void forGeometryTests(String geometry, int zoomLevel, String quadType, List<Object> expectedResults) throws Exception {
    List<Object> tiles = buildForGeometryQuery(geometry, zoomLevel, quadType);
    if(DEBUG_MODE){
      printFcForVisualization(geometry, zoomLevel, quadType);
      return;
    }

    if(expectedResults.get(0) instanceof Integer){
      List<Long> expectedResultsAsLong = expectedResults.stream()
          .map(o -> Long.valueOf((Integer) o))
          .toList();
      assertTrue(tiles.containsAll(expectedResultsAsLong));
    }else
      assertTrue(tiles.containsAll(expectedResults ));
  }

  public List<Object> buildForGeometryQuery(String geometryInput, int targetLevel, String quadType) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      String quadFunction = quadType.equals("HERE_QUAD") ? "here_quad" : "mercator_quad" ;

      SQLQuery query = new SQLQuery(
          "SELECT array_agg("+quadFunction+") FROM( SELECT "    + quadFunction
              + "(colX,rowY,level) FROM for_geometry(ST_GeomFromText(#{geometryInput}, 4326)," +
              " #{targetLevel}, #{quadType}))A;"
      ).withNamedParameter("geometryInput", geometryInput)
          .withNamedParameter("targetLevel", targetLevel)
          .withNamedParameter("quadType", quadType);
      return query.run(dsp, rs -> {rs.next(); return Arrays.asList( (Object[]) rs.getArray(1).getArray());});
    }
  }

  public void printFcForVisualization(String geometryInput, int targetLevel, String quadType) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      String quadToBBOXFunction = quadType.equals("HERE_QUAD") ? "here_quad_to_bbox" : "mercator_quad_to_bbox";

      SQLQuery query = new SQLQuery(
          """
              SELECT jsonb_build_object(
                  'type', 'FeatureCollection',
                  'features', jsonb_agg(feature) || jsonb_build_array(
                      jsonb_build_object(
                          'type', 'Feature',
                          'properties', jsonb_build_object(),
                          'geometry', ST_AsGeoJSON(ST_GeomFromText(#{geometryInput}, 4326))::jsonb
                      )
                  )
              )
              FROM (
                  SELECT jsonb_build_object(
                      'type', 'Feature',
                      'properties', jsonb_build_object(),
                      'geometry', ST_AsGeoJSON(
              """
              + quadToBBOXFunction +
              """
                  (colX,rowY,level))::jsonb
                  ) AS feature
                  FROM for_geometry(ST_GeomFromText(#{geometryInput}, 4326), #{targetLevel}, #{quadType})
              ) A;
              """
      ).withNamedParameter("geometryInput", geometryInput)
          .withNamedParameter("targetLevel", targetLevel)
          .withNamedParameter("quadType", quadType);

      query.run(dsp, rs -> {
        rs.next();
        System.out.println(geometryInput + " / " + targetLevel + " / " + quadType + " =>");
        System.out.println(rs.getString(1) + "\n");
        return null;
      });
    }
  }
}
