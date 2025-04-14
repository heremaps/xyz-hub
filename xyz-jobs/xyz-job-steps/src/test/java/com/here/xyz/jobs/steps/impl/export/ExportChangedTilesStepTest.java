/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.jobs.steps.impl.export;

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.jobs.steps.impl.transport.ExportChangedTiles.QuadType;
import com.here.xyz.models.geojson.coordinates.LinearRingCoordinates;
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Polygon;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

@Disabled
public class ExportChangedTilesStepTest extends ExportTestBase {
    private final static int VERSIONS_TO_KEEP = 100;
    private final String SPACE_ID_EXT = SPACE_ID + "_ext";

    @BeforeEach
    public void setUp() throws Exception {
        createSpace(new Space().withId(SPACE_ID).withVersionsToKeep(VERSIONS_TO_KEEP) , false);

        FeatureCollection fcBase = XyzSerializable.deserialize("""
                {
                     "type": "FeatureCollection",
                     "features": [
                         {
                             "type": "Feature",
                             "id": "point1_base",
                             "properties": {
                                "value" : "Ireland"
                             },
                             "geometry": {
                                 "coordinates": [
                                    -8.302116,
                                     53.34262
                                 ],
                                 "type": "Point"
                             }
                         },
                        {
                          "type": "Feature",
                          "id": "point2_base",
                          "properties": {
                             "value" : "France"
                          },
                          "geometry": {
                            "coordinates": [
                              2.466275756203231,
                              47.12080977623893
                            ],
                            "type": "Point"
                          }
                        }
                     ]
                 }
                """, FeatureCollection.class);

        putFeatureCollectionToSpace(SPACE_ID, fcBase);
        //Create Composite Space
        createSpace(new Space().withId(SPACE_ID_EXT).withExtension(new Space.Extension().withSpaceId(SPACE_ID)).withVersionsToKeep(VERSIONS_TO_KEEP), false);

        //TODO: Do not create FeatureCollections out of a String, create them using the Model instead
        FeatureCollection fc1 = XyzSerializable.deserialize("""
                {
                     "type": "FeatureCollection",
                     "features": [
                         {
                             "type": "Feature",
                             "id": "point1_delta",
                             "properties": {
                                "value" : "France"
                             },
                             "geometry": {
                                 "coordinates": [
                                    2.5924,
                                    47.0581
                                 ],
                                 "type": "Point"
                             }
                         },
                          {
                             "type": "Feature",
                             "id": "point2_delta",
                             "properties": {
                                "value" : "Russia"
                             },
                             "geometry": {
                                 "coordinates": [
                                      34.89167,
                                      39.68783
                                 ],
                                 "type": "Point"
                             }
                         }
                     ]
                 }
                """, FeatureCollection.class);

        FeatureCollection fc2 = XyzSerializable.deserialize("""
                {
                     "type": "FeatureCollection",
                     "features": [
                         {
                             "type": "Feature",
                             "id": "point1_delta",
                             "properties": {
                                "value" : "Norway"
                             },
                             "geometry": {
                                 "coordinates": [
                                     7.59619,
                                     59.5372
                                 ],
                                 "type": "Point"
                             }
                         },
                         {
                             "type": "Feature",
                             "id": "point3_delta",
                             "properties": {
                                "value" : "Africa"
                             },
                             "geometry": {
                                 "coordinates": [
                                      0.532815,
                                      0.459387
                                 ],
                                 "type": "Point"
                             }
                         }
                     ]
                 }
                """, FeatureCollection.class);

        //=> LINESTRING(6.353809489694754 51.08958733812065, 8.231999783047115 51.359089906708846, 7.937709200611209 50.5346535571293, 9.337868867777473 50.26303968016663, 9.34797694869323 49.3892593362186)
        FeatureCollection fc3 = XyzSerializable.deserialize("""
                {
                  "type": "FeatureCollection",
                  "features": [
                    {
                      "type": "Feature",
                      "id": "line4_delta",
                      "properties": {
                        "value" : "Germany"
                      },
                      "geometry": {
                        "coordinates": [
                          [
                            6.353809489694754,
                            51.08958733812065
                          ],
                          [
                            8.231999783047115,
                            51.359089906708846
                          ],
                          [
                            7.937709200611209,
                            50.5346535571293
                          ],
                          [
                            9.337868867777473,
                            50.26303968016663
                          ],
                          [
                            9.34797694869323,
                            49.3892593362186
                          ]
                        ],
                        "type": "LineString"
                      }
                    }
                  ]
                }
                """, FeatureCollection.class);

        //=>Add two new Features (point1_delta, point2_delta) : Version 1
        putFeatureCollectionToSpace(SPACE_ID_EXT, fc1);

        //=>Add point3_delta and modify (+move) point1_delta : Version 2
        putFeatureCollectionToSpace(SPACE_ID_EXT, fc2);

        //=>Delete point1_base  : Version 3
        deleteFeaturesInSpace(SPACE_ID_EXT, List.of("point1_base"));

        //=>Delete point2_delta : Version 4
        deleteFeaturesInSpace(SPACE_ID_EXT, List.of("point2_delta"));

        //=>Add line1 : Version 5
        putFeatureCollectionToSpace(SPACE_ID_EXT, fc3);
    }

    @AfterEach
    public void cleanup() throws SQLException {
        super.cleanup();
        deleteSpace(SPACE_ID_EXT);
    }

    @Test
    public void Export_Version0to1() throws IOException, InterruptedException {
        executeExportChangedTilesStepAndCheckResults(SPACE_ID_EXT, 5, QuadType.HERE_QUAD,
                new Ref("0..1") , List.of(), new FeatureCollection().withFeatures(
                        List.of(new Feature().withId("point1_delta"),new Feature().withId("point2_delta"),
                            new Feature().withId("point2_base") //because it is in the same tile as point1_delta
                        )
                ));
    }

    @Test
    public void Export_Version0to4() throws IOException, InterruptedException {
        executeExportChangedTilesStepAndCheckResults(SPACE_ID_EXT, 5, QuadType.HERE_QUAD,
                new Ref("0..4") , List.of("1269"), new FeatureCollection().withFeatures(
                        List.of(new Feature().withId("point1_delta"),new Feature().withId("point3_delta"))
                ));
    }

    @Test
    public void Export_Version1to2() throws IOException, InterruptedException {
        executeExportChangedTilesStepAndCheckResults(SPACE_ID_EXT, 5, QuadType.HERE_QUAD,
                new Ref("1..2") , List.of(), //1440 is missing, because point2_base remains in it
                        new FeatureCollection().withFeatures(
                        List.of(new Feature().withId("point1_delta"), new Feature().withId("point3_delta"),
                                new Feature().withId("point2_base"))
                ));
    }

    @Test
    public void Export_Version2to4() throws IOException, InterruptedException {
        executeExportChangedTilesStepAndCheckResults(SPACE_ID_EXT, 5, QuadType.HERE_QUAD,
                new Ref("2..4") , List.of("1269", "1423"), new FeatureCollection().withFeatures(
                        List.of() //only deletions have happened
                ));
    }

    @Test
    public void Export_Version0toHEAD() throws IOException, InterruptedException {
        executeExportChangedTilesStepAndCheckResults(SPACE_ID_EXT, 5, QuadType.HERE_QUAD,
                new Ref("0..HEAD") , List.of("1269"), new FeatureCollection().withFeatures(
                        List.of(
                                new Feature().withId("point1_delta"),
                                new Feature().withId("point3_delta"),
                                new Feature().withId("point2_base"),
                                new Feature().withId("line4_delta")
                        )
                ));
    }

    @Test
    public void Export_Version2to4WithPropertyFilter() throws IOException, InterruptedException {
        executeExportChangedTilesStepAndCheckResults(SPACE_ID_EXT, 5, QuadType.HERE_QUAD,
                new Ref("1..2") ,null, PropertiesQuery.fromString("p.value=Africa"),
                List.of("1269", "1423"), new FeatureCollection().withFeatures(
                        List.of(
                                new Feature().withId("point3_delta")
                        )
                ));
    }

    @Test
    public void Export_Version0toHEADWithSpatialFilterClipped() throws IOException, InterruptedException, InvalidGeometryException {

        PolygonCoordinates polygonCoordinates = new PolygonCoordinates();
        LinearRingCoordinates lrc = new LinearRingCoordinates();

        // Define the polygon coordinates from the new GeoJSON
        lrc.add(new Position(6.862454740359112, 51.266833510249285));
        lrc.add(new Position(6.862454740359112, 50.94927282395278));
        lrc.add(new Position(8.809774102827362, 50.94927282395278));
        lrc.add(new Position(8.809774102827362, 51.266833510249285));
        lrc.add(new Position(6.862454740359112, 51.266833510249285)); // Closing the ring

        polygonCoordinates.add(lrc);

        executeExportChangedTilesStepAndCheckResults(SPACE_ID_EXT, 8, QuadType.HERE_QUAD,
                new Ref("0..HEAD") ,
                new SpatialFilter()
                        .withGeometry(new Polygon().withCoordinates(polygonCoordinates))
                        .withClip(true), null,
                List.of("1269"), new FeatureCollection().withFeatures(
                        //spatialFilter crosses two tiles
                        List.of(
                                new Feature().withId("line4_delta"),
                                new Feature().withId("line4_delta")
                        )
                ));
    }

    @Test
    public void Export_SpatialFilterWithoutHittingChanges() throws IOException, InterruptedException, InvalidGeometryException {

        PolygonCoordinates polygonCoordinates = new PolygonCoordinates();
        LinearRingCoordinates lrc = new LinearRingCoordinates();

        // Define the polygon coordinates from the new GeoJSON
        lrc.add(new Position(6.862454740359112, 51.266833510249285));
        lrc.add(new Position(6.862454740359112, 50.94927282395278));
        lrc.add(new Position(8.809774102827362, 50.94927282395278));
        lrc.add(new Position(8.809774102827362, 51.266833510249285));
        lrc.add(new Position(6.862454740359112, 51.266833510249285)); // Closing the ring

        polygonCoordinates.add(lrc);

        executeExportChangedTilesStepAndCheckResults(SPACE_ID_EXT, 8, QuadType.HERE_QUAD,
                new Ref("0..HEAD") ,
                new SpatialFilter()
                        .withGeometry(new Polygon().withCoordinates(polygonCoordinates))
                        .withClip(false), null,
                List.of("1269"), new FeatureCollection().withFeatures(
                        //spatialFilter crosses two tiles
                        List.of(
                                new Feature().withId("line4_delta"),
                                new Feature().withId("line4_delta"),
                                new Feature().withId("line4_delta"),
                                new Feature().withId("line4_delta")
                        )
                ));
    }

    @Test
    public void ExportChangedTilesStepVersion0toHEADWithSpatialFilterNotClipped() throws IOException, InterruptedException, InvalidGeometryException {
        PolygonCoordinates polygonCoordinates = new PolygonCoordinates();
        LinearRingCoordinates lrc = new LinearRingCoordinates();

        //No change is inside this polygon
        lrc.add(new Position(46.45782151089023, 11.866315363309141));
        lrc.add(new Position(46.45782151089023, 8.970767248921192));
        lrc.add(new Position(51.31537254401951, 8.970767248921192));
        lrc.add(new Position(51.31537254401951, 11.866315363309141));
        lrc.add(new Position(46.45782151089023, 11.866315363309141)); // Closing the ring

        polygonCoordinates.add(lrc);

        executeExportChangedTilesStepAndCheckResults(SPACE_ID_EXT, 8, QuadType.HERE_QUAD,
                new Ref("0..HEAD") ,
                new SpatialFilter()
                        .withGeometry(new Polygon().withCoordinates(polygonCoordinates))
                        .withClip(false), null,
                List.of(), new FeatureCollection());
    }
}