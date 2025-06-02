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

import static com.here.xyz.models.hub.Ref.HEAD;

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CompositeExportStepTest extends ExportTestBase {
    /**
     * fcWithMixedGeometryTypes.geojson:
     * 11 features:
     *
     * 3 Points
     * 1 MultiPoint (2 Points)
     * 2 Lines
     * 2 MultiLine (2 Lines each)
     * 1 Polygon with hole
     * 1 Polygon without hole
     * 1 MultiPolygon (2 Polygons)
     *
     */
    private final String SPACE_ID_EXT = SPACE_ID + "_ext";

    @BeforeEach
    public void setUp() throws Exception {
        putFeatureCollectionToSpace(SPACE_ID, readTestFeatureCollection("/testFeatureCollections/fcWithMixedGeometryTypes.geojson"));

        //Create Composite Space
        createSpace(new Space().withId(SPACE_ID_EXT).withExtension(new Space.Extension().withSpaceId(SPACE_ID)), false);

        //Delete Feature with id foo_polygon
        deleteFeaturesInSpace(SPACE_ID_EXT, List.of("foo_polygon"));

        //Add two new Features
        FeatureCollection fc2 = XyzSerializable.deserialize("""
                {
                     "type": "FeatureCollection",
                     "features": [
                         {
                             "type": "Feature",
                             "id": "new_point1",
                             "properties": {},
                             "geometry": {
                                 "coordinates": [
                                     8.43,
                                     50.06
                                 ],
                                 "type": "Point"
                             }
                         },
                         {
                             "type": "Feature",
                             "id": "new_point2",
                             "properties": {},
                             "geometry": {
                                 "coordinates": [
                                     8.49,
                                     50.07
                                 ],
                                 "type": "Point"
                             }
                         }
                     ]
                 }
                """, FeatureCollection.class);
        putFeatureCollectionToSpace(SPACE_ID_EXT, fc2);
    }

    @AfterEach
    public void cleanup() throws SQLException {
        super.cleanup();
        deleteSpace(SPACE_ID_EXT);
    }

    @Test
    public void exportWithContextSuper() throws IOException, InterruptedException {
        executeExportStepAndCheckResults(SPACE_ID_EXT, SpaceContext.SUPER, null, null,
            new Ref(HEAD), "/search?context=SUPER");
    }

    @Test
    public void exportWithContextSuperAndWithPropertyFilter() throws Exception {
        //new_point does not exist in base
        exportWithContextAndWithPropertyFilter(SpaceContext.SUPER,
                URLEncoder.encode("f.id=\"new_point1\"", StandardCharsets.UTF_8));
    }

    @Test
    public void exportWithContextDefault() throws IOException, InterruptedException {
        executeExportStepAndCheckResults(SPACE_ID_EXT, SpaceContext.DEFAULT, null, null,
            new Ref(HEAD), "/search?context=DEFAULT");
    }

    @Test
    public void exportWithoutContext() throws IOException, InterruptedException {
        //Default context should get used
        executeExportStepAndCheckResults(SPACE_ID_EXT, null, null, null,
            new Ref(HEAD), "/search?context=DEFAULT");
    }

    @Test
    public void exportWithContextDefaultAndWithPropertyFilter() throws Exception {
        //new_point exists in composite compound
        exportWithContextAndWithPropertyFilter(SpaceContext.DEFAULT,
                URLEncoder.encode("f.id=\"new_point1\"", StandardCharsets.UTF_8));
    }

    @Test
    public void exportWithContextExtension() throws IOException, InterruptedException {
        executeExportStepAndCheckResults(SPACE_ID_EXT, SpaceContext.EXTENSION, null, null,
            new Ref(HEAD), "/search?context=EXTENSION");
    }

    @Test
    public void exportWithContextExtensionAndWithPropertyFilter() throws Exception {
        //new_point exists in extension
        exportWithContextAndWithPropertyFilter(SpaceContext.EXTENSION,
                URLEncoder.encode("f.id=\"new_point1\"", StandardCharsets.UTF_8));
    }

    private void exportWithContextAndWithPropertyFilter(SpaceContext context, String propertiesQuery) throws Exception {
        String hubQuery = "/search?context="+context+"&"+ propertiesQuery;

        executeExportStepAndCheckResults(SPACE_ID_EXT, context, null,
                PropertiesQuery.fromString(propertiesQuery), new Ref(HEAD), hubQuery);
    }
}
