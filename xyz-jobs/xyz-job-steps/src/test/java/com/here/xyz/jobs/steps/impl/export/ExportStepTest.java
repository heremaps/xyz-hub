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

import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Point;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class ExportStepTest extends ExportTestBase {
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


    @BeforeEach
    public void setUp() throws Exception {
        putFeatureCollectionToSpace(SPACE_ID, readTestFeatureCollection("/testFeatureCollections/fcWithMixedGeometryTypes.geojson"));
    }

    @Test
    public void exportUnfiltered() throws Exception {
        executeExportStepAndCheckResults(SPACE_ID, null, null, null, null,
                "/search");
    }

    @Test
    public void exportWithPropertyFilter() throws Exception {
        String propertiesQuery = URLEncoder.encode("p.description=\"Point\"", StandardCharsets.UTF_8);
        executeExportStepAndCheckResults(SPACE_ID, null, null,
                PropertiesQuery.fromString(propertiesQuery), null, "/search?" + propertiesQuery);
    }

    @Test
    public void exportStepWithSpatialFilter() throws Exception {
        SpatialFilter spatialFilter = new SpatialFilter()
                .withGeometry(
                        new Point().withCoordinates(new PointCoordinates(8.6709594,50.102964))
                )
                .withRadius(5500)
                .withClip(true);

        executeExportStepAndCheckResults(SPACE_ID, null, spatialFilter,  null, null,
                "spatial?lat=50.102964&lon=8.6709594&clip=true&radius=5500");
    }

    @Test
    public void exportWithSpatialAndPropertyFilter() throws Exception {
        String propertiesQuery = URLEncoder.encode("p.description=\"Point\"", StandardCharsets.UTF_8);
        String hubQuery = "spatial?lat=50.102964&lon=8.6709594&clip=true&radius=55000&"
            + propertiesQuery;

        SpatialFilter spatialFilter = new SpatialFilter()
                .withGeometry(
                        new Point().withCoordinates(new PointCoordinates(8.6709594,50.102964))
                )
                .withRadius(55000)
                .withClip(true);

        executeExportStepAndCheckResults(SPACE_ID, null, spatialFilter,
                PropertiesQuery.fromString(propertiesQuery), null, hubQuery);
    }
}
