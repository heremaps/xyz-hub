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

package com.here.xyz.jobs.steps.impl;

import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.FileStatistics;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ExportStepTest extends StepTest {
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
    public void testExportSpaceToFilesStepUnfiltered() throws Exception {
        FeatureCollection allExistingFeatures = getFeaturesFromSmallSpace(SPACE_ID, null,false);

        LambdaBasedStep step = new ExportSpaceToFiles()
                .withSpaceId(SPACE_ID)
                .withJobId(JOB_ID);

        sendLambdaStepRequest(step, LambdaBasedStep.LambdaStepRequest.RequestType.START_EXECUTION, false);
        Thread.sleep(2000);
        //TODO: switch back to simulation if test issue is fixed
        //sendLambdaStepRequestBlock(step);
        checkOutputs(allExistingFeatures, step.loadOutputs(true));
    }

    @Test
    public void testExportSpaceToFilesStepWithPropertyFilter() throws Exception {
        String propertyFilterString = "p.description=\"Point\"";

        FeatureCollection allExistingFeatures = getFeaturesFromSmallSpace(SPACE_ID, propertyFilterString, false);

        LambdaBasedStep step = new ExportSpaceToFiles()
                .withPropertyFilter(PropertiesQuery.fromString(propertyFilterString))
                .withSpaceId(SPACE_ID)
                .withJobId(JOB_ID);

        sendLambdaStepRequest(step, LambdaBasedStep.LambdaStepRequest.RequestType.START_EXECUTION, false);
        Thread.sleep(2000);

        //TODO: switch back to simulation if test issue is fixed
        //sendLambdaStepRequestBlock(step);
        checkOutputs(allExistingFeatures, step.loadOutputs(true));
    }

    @Test
    public void testExportSpaceToFilesStepWithSpatialFilter() throws Exception {
        String spatialFilterString = "spatial?lat=50.102964&lon=8.6709594&clip=true&radius=5500";

        SpatialFilter spatialFilter = new SpatialFilter()
                .withGeometry(
                    new Point().withCoordinates(new PointCoordinates(8.6709594,50.102964))
                )
                .withRadius(5500)
                .withClip(true);

        FeatureCollection allExistingFeatures = customReadFeaturesQuery(SPACE_ID, spatialFilterString);

        LambdaBasedStep step = new ExportSpaceToFiles()
                .withSpatialFilter(spatialFilter)
                .withSpaceId(SPACE_ID)
                .withJobId(JOB_ID);

        sendLambdaStepRequest(step, LambdaBasedStep.LambdaStepRequest.RequestType.START_EXECUTION, false);
        Thread.sleep(2000);
        //TODO: switch back to simulation if test issue is fixed
        //sendLambdaStepRequestBlock(step);
        checkOutputs(allExistingFeatures, step.loadOutputs(true));
    }

    private void checkOutputs(FeatureCollection expectedFeatures, List<Output> outputs) throws IOException {
        Assertions.assertNotEquals(0, outputs.size());

        List<Feature>  exportedFeatures = new ArrayList<>();

        for (Object output : outputs) {
            if(output instanceof DownloadUrl) {
                exportedFeatures.addAll(downloadFileAndSerializeFeatures((DownloadUrl) output));
            }else if(output instanceof FileStatistics statistics) {
                Assertions.assertEquals(expectedFeatures.getFeatures().size(), statistics.getExportedFeatures());
                Assertions.assertTrue(statistics.getExportedFiles() > 0);
            }
        }

        List<String> existingFeaturesIdList = expectedFeatures.getFeatures().stream().map(Feature::getId).collect(Collectors.toList());
        List<String> exportedFeaturesFeaturesIdList = exportedFeatures.stream().map(Feature::getId).collect(Collectors.toList());

        Assertions.assertTrue(exportedFeaturesFeaturesIdList.containsAll(existingFeaturesIdList));
    }
}
