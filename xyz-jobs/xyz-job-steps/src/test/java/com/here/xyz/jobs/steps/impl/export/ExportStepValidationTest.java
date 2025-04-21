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

package com.here.xyz.jobs.steps.impl.export;

import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.impl.StepTest;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.models.geojson.coordinates.LinearRingCoordinates;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Polygon;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ExportStepValidationTest extends StepTest {

    @BeforeEach
    public void setup() throws SQLException {
        cleanup();
        createSpace(new Space().withId(SPACE_ID).withVersionsToKeep(10),false);
        //Write three versions
        putRandomFeatureCollectionToSpace(SPACE_ID, 2);
        putRandomFeatureCollectionToSpace(SPACE_ID, 2);
        putRandomFeatureCollectionToSpace(SPACE_ID, 2);
    }

    @Test
    public void testInvalidSpatialFilter(){
        SpatialFilter spatialFilter = new SpatialFilter()
                .withRadius(5500)
                .withClip(true);

        LambdaBasedStep step = new ExportSpaceToFiles()
                .withVersionRef(new Ref(1))
                .withSpatialFilter(spatialFilter)
                .withSpaceId(SPACE_ID)
                .withJobId(JOB_ID);

        //Check ExceptionType - Geometry is null, leads into ValidationError
        Assertions.assertThrows(ValidationException.class, () -> step.validate());
    }

    @Test
    public void testSpatialPointFilterWhichIsToLarge() throws InvalidGeometryException {
        SpatialFilter spatialFilter = new SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(50 ,8)))
                .withRadius(17899);

        LambdaBasedStep step = new ExportSpaceToFiles()
                .withVersionRef(new Ref(1))
                .withSpatialFilter(spatialFilter)
                .withSpaceId(SPACE_ID)
                .withJobId(JOB_ID);

        Assertions.assertThrows(ValidationException.class, () -> step.validate());
    }

    @Test
    public void testSpatialBBOXFilterWhichIsToLarge() throws InvalidGeometryException {
        PolygonCoordinates bbox = new PolygonCoordinates();
        LinearRingCoordinates rC = new LinearRingCoordinates();
        rC.add(new Position(14.381642124745554, 59.549930961654525)); // Bottom-left
        rC.add(new Position(14.381642124745554, 58.87408545972943));  // Bottom-right
        rC.add(new Position(15.589007766807157, 58.87408545972943));  // Top-right
        rC.add(new Position(15.589007766807157, 59.549930961654525)); // Top-left
        rC.add(new Position(14.381642124745554, 59.549930961654525)); // Close the ring
        bbox.add(rC);

        SpatialFilter spatialFilter = new SpatialFilter()
                .withGeometry(new Polygon().withCoordinates(bbox));

        LambdaBasedStep step = new ExportSpaceToFiles()
                .withVersionRef(new Ref(1))
                .withSpatialFilter(spatialFilter)
                .withSpaceId(SPACE_ID)
                .withJobId(JOB_ID);

        //Check ExceptionType - Geometry is null, leads into ValidationError
        Assertions.assertThrows(ValidationException.class, () -> step.validate());
    }

    @Test
    public void testInvalidVersionRef(){
        LambdaBasedStep step1 = new ExportSpaceToFiles()
                .withVersionRef(new Ref(5))
                .withSpaceId(SPACE_ID)
                .withJobId(JOB_ID);

        //Check ExceptionType - Ref Version higher than SpaceVersion
        Assertions.assertThrows(ValidationException.class, () -> step1.validate());

        LambdaBasedStep step2 = new ExportSpaceToFiles()
                .withVersionRef(new Ref("1..5"))
                .withSpaceId(SPACE_ID)
                .withJobId(JOB_ID);

        //Check ExceptionType - Ref EndVersion is higher than max SpaceVersion
        Assertions.assertThrows(ValidationException.class, () -> step2.validate());
    }

    private void checkOutputs(FeatureCollection expectedFeatures, List<Output> userOutputs, List<Output> systemOutputs) throws IOException {
        Assertions.assertNotEquals(0, userOutputs.size());
        Assertions.assertNotEquals(0, systemOutputs.size());

        List<Feature>  exportedFeatures = new ArrayList<>();

        //TODO: Deduplicate the following from ExportTestBase
        for (Output output : userOutputs) {
            if (output instanceof DownloadUrl downloadUrl)
                exportedFeatures.addAll(downloadFileAndSerializeFeatures(downloadUrl));
            else if (output instanceof FeatureStatistics statistics)
                Assertions.assertEquals(expectedFeatures.getFeatures().size(), statistics.getFeatureCount());
        }

        List<String> existingFeaturesIdList = expectedFeatures.getFeatures().stream().map(Feature::getId).collect(Collectors.toList());
        List<String> exportedFeaturesFeaturesIdList = exportedFeatures.stream().map(Feature::getId).collect(Collectors.toList());

        Assertions.assertTrue(exportedFeaturesFeaturesIdList.containsAll(existingFeaturesIdList));
    }
}
