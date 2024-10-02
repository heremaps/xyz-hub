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

import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.FileStatistics;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.StatisticsResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.here.xyz.jobs.datasets.space.UpdateStrategy.DEFAULT_UPDATE_STRATEGY;

public class ExportStepTest extends JobStepTest {
    private static final int FILE_COUNT = 1;
    private static final int FEATURE_COUNT = 33;

    @BeforeEach
    public void setUp() throws Exception {
        uploadFiles(JOB_ID, FILE_COUNT, FEATURE_COUNT, ImportFilesToSpace.Format.GEOJSON);

        LambdaBasedStep step = new ImportFilesToSpace()
                .withUpdateStrategy(DEFAULT_UPDATE_STRATEGY)
                .withSpaceId(SPACE_ID);

        sendLambdaStepRequestBlock(step);
    }

    @Test
    public void testExportSpaceToFilesStep() throws Exception {
        StatisticsResponse statsBefore = getStatistics(SPACE_ID);
        FeatureCollection allExistingFeatures = getAllFeaturesFromSmallSpace(SPACE_ID);

        LambdaBasedStep step = new ExportSpaceToFiles()
                .withSpaceId(SPACE_ID)
                .withJobId(JOB_ID);

        sendLambdaStepRequestBlock(step);

        StatisticsResponse statsAfter = getStatistics(SPACE_ID);
        Assertions.assertEquals(statsBefore.getCount().getValue(), statsAfter.getCount().getValue());

        List outputs = step.loadOutputs(true);
        Assertions.assertNotEquals(0, outputs.size());

        List<Feature>  exportedFeatures = new ArrayList<>();

        for (Object output : outputs) {
            if(output instanceof DownloadUrl) {
                exportedFeatures.addAll(downloadFileAndSerializeFeatures((DownloadUrl) output));
            }else if(output instanceof FileStatistics statistics) {
                Assertions.assertEquals(getExpectedFeatureCount(), statistics.getExportedFeatures());
                Assertions.assertEquals(getExpectedFeatureCount() > ExportSpaceToFiles.PARALLELIZTATION_MIN_THRESHOLD ?
                        ExportSpaceToFiles.PARALLELIZTATION_THREAD_COUNT : 1 , statistics.getExportedFiles());
            }
        }

        Assertions.assertEquals(getExpectedFeatureCount(), allExistingFeatures.getFeatures().size());

        List<String> existingFeaturesIdList = allExistingFeatures.getFeatures().stream().map(Feature::getId).collect(Collectors.toList());
        List<String> exportedFeaturesFeaturesIdList = exportedFeatures.stream().map(Feature::getId).collect(Collectors.toList());

        Assertions.assertTrue(exportedFeaturesFeaturesIdList.containsAll(existingFeaturesIdList));
    }

    private int getExpectedFeatureCount(){
        return FILE_COUNT * FEATURE_COUNT;
    }
}
