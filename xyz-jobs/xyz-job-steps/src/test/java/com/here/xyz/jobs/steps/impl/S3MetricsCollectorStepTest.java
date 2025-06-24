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

package com.here.xyz.jobs.steps.impl;

import static com.here.xyz.jobs.steps.Step.InputSet.USER_INPUTS;
import static com.here.xyz.jobs.steps.Step.Visibility.USER;
import static com.here.xyz.jobs.steps.impl.S3MetricsCollectorStep.S3_METRICS;
import static com.here.xyz.jobs.util.test.StepTestBase.S3ContentType.APPLICATION_JSON;

import com.google.common.io.ByteStreams;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.models.hub.Ref;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class S3MetricsCollectorStepTest extends StepTest {

    @Test
    public void testSingleFileMetricsCollection() throws Exception {
        uploadInputFile(JOB_ID, ByteStreams.toByteArray(inputStream("/testFiles/file1.geojson")), APPLICATION_JSON);

        S3MetricsCollectorStep step = new S3MetricsCollectorStep()
                .withJobId(JOB_ID)
                .withOutputSetVisibility(S3_METRICS, USER)
                .withInputSets(List.of(USER_INPUTS.get()));

        sendLambdaStepRequestBlock(step, true);

        List<Output> testOutputs = step.loadUserOutputs();

        Assertions.assertEquals(1, testOutputs.size());
        Assertions.assertInstanceOf(FeatureStatistics.class, testOutputs.get(0));

        FeatureStatistics stats = (FeatureStatistics) testOutputs.get(0);
        Assertions.assertEquals(1, stats.getFileCount());
        Assertions.assertTrue(stats.getByteSize() > 0);
    }

    @Test
    public void testMultipleFilesMetricsCollection() throws Exception {
        uploadInputFile(JOB_ID, ByteStreams.toByteArray(inputStream("/testFiles/file1.geojson")), APPLICATION_JSON);
        uploadInputFile(JOB_ID, ByteStreams.toByteArray(inputStream("/testFiles/file2.geojson")), APPLICATION_JSON);

        S3MetricsCollectorStep step = new S3MetricsCollectorStep()
                .withJobId(JOB_ID)
                .withOutputSetVisibility(S3_METRICS, USER)
                .withInputSets(List.of(USER_INPUTS.get()));

        sendLambdaStepRequestBlock(step, true);

        List<Output> testOutputs = step.loadUserOutputs();

        Assertions.assertEquals(1, testOutputs.size());
        Assertions.assertInstanceOf(FeatureStatistics.class, testOutputs.get(0));

        FeatureStatistics stats1 = (FeatureStatistics) testOutputs.get(0);
        Assertions.assertEquals(2, stats1.getFileCount());
        Assertions.assertTrue(stats1.getByteSize() > 0);
    }

    @Test
    public void testVersionAndTagAttributes() throws Exception {
        uploadInputFile(JOB_ID, ByteStreams.toByteArray(inputStream("/testFiles/file1.geojson")), APPLICATION_JSON);

        S3MetricsCollectorStep step = new S3MetricsCollectorStep()
                .withVersion(new Ref("1"))
                .withProvidedTag("test-tag")
                .withJobId(JOB_ID)
                .withOutputSetVisibility(S3_METRICS, USER)
                .withInputSets(List.of(USER_INPUTS.get()));

        sendLambdaStepRequestBlock(step, true);

        List<Output> testOutputs = step.loadUserOutputs();

        Assertions.assertEquals(1, testOutputs.size());
        Assertions.assertInstanceOf(FeatureStatistics.class, testOutputs.get(0));

        FeatureStatistics stats = (FeatureStatistics) testOutputs.get(0);
        Assertions.assertEquals(1, stats.getVersionRef().getVersion());
        Assertions.assertEquals("test-tag", stats.getTag());
    }

    @Test
    public void testEmptyInputs() throws Exception {

        S3MetricsCollectorStep step = new S3MetricsCollectorStep()
                .withJobId(JOB_ID)
                .withOutputSetVisibility(S3_METRICS, USER)
                .withInputSets(List.of(USER_INPUTS.get()));

        sendLambdaStepRequestBlock(step, true);

        List<Output> testOutputs = step.loadUserOutputs();

        Assertions.assertEquals(1, testOutputs.size());
    }

    @Test
    public void testFileWithNoFeatures() throws Exception {
        String jsonWithoutFeatures = "{\"type\": \"FeatureCollection\", \"features\": []}";
        uploadInputFile(JOB_ID, jsonWithoutFeatures.getBytes(), APPLICATION_JSON);

        S3MetricsCollectorStep step = new S3MetricsCollectorStep()
                .withJobId(JOB_ID)
                .withOutputSetVisibility(S3_METRICS, USER)
                .withInputSets(List.of(USER_INPUTS.get()));

        sendLambdaStepRequestBlock(step, true);

        List<Output> testOutputs = step.loadUserOutputs();

        Assertions.assertEquals(1, testOutputs.size());
        Assertions.assertInstanceOf(FeatureStatistics.class, testOutputs.get(0));

        FeatureStatistics stats = (FeatureStatistics) testOutputs.get(0);
        Assertions.assertEquals(1, stats.getFileCount());
        Assertions.assertTrue(stats.getByteSize() > 0);
        Assertions.assertEquals(0, stats.getFeatureCount());
    }

    @Test
    public void testBothOutputSetsHaveSameContent() throws Exception {
        uploadInputFile(JOB_ID, ByteStreams.toByteArray(inputStream("/testFiles/file1.geojson")), APPLICATION_JSON);

        S3MetricsCollectorStep step = new S3MetricsCollectorStep()
                .withJobId(JOB_ID)
                .withInputSets(List.of(USER_INPUTS.get()));

        step.execute(false);

        List<Output> featureStatistics = step.loadUserOutputs();

        Assertions.assertEquals(1, featureStatistics.size());

        FeatureStatistics stats = (FeatureStatistics) featureStatistics.get(0);

        Assertions.assertEquals(stats.getFileCount(), stats.getFileCount());
        Assertions.assertEquals(stats.getByteSize(), stats.getByteSize());
        Assertions.assertEquals(stats.getFeatureCount(), stats.getFeatureCount());
    }

    @Test
    public void testMetadataUpload() throws Exception {
        uploadInputFile(JOB_ID, ByteStreams.toByteArray(inputStream("/testFiles/file1.geojson")), APPLICATION_JSON);

        S3MetricsCollectorStep step = new S3MetricsCollectorStep()
                .withJobId(JOB_ID)
                .withOutputMetadata(Map.of("layerId", "address"))
                .withInputSets(List.of(USER_INPUTS.get()));

        step.execute(false);

        List<Output> featureStatistics = step.loadUserOutputs();

        Assertions.assertEquals(1, featureStatistics.size());

        FeatureStatistics stats = (FeatureStatistics) featureStatistics.get(0);

        Assertions.assertFalse(stats.getMetadata().isEmpty());
        Assertions.assertEquals("address", stats.getMetadata().get("layerId"));
    }

    private InputStream inputStream(String fileName) {
        return getClass().getResourceAsStream(fileName);
    }
}
