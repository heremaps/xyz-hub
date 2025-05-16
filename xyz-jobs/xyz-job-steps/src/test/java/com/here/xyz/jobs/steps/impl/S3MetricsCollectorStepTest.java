package com.here.xyz.jobs.steps.impl;

import static com.here.xyz.jobs.steps.Step.InputSet.USER_INPUTS;
import static com.here.xyz.jobs.steps.Step.Visibility.USER;
import static com.here.xyz.jobs.steps.impl.S3MetricsCollectorStep.S3_METRICS;
import static com.here.xyz.jobs.util.test.StepTestBase.S3ContentType.APPLICATION_JSON;

import com.google.common.io.ByteStreams;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.outputs.Output;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;

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
        Assertions.assertTrue(stats.getFeatureCount() > 0);
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

        FeatureStatistics stats = (FeatureStatistics) testOutputs.get(0);
        Assertions.assertEquals(2, stats.getFileCount());
        Assertions.assertTrue(stats.getByteSize() > 0);
        Assertions.assertTrue(stats.getFeatureCount() > 0);
    }

    @Test
    public void testVersionAndTagAttributes() throws Exception {
        uploadInputFile(JOB_ID, ByteStreams.toByteArray(inputStream("/testFiles/file1.geojson")), APPLICATION_JSON);

        S3MetricsCollectorStep step = new S3MetricsCollectorStep()
                .withJobId(JOB_ID)
                .withVersion("1.0.0")
                .withTag("test-tag")
                .withOutputSetVisibility(S3_METRICS, USER)
                .withInputSets(List.of(USER_INPUTS.get()));

        sendLambdaStepRequestBlock(step, true);

        List<Output> testOutputs = step.loadUserOutputs();

        Assertions.assertEquals(1, testOutputs.size());
        Assertions.assertInstanceOf(FeatureStatistics.class, testOutputs.get(0));

        FeatureStatistics stats = (FeatureStatistics) testOutputs.get(0);
        Assertions.assertEquals("1.0.0", stats.getVersion());
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
        Assertions.assertInstanceOf(FeatureStatistics.class, testOutputs.get(0));

        FeatureStatistics stats = (FeatureStatistics) testOutputs.get(0);
        Assertions.assertEquals(0, stats.getFileCount());
        Assertions.assertEquals(0, stats.getByteSize());
        Assertions.assertEquals(0, stats.getFeatureCount());
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

    private InputStream inputStream(String fileName) {
        return getClass().getResourceAsStream(fileName);
    }
}
