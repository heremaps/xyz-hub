package com.here.xyz.jobs.steps.impl;

import static com.here.xyz.jobs.steps.Step.InputSet.USER_INPUTS;

import com.google.common.io.ByteStreams;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.execution.SyncLambdaStep;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.jobs.util.S3Client;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class CompressGeoObjectsTest extends StepTest {

    @Test
    public void testSingleFileCompression() throws Exception {
        uploadInputFile(JOB_ID, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file2.geojson")), S3ContentType.APPLICATION_JSON);

        SyncLambdaStep step = new CompressStep()
                .withJobId(JOB_ID)
                .withOutputSetVisibility(CompressStep.COMPRESSED_DATA, Step.Visibility.USER)
                .withInputSets(List.of(USER_INPUTS.get()));

        sendLambdaStepRequestBlock(step, true);

        List<Output> testOutputs = step.loadUserOutputs();
        Assertions.assertEquals(1, testOutputs.size());

        byte[] archiveBytes = S3Client.getInstance().loadObjectContent(testOutputs.get(0).getS3Key());
        List<String> zipContents = getZipContents(archiveBytes);

        Assertions.assertEquals(1, zipContents.size());
    }

    @Test
    public void testMultipleFilesCompression() throws Exception {

        uploadInputFile(JOB_ID, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file1.geojson")), S3ContentType.APPLICATION_JSON);
        uploadInputFile(JOB_ID, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file2.geojson")), S3ContentType.APPLICATION_JSON);

        SyncLambdaStep step = new CompressStep()
                .withJobId(JOB_ID)
                .withOutputSetVisibility(CompressStep.COMPRESSED_DATA, Step.Visibility.USER)
                .withInputSets(List.of(USER_INPUTS.get()));

        sendLambdaStepRequestBlock(step, true);

        List<Output> testOutputs = step.loadUserOutputs();

        Assertions.assertEquals(1, testOutputs.size());

        byte[] archiveBytes = S3Client.getInstance().loadObjectContent(testOutputs.get(0).getS3Key());

        List<String> zipContents = getZipContents(archiveBytes);
        Assertions.assertEquals(2, zipContents.size());
    }

    @Test
    public void testCompressionWithOutputPath() throws Exception {

        String mockStepId = "mockStepId";
        String mockOutputStepName = "mockOutputStepName";

        uploadOutputFile(JOB_ID, mockStepId, mockOutputStepName, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file1.geojson")), S3ContentType.APPLICATION_JSON);
        uploadOutputFile(JOB_ID, mockStepId, mockOutputStepName, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file2.geojson")), S3ContentType.APPLICATION_JSON);

        SyncLambdaStep step = new CompressStep()
                .withJobId(JOB_ID)
                .withOutputSetVisibility(CompressStep.COMPRESSED_DATA, Step.Visibility.USER)
                .withInputSets(List.of(new Step.InputSet(JOB_ID, mockStepId, mockOutputStepName, false)));

        sendLambdaStepRequestBlock(step, true);

        List<Output> testOutputs = step.loadUserOutputs();

        Assertions.assertEquals(1, testOutputs.size());

        byte[] archiveBytes = S3Client.getInstance().loadObjectContent(testOutputs.get(0).getS3Key());

        List<String> zipContents = getZipContents(archiveBytes);
        Assertions.assertEquals(2, zipContents.size());
    }

    @Test
    public void testEmptyInputCompression() throws Exception {

        uploadInputFile(JOB_ID, new byte[0], S3ContentType.APPLICATION_JSON);

        SyncLambdaStep step = new CompressStep()
                .withJobId(JOB_ID)
                .withOutputSetVisibility(CompressStep.COMPRESSED_DATA, Step.Visibility.USER)
                .withInputSets(List.of(USER_INPUTS.get()));

        sendLambdaStepRequestBlock(step, true);

        List<Output> testOutputs = step.loadUserOutputs();
        Assertions.assertEquals(1, testOutputs.size());

        byte[] archiveBytes = S3Client.getInstance().loadObjectContent(testOutputs.get(0).getS3Key());

        List<String> zipContents = getZipContents(archiveBytes);
        Assertions.assertTrue(zipContents.isEmpty());
    }

    @Test
    public void testCompressionWithBlankMetadataGrouping() throws Exception {

        uploadInputFile(JOB_ID, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file1.geojson")), S3ContentType.APPLICATION_JSON);
        uploadInputFile(JOB_ID, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file2.geojson")), S3ContentType.APPLICATION_JSON);

        SyncLambdaStep step = new CompressStep()
                .withGroupByMetadataKey("")
                .withJobId(JOB_ID)
                .withOutputSetVisibility(CompressStep.COMPRESSED_DATA, Step.Visibility.USER)
                .withInputSets(List.of(USER_INPUTS.get()));

        sendLambdaStepRequestBlock(step, true);

        List<Output> testOutputs = step.loadUserOutputs();
        Assertions.assertEquals(1, testOutputs.size());

        byte[] archiveBytes = S3Client.getInstance().loadObjectContent(testOutputs.get(0).getS3Key());

        List<String> zipContents = getZipContents(archiveBytes);
        Assertions.assertEquals(2, zipContents.size());
    }

    @Ignore
    @Test
    public void testGroupByMetadataKey() throws Exception {

        String mockStepId = "mockStepId";
        String mockOutputStepName = "mockOutputStepName";

        uploadOutputFile(JOB_ID, mockStepId, mockOutputStepName, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file1.geojson")), S3ContentType.APPLICATION_JSON);
        uploadOutputFile(JOB_ID, mockStepId, mockOutputStepName, ByteStreams.toByteArray(this.getClass().getResourceAsStream("/testFiles/file2.geojson")), S3ContentType.APPLICATION_JSON);

        SyncLambdaStep step = new CompressStep()
                .withGroupByMetadataKey("groupKey")
                .withJobId(JOB_ID)
                .withOutputSetVisibility(CompressStep.COMPRESSED_DATA, Step.Visibility.USER)
                .withInputSets(List.of(new Step.InputSet(JOB_ID, mockStepId, mockOutputStepName, false)));

        sendLambdaStepRequestBlock(step, true);

        List<Output> testOutputs = step.loadUserOutputs();
        Assertions.assertEquals(1, testOutputs.size(), "Files grouped by metadata key should still create a single ZIP output.");

        byte[] archiveBytes = S3Client.getInstance().loadObjectContent(testOutputs.get(0).getS3Key());

        List<String> zipContents = getZipContents(archiveBytes);
        Assertions.assertTrue(zipContents.contains("file1.geojson"));
        Assertions.assertTrue(zipContents.contains("file2.geojson"));
    }

    private List<String> getZipContents(byte[] zipBytes) throws Exception {
        List<String> entries = new ArrayList<>();
        try (ZipInputStream zipInputStream = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                entries.add(entry.getName());
            }
        }
        return entries;
    }
}