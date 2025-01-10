package com.here.xyz.jobs.steps.impl;

import static com.here.xyz.jobs.steps.Step.InputSet.USER_INPUTS;
import static com.here.xyz.jobs.steps.Step.Visibility.USER;
import static com.here.xyz.jobs.steps.impl.transport.CompressFiles.COMPRESSED_DATA;
import static com.here.xyz.jobs.util.test.StepTestBase.S3ContentType.APPLICATION_JSON;

import com.google.common.io.ByteStreams;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.execution.SyncLambdaStep;
import com.here.xyz.jobs.steps.impl.transport.CompressFiles;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.jobs.util.S3Client;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class CompressFilesStepTest extends StepTest {

  @Test
  public void testSingleFileCompression() throws Exception {
    uploadInputFile(JOB_ID, ByteStreams.toByteArray(inputStream("/testFiles/file2.geojson")), APPLICATION_JSON);

    SyncLambdaStep step = new CompressFiles()
        .withJobId(JOB_ID)
        .withOutputSetVisibility(COMPRESSED_DATA, USER)
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

    uploadInputFile(JOB_ID, ByteStreams.toByteArray(inputStream("/testFiles/file1.geojson")), APPLICATION_JSON);
    uploadInputFile(JOB_ID, ByteStreams.toByteArray(inputStream("/testFiles/file2.geojson")), APPLICATION_JSON);

    SyncLambdaStep step = new CompressFiles()
        .withJobId(JOB_ID)
        .withOutputSetVisibility(COMPRESSED_DATA, USER)
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

    uploadOutputFile(JOB_ID, mockStepId, mockOutputStepName,
        ByteStreams.toByteArray(inputStream("/testFiles/file1.geojson")), APPLICATION_JSON);
    uploadOutputFile(JOB_ID, mockStepId, mockOutputStepName,
        ByteStreams.toByteArray(inputStream("/testFiles/file2.geojson")), APPLICATION_JSON);

    SyncLambdaStep step = new CompressFiles()
        .withJobId(JOB_ID)
        .withOutputSetVisibility(COMPRESSED_DATA, USER)
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

    uploadInputFile(JOB_ID, new byte[0], APPLICATION_JSON);

    SyncLambdaStep step = new CompressFiles()
        .withJobId(JOB_ID)
        .withOutputSetVisibility(COMPRESSED_DATA, USER)
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

    uploadInputFile(JOB_ID, ByteStreams.toByteArray(inputStream("/testFiles/file1.geojson")), APPLICATION_JSON);
    uploadInputFile(JOB_ID, ByteStreams.toByteArray(inputStream("/testFiles/file2.geojson")), APPLICATION_JSON);

    SyncLambdaStep step = new CompressFiles()
        .withGroupByMetadataKey("")
        .withJobId(JOB_ID)
        .withOutputSetVisibility(COMPRESSED_DATA, USER)
        .withInputSets(List.of(USER_INPUTS.get()));

    sendLambdaStepRequestBlock(step, true);

    List<Output> testOutputs = step.loadUserOutputs();
    Assertions.assertEquals(1, testOutputs.size());

    byte[] archiveBytes = S3Client.getInstance().loadObjectContent(testOutputs.get(0).getS3Key());

    List<String> zipContents = getZipContents(archiveBytes);
    Assertions.assertEquals(2, zipContents.size());
  }

  @Test
  public void testGroupBySingleMetadataKey() throws Exception {

    String mockStepId = "mockStepId";
    String mockOutputStepName = "mockOutputStepName";
    String layer = "address";

    uploadOutputFile(JOB_ID, mockStepId, mockOutputStepName,
        ByteStreams.toByteArray(inputStream("/testFiles/file1.geojson")), APPLICATION_JSON);
    uploadOutputFile(JOB_ID, mockStepId, mockOutputStepName,
        ByteStreams.toByteArray(inputStream("/testFiles/file2.geojson")), APPLICATION_JSON);

    SyncLambdaStep step = new CompressFiles()
        .withGroupByMetadataKey("layer")
        .withJobId(JOB_ID)
        .withOutputSetVisibility(COMPRESSED_DATA, USER)
        .withInputSets(List.of(new Step.InputSet(JOB_ID, mockStepId, mockOutputStepName, false, Map.of("layer", layer))));

    sendLambdaStepRequestBlock(step, true);

    List<Output> testOutputs = step.loadUserOutputs();
    Assertions.assertEquals(1, testOutputs.size(), "Files grouped by metadata key should still create a single ZIP output.");

    byte[] archiveBytes = S3Client.getInstance().loadObjectContent(testOutputs.get(0).getS3Key());

    List<String> zipContents = getZipContents(archiveBytes);
    Assertions.assertTrue(zipContents.contains(layer + "/"));
    Assertions.assertEquals(3, zipContents.size());
  }

  @Test
  public void testGroupByMultipleMetadataKeys() throws Exception {
    String mockStepId = "mockStepId";
    String mockStepId2 = "mockStepId2";
    String mockOutputStepName = "mockOutputStepName";
    String layer = "address";
    String layer2 = "building";

    uploadOutputFile(JOB_ID, mockStepId, mockOutputStepName,
        ByteStreams.toByteArray(inputStream("/testFiles/file1.geojson")), APPLICATION_JSON);

    uploadOutputFile(JOB_ID, mockStepId2, mockOutputStepName,
        ByteStreams.toByteArray(inputStream("/testFiles/file2.geojson")), APPLICATION_JSON);

    SyncLambdaStep step = new CompressFiles()
        .withGroupByMetadataKey("layer")
        .withJobId(JOB_ID)
        .withOutputSetVisibility(COMPRESSED_DATA, USER)
        .withInputSets(List.of(
            new Step.InputSet(JOB_ID, mockStepId, mockOutputStepName, false, Map.of(
                "layer", layer
            )),
            new Step.InputSet(JOB_ID, mockStepId2, mockOutputStepName, false, Map.of(
                "layer", layer2
            ))));

    sendLambdaStepRequestBlock(step, true);

    List<Output> testOutputs = step.loadUserOutputs();
    Assertions.assertEquals(1, testOutputs.size());

    byte[] archiveBytes = S3Client.getInstance().loadObjectContent(testOutputs.get(0).getS3Key());

    List<String> zipContents = getZipContents(archiveBytes);
    Assertions.assertTrue(zipContents.contains(layer + "/"));
    Assertions.assertTrue(zipContents.contains(layer2 + "/"));
    Assertions.assertEquals(4, zipContents.size());
  }

  @Test
  public void testMoreFilesWithGroupByMultipleMetadataKeys() throws Exception {
    String mockStepId = "mockStepId";
    String mockStepId2 = "mockStepId2";
    String mockOutputStepName = "mockOutputStepName";
    String layer = "address";
    String layer2 = "building";

    uploadOutputFile(JOB_ID, mockStepId, mockOutputStepName,
        ByteStreams.toByteArray(inputStream("/testFiles/file1.geojson")), APPLICATION_JSON);
    uploadOutputFile(JOB_ID, mockStepId, mockOutputStepName,
        ByteStreams.toByteArray(inputStream("/testFiles/file1.geojson")), APPLICATION_JSON);
    uploadOutputFile(JOB_ID, mockStepId, mockOutputStepName,
        ByteStreams.toByteArray(inputStream("/testFiles/file1.geojson")), APPLICATION_JSON);
    uploadOutputFile(JOB_ID, mockStepId, mockOutputStepName,
        ByteStreams.toByteArray(inputStream("/testFiles/file2.geojson")), APPLICATION_JSON);

    uploadOutputFile(JOB_ID, mockStepId2, mockOutputStepName,
        ByteStreams.toByteArray(inputStream("/testFiles/file1.geojson")), APPLICATION_JSON);
    uploadOutputFile(JOB_ID, mockStepId2, mockOutputStepName,
        ByteStreams.toByteArray(inputStream("/testFiles/file2.geojson")), APPLICATION_JSON);

    SyncLambdaStep step = new CompressFiles()
        .withGroupByMetadataKey("layer")
        .withJobId(JOB_ID)
        .withOutputSetVisibility(COMPRESSED_DATA, USER)
        .withInputSets(List.of(
            new Step.InputSet(JOB_ID, mockStepId, mockOutputStepName, false, Map.of(
                "layer", layer
            )),
            new Step.InputSet(JOB_ID, mockStepId2, mockOutputStepName, false, Map.of(
                "layer", layer2
            ))));

    sendLambdaStepRequestBlock(step, true);

    List<Output> testOutputs = step.loadUserOutputs();
    Assertions.assertEquals(1, testOutputs.size());

    byte[] archiveBytes = S3Client.getInstance().loadObjectContent(testOutputs.get(0).getS3Key());

    List<String> zipContents = getZipContents(archiveBytes);
    Assertions.assertTrue(zipContents.contains(layer + "/"));
    Assertions.assertEquals(5, zipContents.stream().filter(s -> s.startsWith(layer + "/")).count());
    Assertions.assertTrue(zipContents.contains(layer2 + "/"));
    Assertions.assertEquals(3, zipContents.stream().filter(s -> s.startsWith(layer2 + "/")).count());
    Assertions.assertEquals(8, zipContents.size());
  }

  @Test
  public void testGroupByNonExistentMetadataKey() throws Exception {
    String mockStepId = "mockStepId";
    String mockOutputStepName = "mockOutputStepName";

    uploadOutputFile(JOB_ID, mockStepId, mockOutputStepName,
        ByteStreams.toByteArray(inputStream("/testFiles/file1.geojson")), APPLICATION_JSON);

    uploadOutputFile(JOB_ID, mockStepId, mockOutputStepName,
        ByteStreams.toByteArray(inputStream("/testFiles/file2.geojson")), APPLICATION_JSON);

    SyncLambdaStep step = new CompressFiles()
        .withGroupByMetadataKey("nonExistentKey")
        .withJobId(JOB_ID)
        .withOutputSetVisibility(COMPRESSED_DATA, USER)
        .withInputSets(List.of(new Step.InputSet(JOB_ID, mockStepId, mockOutputStepName, false, Map.of())));

    sendLambdaStepRequestBlock(step, true);

    List<Output> testOutputs = step.loadUserOutputs();
    Assertions.assertEquals(1, testOutputs.size());

    byte[] archiveBytes = S3Client.getInstance().loadObjectContent(testOutputs.get(0).getS3Key());
    List<String> zipContents = getZipContents(archiveBytes);

    Assertions.assertEquals(2, zipContents.size(), "Files should not be grouped due to missing metadata keys.");
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

  private InputStream inputStream(String fileName) {
    return getClass().getResourceAsStream(fileName);
  }
}