package com.here.naksha.lib.extmanager.helpers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.here.naksha.lib.extmanager.BaseSetup;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.S3Uri;

public class AmazonS3ClientTest extends BaseSetup {

  @Test
  public void testGetFile() throws IOException {
    AmazonS3Helper s3Helper= Mockito.spy(new AmazonS3Helper());
    S3Uri s3Uri= mock(S3Uri.class);
    doReturn(s3Uri).when(s3Helper).getS3Uri(anyString());
//    doReturn(Optional.of("_tmp")).when(s3Uri).bucket();
    doReturn(new FileInputStream("src/test/resources/data/extension.txt")).when(s3Helper).getS3Object(any());
    File file=s3Helper.getFile("s3://naksa-test/test.jar");
    Assertions.assertNotNull(file);
  }

  @Test
  public void testGetFileContent() throws IOException {
    final String fileName="src/test/resources/data/extension.txt";
    String data= Files.readAllLines(Paths.get(fileName)).stream().collect(Collectors.joining());
    AmazonS3Helper s3Helper= Mockito.spy(new AmazonS3Helper());

    S3Uri s3Uri= mock(S3Uri.class);
    doReturn(s3Uri).when(s3Helper).getS3Uri(anyString());
    doReturn(new FileInputStream(fileName)).when(s3Helper).getS3Object(any());
    Assertions.assertEquals(data, s3Helper.getFileContent("s3://naksa-test/test.jar"));
  }

}
