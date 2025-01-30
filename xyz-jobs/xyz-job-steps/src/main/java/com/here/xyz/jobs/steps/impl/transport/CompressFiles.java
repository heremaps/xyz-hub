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

package com.here.xyz.jobs.steps.impl.transport;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.jobs.steps.execution.SyncLambdaStep;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.util.S3Client;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CompressFiles extends SyncLambdaStep {

  public static final String COMPRESSED_DATA = "compressed-data";
  private static final int DEFAULT_BUFFER_SIZE = 8192;
  private static final String ZIP_CONTENT_TYPE = "application/zip";
  private static final Logger logger = LogManager.getLogger();
  private final Set<String> createdFolders = new HashSet<>();

  /**
   * If this field is provided and the input has the corresponding metadata key-value, we will create a folder by that key-value and place
   * the file(s) inside it.
   */
  @JsonView({Internal.class, Static.class})
  private String groupByMetadataKey;

  {
    setOutputSets(List.of(new OutputSet(COMPRESSED_DATA, Visibility.SYSTEM, ".zip")));
  }

  @Override
  public int getTimeoutSeconds() {
    int estimatedSeconds = getEstimatedExecutionSeconds();
    return Math.max(estimatedSeconds * 4, 60);
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    // 1 second for each 1 MB of input data
    long totalByteSize = loadInputs(UploadUrl.class).stream()
        .mapToLong(Input::getByteSize)
        .sum();
    return (int) (totalByteSize / (1024 * 1024));
  }

  @Override
  public String getDescription() {
    return "Compresses GeoJSON objects uploaded to S3 into a ZIP archive.";
  }

  @Override
  public void execute() throws Exception {
    // stores all data in memory, which may need optimization for large data sets
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ZipOutputStream zipStream = new ZipOutputStream(outputStream)) {

      for (InputSet inputSet : getInputSets()) {
        for (Input input : loadInputs(inputSet, UploadUrl.class)) {
          processInputToZip(input, inputSet, zipStream);
        }
      }

      zipStream.finish();

      byte[] byteArray = outputStream.toByteArray();

      registerOutputs(List.of(new DownloadUrl()
          .withContent(byteArray)
          .withContentType(ZIP_CONTENT_TYPE)
          .withByteSize(byteArray.length)
      ), COMPRESSED_DATA);

      logger.info("ZIP successfully written to S3");
    }
    catch (Exception e) {
      logger.error("Error processing inputs into ZIP: ", e);
      throw e;
    }
  }

  private void processInputToZip(Input input, InputSet inputSet, ZipOutputStream zipStream) {
    try {
      S3Client sourceClient = S3Client.getInstance(input.getS3Bucket());
      String zipEntryPath = composeFileName(input, inputSet);

      if (groupByMetadataKey != null && !groupByMetadataKey.isEmpty()
          && input.getMetadata() != null && !input.getMetadata().isEmpty()) {
        String folderName = (String) input.getMetadata().getOrDefault(groupByMetadataKey, "default");

        if (!createdFolders.contains(folderName)) {
          createFolderInZip(folderName, zipStream);
          createdFolders.add(folderName);
        }

        zipEntryPath = folderName + "/" + zipEntryPath;
      }

      if (sourceClient.isFolder(input.getS3Key()))
        addFolderToZip(input, sourceClient, zipEntryPath, zipStream);
      else
        addFileToZip(input, sourceClient, zipEntryPath, zipStream);
    }
    catch (Exception e) {
      logger.error("Error processing input {} into ZIP. Skipping. Error: ", input.getS3Key(), e);
    }
  }

  private String composeFileName(Input input, InputSet inputSet) {
    String fullPath = input.getS3Key();
    String partToCut = inputSet.toS3Path(getJobId());

    if (fullPath.startsWith(partToCut))
      return fullPath.substring(partToCut.length() + 1);
    else
      throw new IllegalArgumentException("partToCut is not at the beginning of fullPath: " + fullPath);
  }

  /**
   * Creates an empty folder entry in the ZIP. Example: if folderName = "myFolder", then we create the entry "myFolder/".
   */
  private void createFolderInZip(String folderName, ZipOutputStream zipStream) {
    try {
      ZipEntry folderEntry = new ZipEntry(folderName + "/");
      zipStream.putNextEntry(folderEntry);
      zipStream.closeEntry();

      logger.info("Created folder entry '{}' in the ZIP.", folderName + "/");
    }
    catch (IOException e) {
      logger.error("Error creating folder '{}' in the ZIP. Skipping. Error: ", folderName, e);
    }
  }

  private void addFolderToZip(Input input, S3Client sourceClient, String zipEntryPath, ZipOutputStream zipStream) throws Exception {
    List<String> objectKeys = sourceClient.listObjects(input.getS3Key());

    if (objectKeys.isEmpty()) {
      // create an empty folder inside the Zip
      zipStream.putNextEntry(new ZipEntry(zipEntryPath + "/"));
      zipStream.closeEntry();
    }
    else {
      for (String childKey : objectKeys) {
        // ignoring the folder itself
        if (childKey.equals(input.getS3Key()) || childKey.equals(input.getS3Key() + "/"))
          continue;

        try (InputStream childStream = sourceClient.streamObjectContent(childKey)) {
          writeZipEntry(zipStream, childKey, childStream);
        }
      }
    }
  }

  private void addFileToZip(Input input, S3Client sourceClient, String zipEntryPath, ZipOutputStream zipStream) {
    try (InputStream fileStream = sourceClient.streamObjectContent(input.getS3Key())) {
      writeZipEntry(zipStream, zipEntryPath, fileStream);
      logger.info("Added file '{}' to ZIP under entry '{}'. Size: {} bytes", input.getS3Key(), zipEntryPath, input.getByteSize());
    }
    catch (Exception e) {
      logger.error("Error adding file '{}' to ZIP at '{}'. Skipping. Error: ", input.getS3Key(), zipEntryPath, e);
    }
  }

  private void writeZipEntry(ZipOutputStream zipStream, String entryPath, InputStream contentStream) throws IOException {
    if (contentStream == null) {
      logger.warn("Content stream for '{}' is null. Skipping entry.", entryPath);
      return;
    }

    try (BufferedInputStream bufferedStream = new BufferedInputStream(contentStream, DEFAULT_BUFFER_SIZE)) {
      zipStream.putNextEntry(new ZipEntry(entryPath));

      byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
      int bytesRead;
      while ((bytesRead = bufferedStream.read(buffer)) != -1) {
        zipStream.write(buffer, 0, bytesRead);
      }

      zipStream.closeEntry();
    }
  }

  @Override
  public boolean validate() throws BaseHttpServerVerticle.ValidationException {
    return true;
  }

  public String getGroupByMetadataKey() {
    return groupByMetadataKey;
  }

  public void setGroupByMetadataKey(String value) {
    this.groupByMetadataKey = value;
  }

  public CompressFiles withGroupByMetadataKey(String value) {
    setGroupByMetadataKey(value);
    return this;
  }
}