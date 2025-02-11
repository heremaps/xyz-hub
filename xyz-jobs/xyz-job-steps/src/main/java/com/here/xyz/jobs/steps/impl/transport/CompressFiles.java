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
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CompressFiles extends SyncLambdaStep {

  public static final String COMPRESSED_DATA = "compressed-data";
  private static final int DEFAULT_BUFFER_SIZE = 8192;
  private static final String ZIP_CONTENT_TYPE = "application/zip";
  private static final String ARCHIVE_FILE_SUFFIX = ".zip";
  private static final Logger logger = LogManager.getLogger();
  private final Set<String> createdFolders = new HashSet<>();

  /**
   * If this field is provided and the input has the corresponding metadata key-value, we will create a folder by that key-value and place
   * the file(s) inside it.
   */
  @JsonView({Internal.class, Static.class})
  private String groupByMetadataKey;

  /**
   * Specifies the desired size (in bytes) for each individual file within the ZIP archive.
   * If set to -1, files are included without splitting or concatenating based on size.
   */
  @JsonView({Internal.class, Static.class})
  private long desiredContainedFilesize = -1;

  /**
   * Prefix for the archive filename. Set this to customize the archive name.
   */
  @JsonView({Internal.class, Static.class})
  private String archiveFileNamePrefix = null;

  /**
   * Specifies the folder unwrapping level.
   * -1: Do not unwrap
   *  0: Unwrap indefinitely
   *  >0: Remove up to specific level
   */
  @JsonView({Internal.class, Static.class})
  private int unwrapFolderLevel = -1;

  {
    setOutputSets(List.of(new OutputSet(COMPRESSED_DATA, Visibility.SYSTEM, ARCHIVE_FILE_SUFFIX)));
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
  public void execute(boolean resume) throws Exception {
    //TODO: In case of resume, first delete the old zip file or continue to extend it if possible

    // stores all data in memory, which may need optimization for large data sets
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
         ZipOutputStream zipStream = new ZipOutputStream(outputStream)) {

      processInputs(zipStream);

      zipStream.finish();

      byte[] byteArray = outputStream.toByteArray();

      registerOutputs(List.of(new DownloadUrl()
          .withContent(byteArray)
          .withContentType(ZIP_CONTENT_TYPE)
          .withByteSize(byteArray.length)
          .withFileName(archiveFileNamePrefix != null
              ? archiveFileNamePrefix + "_" + getJobId() + ARCHIVE_FILE_SUFFIX
              : null)
      ), COMPRESSED_DATA);

      logger.info("ZIP successfully written to S3");
    }
    catch (Exception e) {
      logger.error("Error processing inputs into ZIP: ", e);
      throw e;
    }
  }

  private void processInputs(ZipOutputStream zipStream) throws IOException {
    if (desiredContainedFilesize == -1) {
      for (InputSet inputSet : getInputSets()) {
        for (Input input : loadInputs(inputSet, UploadUrl.class)) {
          addInputToZip(input, inputSet, zipStream);
        }
      }
    } else {
      compressNormalizedFiles(zipStream);
    }
  }

  private void compressNormalizedFiles(ZipOutputStream zipStream) throws IOException {
    Map<String, List<Input>> groupedInputs = groupInputs(loadAllInputs());
    long currentSize = 0;

    try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {

      for (Map.Entry<String, List<Input>> inputSet : groupedInputs.entrySet()) {

        String processedFolderName = processFolder(inputSet.getKey(), zipStream);

        for (Input input : inputSet.getValue()) {

          if (input.getByteSize() > desiredContainedFilesize) {
            splitAndAddInputToZip(input, zipStream, processedFolderName);
          } else {
            if (currentSize + input.getByteSize() > desiredContainedFilesize) {
              flushBuffer(buffer, zipStream, processedFolderName);
              currentSize = 0;
            }
            ByteArrayOutputStream tempStream = new ByteArrayOutputStream();
            bufferData(buffer, input, tempStream);
            currentSize += input.getByteSize();
          }
        }
        flushBuffer(buffer, zipStream, processedFolderName);
        currentSize = 0;
      }
    }
  }

  private String processFolder(String originalFolderName, ZipOutputStream zipStream) {
    String processedFolderName = unwrapPath(originalFolderName);
    if (processedFolderName != null && !createdFolders.contains(processedFolderName)) {
      createFolderInZip(processedFolderName, zipStream);
    }
    return processedFolderName;
  }

  private Map<String, List<Input>> groupInputs(List<Input> inputs) {
    if (groupByMetadataKey != null && !groupByMetadataKey.isEmpty()) {
      return inputs.stream()
          .collect(Collectors.groupingBy(
              input -> (String) input.getMetadata().getOrDefault(groupByMetadataKey, null)
          ));
    } else {
      Map<String, List<Input>> singleGroup = new HashMap<>();
      singleGroup.put(null, inputs);
      return singleGroup;
    }
  }

  private List<Input> loadAllInputs() {
    return getInputSets().stream()
        .flatMap(inputSet -> loadInputs(inputSet, UploadUrl.class).stream())
        .collect(Collectors.toList());
  }

  private void splitAndAddInputToZip(Input input, ZipOutputStream zipStream, String processedFolderName) {
    try {
      S3Client sourceClient = S3Client.getInstance(input.getS3Bucket());
      try (InputStream fileStream = sourceClient.streamObjectContent(input.getS3Key());
           BufferedReader reader = new BufferedReader(new InputStreamReader(fileStream))) {
        String line;
        ByteArrayOutputStream partStream = new ByteArrayOutputStream();
        long bytesWritten = 0;

        while ((line = reader.readLine()) != null) {
          byte[] lineBytes = line.getBytes();
          // if limit reached and we have what to flush
          if ((bytesWritten + lineBytes.length > desiredContainedFilesize) && partStream.size() > 0) {
            addZipEntry(zipStream, partStream.toByteArray(), processedFolderName);
            partStream.reset();
            bytesWritten = 0;
          }
          partStream.write(lineBytes);
          bytesWritten += lineBytes.length;
        }
        if (partStream.size() > 0) {
          addZipEntry(zipStream, partStream.toByteArray(), processedFolderName);
        }
      }
    }
    catch (Exception e) {
      logger.error("Error splitting and adding file '{}' to ZIP. Skipping. Error: ", input.getS3Key(), e);
    }
  }

  private void addBufferedEntryToZip(ByteArrayOutputStream buffer, ZipOutputStream zipStream, String folderName) {
    String zipEntryPath = composeEntryPath(UUID.randomUUID().toString(), folderName);
    createZipEntry(zipEntryPath, buffer.toByteArray(), zipStream);
  }

  private String composeEntryPath(String fileName, String folderName) {
    String zipEntryPath = String.valueOf(fileName);
    if (groupByMetadataKey != null && !groupByMetadataKey.isEmpty()) {
      if (folderName != null && !folderName.isEmpty() && !folderName.equals("/")) {
        zipEntryPath = folderName + "/" + zipEntryPath;
      }
    }
    return zipEntryPath;
  }

  private void addZipEntry(ZipOutputStream zipStream, byte[] data, String folderName) {
    String fileName = UUID.randomUUID().toString();
    try {
      String zipEntryPath;
      if (folderName != null && !folderName.isEmpty() && !folderName.equals("/"))
        zipEntryPath = folderName + "/" + fileName;
      else
        zipEntryPath = String.valueOf(fileName);
      ZipEntry entry = new ZipEntry(zipEntryPath);
      zipStream.putNextEntry(entry);
      zipStream.write(data);
      zipStream.closeEntry();
      logger.debug("Added entry '{}' to ZIP. Size: {} bytes", zipEntryPath, data.length);
    }
    catch (IOException e) {
      logger.error("Error adding entry to ZIP. Skipping. Error: ", e);
    }
  }

  private void bufferInputContent(Input input, ByteArrayOutputStream buffer) {
    try {
      S3Client sourceClient = S3Client.getInstance(input.getS3Bucket());
      try (InputStream fileStream = sourceClient.streamObjectContent(input.getS3Key())) {
        byte[] data = fileStream.readAllBytes();
        buffer.write(data);
      }
    }
    catch (Exception e) {
      logger.error("Error processing input '{}' to buffer. Skipping. Error: ", input.getS3Key(), e);
    }
  }

  private void addInputToZip(Input input, InputSet inputSet, ZipOutputStream zipStream) {
    try {
      S3Client sourceClient = S3Client.getInstance(input.getS3Bucket());
      String zipEntryPath = composeFileName(input, inputSet);

      if (groupByMetadataKey != null && !groupByMetadataKey.isEmpty()
          && input.getMetadata() != null && !input.getMetadata().isEmpty()) {

        String folderName = (String) input.getMetadata().getOrDefault(groupByMetadataKey, null);
        String processedFolderName = processFolder(folderName, zipStream);

        if (processedFolderName != null && !processedFolderName.isEmpty() && !processedFolderName.equals("/")) {
          zipEntryPath = processedFolderName + "/" + zipEntryPath;
        }
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
    return sanitizeFileName(fullPath, partToCut);
  }

  private String sanitizeFileName(String fullPath, String partToCut) {
    if (fullPath.startsWith(partToCut))
      return fullPath.substring(partToCut.length() + 1);
    else
      throw new IllegalArgumentException("partToCut is not at the beginning of fullPath: " + fullPath);
  }

  /**
   * Creates an empty folder entry in the ZIP. Example: if folderName = "myFolder", then we create the entry "myFolder/".
   */
  private void createFolderInZip(String folderName, ZipOutputStream zipStream) {
    String folderEntryPath = folderName + "/";
    createZipEntry(folderEntryPath, new byte[0], zipStream);
    createdFolders.add(folderName);

    logger.debug("Created folder entry '{}' in the ZIP.", folderName + "/");
  }

  private void createZipEntry(String entryPath, byte[] data, ZipOutputStream zipStream) {
    try {
      ZipEntry entry = new ZipEntry(entryPath);
      zipStream.putNextEntry(entry);
      zipStream.write(data);
      zipStream.closeEntry();
      logger.debug("Added entry '{}' to ZIP. Size: {} bytes", entryPath, data.length);
    } catch (IOException e) {
      logger.error("Error adding entry '{}' to ZIP. Skipping. Error: ", entryPath, e);
    }
  }

  private void addFolderToZip(Input input, S3Client sourceClient, String zipEntryPath, ZipOutputStream zipStream) throws Exception {
    List<String> objectKeys = sourceClient.listObjects(input.getS3Key());

    if (objectKeys.isEmpty()) {
      // create an empty folder inside the Zip
      zipStream.putNextEntry(new ZipEntry(zipEntryPath + "/"));
      zipStream.closeEntry();
    } else {
      for (String childKey : objectKeys) {
        // ignoring the folder itself
        if (childKey.equals(input.getS3Key()) || childKey.equals(input.getS3Key() + "/"))
          continue;

        try (InputStream childStream = sourceClient.streamObjectContent(childKey)) {
          addFileContentToZip(zipStream, childKey, childStream);
        }
      }
    }
  }

  private void addFileToZip(Input input, S3Client sourceClient, String zipEntryPath, ZipOutputStream zipStream) {
    try (InputStream fileStream = sourceClient.streamObjectContent(input.getS3Key())) {
      addFileContentToZip(zipStream, zipEntryPath, fileStream);
      logger.debug("Added file '{}' to ZIP under entry '{}'. Size: {} bytes", input.getS3Key(), zipEntryPath, input.getByteSize());
    }
    catch (Exception e) {
      logger.error("Error adding file '{}' to ZIP at '{}'. Skipping. Error: ", input.getS3Key(), zipEntryPath, e);
    }
  }

  private void addFileContentToZip(ZipOutputStream zipStream, String entryPath, InputStream contentStream) throws IOException {
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

  private void bufferData(ByteArrayOutputStream buffer, Input input, ByteArrayOutputStream tempStream) {
    try {
      bufferInputContent(input, tempStream);
      buffer.write(tempStream.toByteArray());
    }
    catch (Exception e) {
      logger.error("Error processing input '{}' to buffer. Skipping.", input.getS3Key(), e);
    }
  }

  private void flushBuffer(ByteArrayOutputStream buffer, ZipOutputStream zipStream, String folderName) {
    if (buffer.size() > 0) {
      addBufferedEntryToZip(buffer, zipStream, folderName);
      buffer.reset();
    }
  }

  String unwrapPath(String originalPath) {
    if (originalPath == null) {
      return null;
    }
    if (unwrapFolderLevel == -1) {
      return originalPath;
    }
    if (unwrapFolderLevel == 0) {
      return null;
    }

    String[] parts = originalPath.split("/");
    if (unwrapFolderLevel >= parts.length) {
      return null;
    }
    return String.join("/", java.util.Arrays.copyOfRange(parts, unwrapFolderLevel, parts.length));
  }

  @Override
  public boolean validate() throws BaseHttpServerVerticle.ValidationException {
    if (desiredContainedFilesize != -1) {
      if (desiredContainedFilesize < 1024 * 1024 || desiredContainedFilesize > 5L * 1024 * 1024 * 1024) {
        throw new IllegalArgumentException("desiredContainedFilesize must be between 1MB and 5GB or -1");
      }
    }
    if (archiveFileNamePrefix != null && archiveFileNamePrefix.trim().isEmpty()) {
      throw new IllegalArgumentException("archiveFileNamePrefix cannot be empty");
    }
    if (unwrapFolderLevel < -1) {
      throw new IllegalArgumentException("unwrapFolderLevel must be -1 (do not unwrap), 0 (unwrap indefinitely), or greater than 0");
    }
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

  public long getDesiredContainedFilesize() {
    return desiredContainedFilesize;
  }

  public void setDesiredContainedFilesize(long desiredContainedFilesize) {
    this.desiredContainedFilesize = desiredContainedFilesize;
  }

  public CompressFiles withDesiredContainedFilesize(long desiredContainedFilesize) {
    setDesiredContainedFilesize(desiredContainedFilesize);
    return this;
  }

  public String getArchiveFileNamePrefix() {
    return archiveFileNamePrefix;
  }

  public void setArchiveFileNamePrefix(String archiveFileNamePrefix) {
    this.archiveFileNamePrefix = archiveFileNamePrefix;
  }

  public CompressFiles withArchiveFileNamePrefix(String archiveFileNamePrefix) {
    setArchiveFileNamePrefix(archiveFileNamePrefix);
    return this;
  }

  public int getUnwrapFolderLevel() {
    return unwrapFolderLevel;
  }

  public void setUnwrapFolderLevel(int unwrapFolderLevel) {
    this.unwrapFolderLevel = unwrapFolderLevel;
  }

  public CompressFiles withUnwrapFolderLevel(int unwrapFolderLevel) {
    setUnwrapFolderLevel(unwrapFolderLevel);
    return this;
  }
}