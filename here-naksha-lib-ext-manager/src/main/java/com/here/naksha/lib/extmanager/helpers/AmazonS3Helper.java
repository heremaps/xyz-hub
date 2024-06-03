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
package com.here.naksha.lib.extmanager.helpers;

import com.here.naksha.lib.extmanager.FileClient;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest.Builder;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;

public class AmazonS3Helper implements FileClient {
  private static S3Client s3Client;

  public AmazonS3Helper() {
    // NOTE
    // we avoid upfront initialization of s3Client to avoid exception for executions,
    // where AWS S3 connectivity is not mandatory.
  }

  public S3Client getS3Client() {
    if (s3Client == null) {
      synchronized (AmazonS3Helper.class) {
        if (s3Client == null) {
          s3Client = S3Client.builder().crossRegionAccessEnabled(true).build();
        }
      }
    }
    return s3Client;
  }

  public File getFile(@NotNull String url) throws IOException {
    String extension = url.substring(url.lastIndexOf("."));
    S3Uri s3Uri = getS3Uri(url);
    InputStream inputStream = getS3Object(s3Uri);
    File targetFile = File.createTempFile(s3Uri.bucket().orElse("tmp_"), extension);
    try (FileOutputStream fos = new FileOutputStream(targetFile)) {
      byte[] read_buf = new byte[1024];
      int read_len;
      while ((read_len = inputStream.read(read_buf)) > 0) {
        fos.write(read_buf, 0, read_len);
      }
      inputStream.close();
    }
    return targetFile;
  }

  public InputStream getS3Object(S3Uri s3Uri) {
    final String bucket = s3Uri.bucket().get();
    Builder getObjectBuilder = GetObjectRequest.builder().bucket(bucket);
    if (s3Uri.key().isPresent()) getObjectBuilder.key(s3Uri.key().get());
    return getS3Client().getObject(getObjectBuilder.build());
  }

  @Override
  public String getFileContent(String url) throws IOException {
    S3Uri s3Uri = getS3Uri(url);
    InputStream inputStream = getS3Object(s3Uri);
    // Read the text input stream one line at a time.
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      StringBuilder stringBuilder = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        stringBuilder.append(line);
      }
      return stringBuilder.toString();
    }
  }

  public List<String> listKeysInBucket(String url) {
    S3Uri fileUri = getS3Uri(url);
    String delimiter = "/";

    ListObjectsRequest.Builder listObjectsRequestBuilder =
        ListObjectsRequest.builder().bucket(fileUri.bucket().get()).delimiter(delimiter);

    if (fileUri.key().isPresent())
      listObjectsRequestBuilder.prefix(fileUri.key().get());

    ListObjectsResponse response = getS3Client().listObjects(listObjectsRequestBuilder.build());
    return response.commonPrefixes().stream().map(cm -> cm.prefix()).toList();
  }

  public S3Uri getS3Uri(String url) {
    URI uri = URI.create(url);
    return getS3Client().utilities().parseUri(uri);
  }
}
