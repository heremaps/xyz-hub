/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.connectors;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.here.xyz.responses.XyzError;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings("WeakerAccess")
public class RelocationClient {

  private static final Logger logger = LogManager.getLogger();

  private final static String S3_PATH = "tmp/";
  private AmazonS3 defaultS3Client;
  private Map<String, AmazonS3> s3clients = new ConcurrentHashMap<>();
  private final String bucket;

  public RelocationClient(String bucket) {
    this.bucket = bucket;
  }

  private AmazonS3 getS3Client() {
    return getS3Client(null);
  }

  private AmazonS3 getS3Client(String region) {
    if (region == null) {
      if (defaultS3Client == null)
        defaultS3Client = getS3ClientBuilder()
            .build();
      return defaultS3Client;
    }
    if (s3clients.get(region) == null) {
      s3clients.put(region, getS3ClientBuilder()
          .withRegion(region)
          .build());
    }
    return s3clients.get(region);
  }

  private AmazonS3ClientBuilder getS3ClientBuilder() {
    return AmazonS3ClientBuilder
        .standard()
        .withCredentials(new DefaultAWSCredentialsProviderChain());
  }

  /**
   * Relocates a request or response.
   *
   * @param streamId The streamId of the original request or response
   * @param bytes the bytes of the feature collection to be returned.
   * @return the serialized RelocatedEvent as bytes
   */
  public byte[] relocate(String streamId, byte[] bytes) {
    return null;
  }
  /**
   * Downloads the file from S3.
   */
  public InputStream downloadFromS3(AmazonS3URI amazonS3URI, String region) {
    String downloadRegion = region != null ? region : amazonS3URI.getRegion();
    return getS3Client(downloadRegion).getObject(amazonS3URI.getBucket(), amazonS3URI.getKey()).getObjectContent();
  }

  /**
   * Uploads the data, which should be relocated to S3.
   */
  private void uploadToS3(AmazonS3URI amazonS3URI, byte[] content) {
    ObjectMetadata metaData = new ObjectMetadata();
    metaData.setContentLength(content.length);
    this.getS3Client().putObject(amazonS3URI.getBucket(), amazonS3URI.getKey(), new ByteArrayInputStream(content), metaData);
  }

  private String createS3Uri(String bucket, String key) {
    return "s3://" + bucket + "/" + key;
  }

  private String createS3Uri(String region, String bucket, String key) {
    if (region == null)
      return createS3Uri(bucket, key);
    return "https://" + bucket + ".s3." + region + ".amazonaws.com/" + key;
  }

  private static final boolean runsAsConnectorWithRelocation() {
    return System.getenv("S3_BUCKET") != null;
  }
}
