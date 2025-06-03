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

import com.here.xyz.events.RelocatedEvent;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.here.xyz.responses.XyzError;
import com.here.xyz.util.service.aws.S3Uri;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@SuppressWarnings("WeakerAccess")
public class RelocationClient {

  private static final Logger logger = LogManager.getLogger();

  private final static String S3_PATH = "tmp/";
  private S3Client defaultS3Client;
  private Map<String, S3Client> s3clients = new ConcurrentHashMap<>();
  private final String bucket;

  public RelocationClient(String bucket) {
    this.bucket = bucket;
  }

  private S3Client getS3Client() {
    return getS3Client(null);
  }

  private S3Client getS3Client(String region) {
    if (region == null || region.isEmpty()) {
      if (defaultS3Client == null) {
        defaultS3Client = getS3ClientBuilder()
                .build();
      }
      return defaultS3Client;
    } else {
      if (!s3clients.containsKey(region)) {
        S3Client client = getS3ClientBuilder()
                .region(Region.of(region))
                .build();
        s3clients.put(region, client);
      }
      return s3clients.get(region);
    }
  }

  private S3ClientBuilder getS3ClientBuilder() {
    return S3Client.builder()
            .credentialsProvider(DefaultCredentialsProvider.create());
  }

  /**
   * Relocates a request or response.
   *
   * @param streamId The streamId of the original request or response
   * @param bytes the bytes of the feature collection to be returned.
   * @return the serialized RelocatedEvent as bytes
   */
  public byte[] relocate(String streamId, byte[] bytes) {
    String name = UUID.randomUUID().toString();
    RelocatedEvent event = new RelocatedEvent().withStreamId(streamId);
    String region = System.getenv("AWS_REGION");

    if (runsAsConnectorWithRelocation())
      event.setURI(createS3Uri(region, bucket, S3_PATH + name));
    else {
      //Keep backward compatibility.
      event
              .withLocation(name)
              .withURI(createS3Uri(bucket, S3_PATH + name))
              .withRegion(region);
    }

    logger.debug("{} - Relocating data to: {}", streamId, event.getURI());
    if (event.getURI().startsWith("s3://") || event.getURI().startsWith("http")) {
      uploadToS3(new S3Uri(event.getURI()), bytes, region);
    } else {
      logger.error("{}, Unsupported URI type {} from bucket {}, S3 path {} and name {}", event.getStreamId(), event.getURI(), bucket, S3_PATH, name);
    }

    return event.toString().getBytes();
  }

  /**
   * Returns the input stream of the original event after unwrapping the relocated event.
   *
   * @param event the relocation event.
   * @return the input stream of the original event
   * @throws ErrorResponseException when any error occurred
   */
  public InputStream processRelocatedEvent(RelocatedEvent event) throws ErrorResponseException {
    return processRelocatedEvent(event, null);
  }

  /**
   * Returns the input stream of the original event after unwrapping the relocated event.
   *
   * @param event the relocation event.
   * @param region if not null, the region from where to download the original content
   * @return the input stream of the original event
   * @throws ErrorResponseException when any error occurred
   */
  public InputStream processRelocatedEvent(RelocatedEvent event, String region) throws ErrorResponseException {
    if (event.getURI() == null && event.getLocation() != null) {
      event.setURI(createS3Uri(bucket, S3_PATH + event.getLocation()));
      logger.warn("{}, the RelocatedEvent returned by the connector still uses the deprecated \"location\" field."
              + "The connector should use the field \"URI\" instead.");
    }
    if (event.getRegion() != null && !event.getRegion().isEmpty())
      region = event.getRegion();
    logger.debug("{}, Found relocation event, loading original event from '{}'", event.getStreamId(), event.getURI());

    if (event.getURI().startsWith("s3://") || event.getURI().startsWith("http")) {
      return downloadFromS3(new S3Uri(event.getURI()), region);
    }
    throw new ErrorResponseException(event.getStreamId(), XyzError.ILLEGAL_ARGUMENT, "Unsupported URI type");
  }

  /**
   * Downloads the file from S3.
   */
  public InputStream downloadFromS3(S3Uri s3Uri, String region) {
    return getS3Client(region).getObject(GetObjectRequest.builder()
            .bucket(s3Uri.bucket())
            .key(s3Uri.key())
            .build());
  }

  /**
   * Uploads the data, which should be relocated to S3.
   */
  private void uploadToS3(S3Uri s3Uri, byte[] content, String region) {
    getS3Client(region).putObject(PutObjectRequest.builder()
            .bucket(s3Uri.bucket())
            .key(s3Uri.key())
            .build(), RequestBody.fromBytes(content));
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