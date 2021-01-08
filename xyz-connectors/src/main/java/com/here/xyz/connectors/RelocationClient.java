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
import com.here.xyz.events.RelocatedEvent;
import com.here.xyz.responses.XyzError;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings("WeakerAccess")
public class RelocationClient {

  private static final Logger logger = LogManager.getLogger();

  private final static String S3_PATH = "tmp/";
  private volatile AmazonS3 s3client;
  private final String bucket;

  public RelocationClient(String bucket) {
    this.bucket = bucket;
  }

  private AmazonS3 getS3client() {
    if (s3client == null) {
      s3client = AmazonS3ClientBuilder.standard().withCredentials(new DefaultAWSCredentialsProviderChain()).build();
    }
    return s3client;
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
    RelocatedEvent event = new RelocatedEvent();
    event.setStreamId(streamId);

    // Keep backward compatibility.
    event.setLocation(name);
    event.setURI("s3://" + bucket + "/" + S3_PATH + name);

    logger.debug("{} - Relocating data to: {}", streamId, event.getURI());
    uploadToS3(new AmazonS3URI(event.getURI()), bytes);

    return event.toString().getBytes();
  }

  /**
   * Returns the input stream of the relocated event.
   *
   * @param event the relocation event.
   * @return the input stream of the real event
   * @throws ErrorResponseException when any error occurred
   */
  public InputStream processRelocatedEvent(RelocatedEvent event) throws ErrorResponseException {
    if (event.getURI() == null && event.getLocation() != null) {
      event.setURI("s3://" + bucket + "/" + S3_PATH + event.getLocation());
    }
    logger.debug("{}, Found relocation event, loading final event from '{}'", event.getStreamId(), event.getURI());

    if (event.getURI().startsWith("s3://")) {
      return downloadFromS3(new AmazonS3URI(event.getURI()));
    }

    throw new ErrorResponseException(event.getStreamId(), XyzError.ILLEGAL_ARGUMENT, "Unsupported URI type");
  }

  /**
   * Downloads the file form S3.
   */
  public InputStream downloadFromS3(AmazonS3URI amazonS3URI) {
    return getS3client().getObject(amazonS3URI.getBucket(), amazonS3URI.getKey()).getObjectContent();
  }

  /**
   * Uploads the data, which should be relocated to S3.
   */
  private void uploadToS3(AmazonS3URI amazonS3URI, byte[] content) {
    ObjectMetadata metaData = new ObjectMetadata();
    metaData.setContentLength(content.length);
    getS3client().putObject(amazonS3URI.getBucket(), amazonS3URI.getKey(), new ByteArrayInputStream(content), metaData);
  }
}
