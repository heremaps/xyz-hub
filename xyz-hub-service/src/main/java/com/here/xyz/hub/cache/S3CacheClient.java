/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

package com.here.xyz.hub.cache;

import com.google.common.collect.ImmutableMap;
import com.here.xyz.hub.Service;
import com.here.xyz.util.service.Core;
import io.vertx.core.Future;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3CacheClient implements CacheClient {
  private static final String EXPIRES_AT = "expiresAt";
  private static final String LAST_ACCESSED_AT = "lastAccessedAt";
  private static final String CONTENT_LENGTH = "contentLength";
  private static final long ACCESS_UPDATE_TIME_THRESHOLD = TimeUnit.DAYS.toMillis(1);
  private static CacheClient instance;
  private static final Logger logger = LogManager.getLogger();
  private volatile S3Client s3Client;
  private String bucket;
  private static final String prefix = "xyz-hub-cache/";

  private S3CacheClient() {
    if (Service.configuration.XYZ_HUB_S3_BUCKET == null)
      throw new RuntimeException("No S3 bucket defined. S3CacheClient can not be used.");
    bucket = Service.configuration.XYZ_HUB_S3_BUCKET;
    initS3Client();
  }

  private void initS3Client() {
    S3ClientBuilder builder = S3Client.builder();
    builder.region(Region.of(Service.configuration.AWS_REGION));

    if (Service.configuration.LOCALSTACK_ENDPOINT != null) {
      AwsBasicCredentials awsCreds = AwsBasicCredentials.create("localstack", "localstack");
      builder
              .endpointOverride(URI.create(Service.configuration.LOCALSTACK_ENDPOINT))
              .credentialsProvider(StaticCredentialsProvider.create(awsCreds));
    }

    s3Client = builder.build();
  }

  public static synchronized CacheClient getInstance() {
    try {
      if (instance == null)
        instance = new S3CacheClient();
    }
    catch (Exception e) {
      logger.error("Error when trying to create the S3 client.", e);
      instance = new NoopCacheClient();
    }
    return instance;
  }

  @Override
  public Future<byte[]> get(String key) {
    return Core.vertx.executeBlocking(promise -> {
      try {
        GetObjectRequest getRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(prefix + key)
                .build();

        ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObject(getRequest, ResponseTransformer.toBytes());

        //Update the "lastAccessedAt" metadata field asynchronously
        updateLastAccessedAt(key);

        promise.complete(objectBytes.asByteArray());
      } catch (Exception e) {
        logger.error("Error retrieving object with key: " + key, e);
        promise.complete(null);
      }
    });
  }

  @Override
  public void set(String key, byte[] value, long ttl) {
    Core.vertx.executeBlocking(promise -> {
      final long now = Core.currentTimeMillis();
      long expiresAt = now + ttl;
      Map<String, String> metadata = getMetadata(String.valueOf(expiresAt), now, value.length);

      PutObjectRequest putRequest = PutObjectRequest.builder()
              .bucket(bucket)
              .key(prefix + key)
              .metadata(metadata)
              .build();
      s3Client.putObject(putRequest, RequestBody.fromBytes(value));
      promise.complete();
    }, false);
  }

  private static Map<String, String> getMetadata(long expiresAt, long lastAccessedAt, long contentLength) {
    return ImmutableMap.of(
            EXPIRES_AT, String.valueOf(expiresAt),
            LAST_ACCESSED_AT, String.valueOf(lastAccessedAt),
            CONTENT_LENGTH, String.valueOf(contentLength)
    );
  }

  private static Map<String, String> getMetadata(String expiresAt, long lastAccessedAt, long contentLength) {
    return ImmutableMap.of(
            EXPIRES_AT, expiresAt,
            LAST_ACCESSED_AT, String.valueOf(lastAccessedAt),
            CONTENT_LENGTH, String.valueOf(contentLength)
    );
  }

  private void updateLastAccessedAt(String key) {

    HeadObjectRequest headRequest = HeadObjectRequest.builder()
            .bucket(bucket)
            .key(prefix + key)
            .build();

    HeadObjectResponse headResponse = s3Client.headObject(headRequest);
    String lastAccessedStr = headResponse.metadata().get(LAST_ACCESSED_AT);
    long lastAccessedAt = lastAccessedStr != null ? Long.parseLong(lastAccessedStr) : 0;

      Map<String, String> existingMetadata = headResponse.metadata();
      //Only perform the update if the last update was not done too recently (to save requests)
      long oldAccessedAt = Long.parseLong(existingMetadata.get(LAST_ACCESSED_AT)) + ACCESS_UPDATE_TIME_THRESHOLD;

      if (lastAccessedAt - ACCESS_UPDATE_TIME_THRESHOLD < oldAccessedAt)
        return;

      Core.vertx.executeBlocking(promise -> {
        Map<String, String> newMetadata = new HashMap<>(existingMetadata);
        newMetadata.put(LAST_ACCESSED_AT, String.valueOf(lastAccessedAt));

        CopyObjectRequest copyRequest = CopyObjectRequest.builder()
                .copySource(bucket + "/" + prefix + key)
                .destinationBucket(bucket)
                .destinationKey(prefix + key)
                .metadata(newMetadata)
                .metadataDirective(MetadataDirective.REPLACE)
                .build();

        s3Client.copyObject(copyRequest);
      }, false);
  }

  @Override
  public void remove(String key) {
    Core.vertx.executeBlocking(promise -> {
      DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
              .bucket(bucket)
              .key(prefix + key)
              .build();
      s3Client.deleteObject(deleteRequest);
    }, false);
  }

  @Override
  public void shutdown() {
    instance = null;
    if (s3Client != null) {
      s3Client.close();
    }
  }
}