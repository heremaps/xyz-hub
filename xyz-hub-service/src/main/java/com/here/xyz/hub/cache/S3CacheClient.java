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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.Service;
import io.vertx.core.Future;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class S3CacheClient implements CacheClient {
  private static final String EXPIRES_AT = "expiresAt";
  private static final String LAST_ACCESSED_AT = "lastAccessedAt";
  private static final long ACCESS_UPDATE_TIME_THRESHOLD = TimeUnit.DAYS.toMillis(1);
  private static CacheClient instance;
  private static final Logger logger = LogManager.getLogger();
  private volatile AmazonS3 s3client;
  private String bucket;
  private static final String prefix = "xyz-hub-cache/";


  private S3CacheClient() {
    if (Service.configuration.XYZ_HUB_S3_BUCKET == null)
      throw new RuntimeException("No S3 bucket defined. S3CacheClient can not be used.");
    bucket = Service.configuration.XYZ_HUB_S3_BUCKET;
    initS3Client();
  }

  private void initS3Client() {
    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder
        .standard()
        .withCredentials(new DefaultAWSCredentialsProviderChain());

    if (Service.configuration.LOCALSTACK_ENDPOINT != null) {
      builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
          Service.configuration.LOCALSTACK_ENDPOINT, Service.configuration.AWS_REGION))
          .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("localstack", "localstack")))
          .withPathStyleAccessEnabled(true);
    }

    s3client = builder.build();
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
    return Service.vertx.executeBlocking(promise -> {
      S3Object object = s3client.getObject(bucket, prefix + key);
      try {
        promise.complete(ByteStreams.toByteArray(object.getObjectContent()));
        //Update the "lastAccessedAt" metadata field asynchronously
        updateLastAccessedAt(key, object.getObjectMetadata(), Core.currentTimeMillis());
      }
      catch (IOException e) {
        logger.error("Exception trying to read S3 object with key {}.", key, e);
        promise.complete(null);
      }
    });
  }

  @Override
  public void set(String key, byte[] value, long ttl) {
    Service.vertx.executeBlocking(promise -> {
      final long now = Core.currentTimeMillis();
      s3client.putObject(bucket, prefix + key, new ByteArrayInputStream(value),
          getMetadata(now + TimeUnit.SECONDS.toMillis(ttl), now, value.length));
      promise.complete();
    }, false);
  }

  private static ObjectMetadata getMetadata(long expiresAt, long lastAccessedAt, long contentLength) {
    return getMetadata("" + expiresAt, lastAccessedAt, contentLength);
  }

  private static ObjectMetadata getMetadata(String expiresAt, long lastAccessedAt, long contentLength) {
    ObjectMetadata metaData = new ObjectMetadata();
    metaData.setContentLength(contentLength);
    metaData.setUserMetadata(ImmutableMap.of(
        EXPIRES_AT, "" + expiresAt,
        LAST_ACCESSED_AT, "" + lastAccessedAt
    ));
    return metaData;
  }

  private void updateLastAccessedAt(String key, ObjectMetadata existingMetadata, long lastAccessedAt) {
    //Only perform the update if the last update was not done too recently (to save requests)
    long oldAccessedAt = Long.parseLong(existingMetadata.getUserMetadata().get(LAST_ACCESSED_AT)) + ACCESS_UPDATE_TIME_THRESHOLD;
    if (lastAccessedAt - ACCESS_UPDATE_TIME_THRESHOLD < oldAccessedAt)
      return;
    Service.vertx.executeBlocking(promise -> {
      s3client.copyObject(new CopyObjectRequest()
          .withSourceBucketName(bucket)
          .withSourceKey(key)
          .withDestinationBucketName(bucket)
          .withDestinationKey(key)
          .withNewObjectMetadata(getMetadata(existingMetadata.getUserMetadata().get(EXPIRES_AT), lastAccessedAt,
              existingMetadata.getContentLength()))
      );
      promise.complete();
    }, false);
  }

  @Override
  public void remove(String key) {
    Service.vertx.executeBlocking(promise -> {
      s3client.deleteObject(bucket, prefix + key);
      promise.complete();
    }, false);
  }

  @Override
  public void shutdown() {
    instance = null;
    s3client.shutdown();
  }
}
