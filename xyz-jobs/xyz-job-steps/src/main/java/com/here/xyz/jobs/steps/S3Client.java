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

package com.here.xyz.jobs.steps;

import static com.amazonaws.HttpMethod.GET;
import static com.amazonaws.HttpMethod.PUT;

import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.here.xyz.XyzSerializable;
import com.here.xyz.util.service.aws.SecretManagerCredentialsProvider;

import java.io.ByteArrayInputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class S3Client {
  private static final S3Client instance = new S3Client(Config.instance.JOBS_S3_BUCKET);
  private final String bucketName;
  protected static final int PRESIGNED_URL_EXPIRATION_SECONDS = 7 * 24 * 60 * 60;

  protected final AmazonS3 client;

  protected S3Client(String bucketName) {
    this.bucketName = bucketName;

    final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();

    if (isLocal()) {
      builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
              Config.instance.LOCALSTACK_ENDPOINT.toString(), Config.instance.AWS_REGION))
          .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("localstack", "localstack")))
          .withPathStyleAccessEnabled(true);
    }
    else {
      final String region = Config.instance != null ? Config.instance.AWS_REGION : "eu-west-1"; //TODO: Remove default value
      builder.setRegion(region);
    }

    if (Config.instance != null && Config.instance.JOB_BOT_SECRET_ARN != null) {
      synchronized (S3Client.class) {
        builder.setCredentials(new SecretManagerCredentialsProvider(Config.instance.AWS_REGION,
            Config.instance.LOCALSTACK_ENDPOINT.toString(), Config.instance.JOB_BOT_SECRET_ARN));
      }
    }
    client = builder.build();
  }

  public boolean isLocal() {
    //TODO: Rather encapsulate this in a "local" sub-implementation of S3Client
    if(Config.instance.HUB_ENDPOINT.contains("localhost") ||
            Config.instance.HUB_ENDPOINT.contains("xyz-hub:8080") ||
            Config.instance.HUB_ENDPOINT.contains("host.docker.internal"))
      return true;
    return false;
  }

  public static S3Client getInstance() {
    return instance;
  }

  private URL generatePresignedUrl(String key, HttpMethod method) {
    GeneratePresignedUrlRequest generatePresignedUrlRequest =
        new GeneratePresignedUrlRequest(bucketName, key)
            .withMethod(method)
            .withExpiration(new Date(System.currentTimeMillis() + PRESIGNED_URL_EXPIRATION_SECONDS * 1000));

    return client.generatePresignedUrl(generatePresignedUrlRequest);
  }

  public URL generateDownloadURL(String key) {
    return generatePresignedUrl(key, GET);
  }

  public URL generateUploadURL(String key) {
    return generatePresignedUrl(key, PUT);
  }

  public void deleteS3Folder(String folderPath) {
    for (S3ObjectSummary objectSummary : scanFolder(folderPath))
      client.deleteObject(bucketName, objectSummary.getKey()); //TODO: Run partially in parallel in multiple threads
  }

  public List<S3ObjectSummary> scanFolder(String folderPath) {
    ListObjectsRequest listObjects = new ListObjectsRequest()
        .withPrefix(folderPath)
        .withBucketName(bucketName);

    ObjectListing objectListing = client.listObjects(listObjects);
    List<S3ObjectSummary> summaries = new LinkedList<>(objectListing.getObjectSummaries());
    while (objectListing.isTruncated()) {
      objectListing = client.listNextBatchOfObjects(objectListing);
      summaries.addAll(objectListing.getObjectSummaries());
    }

    return summaries;
  }

  public void putJSONObject(String s3Key, Object content) {
    //TODO: How to deal with errors

    String jsonContent = XyzSerializable.serialize(content);
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentType("application/json");
    metadata.addUserMetadata("type", content.getClass().getSimpleName());

    PutObjectRequest request = new PutObjectRequest(bucketName, s3Key, new ByteArrayInputStream(jsonContent.getBytes(StandardCharsets.UTF_8)), metadata);
    request.setMetadata(metadata);
    client.putObject(request);

    //Alternative (w/o metadata) => client.putObject(bucketName, s3Key, jsonContent);
  }

  public ObjectMetadata loadMetadata(String key) {
    return client.getObjectMetadata(bucketName, key);
  }
}
