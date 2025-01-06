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

package com.here.xyz.jobs.util;

import com.here.xyz.jobs.steps.Config;
import com.here.xyz.util.service.aws.SecretManagerCredentialsProvider;
import com.here.xyz.util.service.aws.SecretManagerCredentialsProviderV2;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPOutputStream;

public class S3Client {
  private static Map<String, S3Client> instances = new ConcurrentHashMap<>();
  private final String bucketName;
  private final String region;
  protected static final int PRESIGNED_URL_EXPIRATION_SECONDS = 7 * 24 * 60 * 60;

  private final software.amazon.awssdk.services.s3.S3Client s3Client;
  private final S3Presigner presigner;

  protected S3Client(String bucketName) {
    this.bucketName = bucketName;

    final S3ClientBuilder builder = software.amazon.awssdk.services.s3.S3Client.builder()
            .region(Region.of(Config.instance.AWS_REGION));
    final S3Presigner.Builder presignerBuilder = S3Presigner.builder()
            .region(Region.of(Config.instance.AWS_REGION));

    if (Config.instance != null && Config.instance.LOCALSTACK_ENDPOINT != null) {
      this.region = Config.instance.AWS_REGION;
      builder.endpointOverride(URI.create(Config.instance.LOCALSTACK_ENDPOINT.toString()))
              .credentialsProvider(StaticCredentialsProvider.create(
                      AwsBasicCredentials.create("localstack", "localstack")));
      presignerBuilder.endpointOverride(URI.create(Config.instance.LOCALSTACK_ENDPOINT.toString()))
              .credentialsProvider(StaticCredentialsProvider.create(
                      AwsBasicCredentials.create("localstack", "localstack")));
    } else if (Config.instance.JOBS_S3_BUCKET.equals(bucketName)) {
      this.region = Config.instance != null ? Config.instance.AWS_REGION : "eu-west-1"; //TODO: Remove default value
    } else {
      region = getInstance().region;
      if (Config.instance.forbiddenSourceRegions().contains(region))
        throw new IllegalArgumentException("Source bucket region " + region + " is not allowed.");
    }

    if (Config.instance != null && Config.instance.JOB_BOT_SECRET_ARN != null) {
      synchronized (S3Client.class) {
        builder.credentialsProvider(new SecretManagerCredentialsProviderV2(Config.instance.AWS_REGION,
                Config.instance.LOCALSTACK_ENDPOINT == null ? null : Config.instance.LOCALSTACK_ENDPOINT.toString(),
                Config.instance.JOB_BOT_SECRET_ARN));
        presignerBuilder.credentialsProvider(new SecretManagerCredentialsProviderV2(Config.instance.AWS_REGION,
                Config.instance.LOCALSTACK_ENDPOINT == null ? null : Config.instance.LOCALSTACK_ENDPOINT.toString(),
                Config.instance.JOB_BOT_SECRET_ARN));
      }
    }
    this.s3Client = builder.build();

    this.presigner = presignerBuilder.build();
  }

  public static S3Client getInstance() {
    return getInstance(Config.instance.JOBS_S3_BUCKET);
  }

  public static S3Client getInstance(String bucketName) {
    if (!instances.containsKey(bucketName)) {
      instances.put(bucketName, new S3Client(bucketName));
    }
    return instances.get(bucketName);
  }

  public URL generateDownloadURL(String key) {
    return generatePresignedUrl(key, SdkHttpMethod.GET);
  }

  public URL generateUploadURL(String key) {
    return generatePresignedUrl(key, SdkHttpMethod.PUT);
  }

  private URL generatePresignedUrl(String key, SdkHttpMethod method) {
    S3Presigner presigner = this.presigner;

    if (method == SdkHttpMethod.GET) {
      GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
              .signatureDuration(Duration.ofSeconds(PRESIGNED_URL_EXPIRATION_SECONDS))
              .getObjectRequest(b -> b.bucket(bucketName).key(key))
              .build();
      return presigner.presignGetObject(presignRequest).url();
    } else if (method == SdkHttpMethod.PUT) {
      PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
              .signatureDuration(Duration.ofSeconds(PRESIGNED_URL_EXPIRATION_SECONDS))
              .putObjectRequest(b -> b.bucket(bucketName).key(key))
              .build();
      return presigner.presignPutObject(presignRequest).url();
    }

    throw new UnsupportedOperationException("Unsupported HTTP Method: " + method);
  }

  public List<S3Object> scanFolder(String folderPath) {
    ListObjectsV2Request request = ListObjectsV2Request.builder()
            .bucket(bucketName)
            .prefix(folderPath)
            .build();

    ListObjectsV2Response response = s3Client.listObjectsV2(request);
    return response.contents();
  }

  public byte[] loadObjectContent(String s3Key) {
    GetObjectRequest request = GetObjectRequest.builder()
            .bucket(bucketName)
            .key(s3Key)
            .build();

    try (InputStream inputStream = s3Client.getObject(request)) {
      return inputStream.readAllBytes();
    } catch (Exception e) {
      throw new RuntimeException("Failed to load object content for key: " + s3Key, e);
    }
  }

  public InputStream streamObjectContent(String s3Key) {
    return streamObjectContent(s3Key, -1, -1);
  }

  public InputStream streamObjectContent(String s3Key, long offset, long length) {
    GetObjectRequest.Builder builder = GetObjectRequest.builder()
            .bucket(bucketName)
            .key(s3Key);

    if (offset >= 0 && length >= 0)
      builder.range("bytes=" + offset + "-" + (offset + length - 1));
    else if (offset >= 0)
      builder.range("bytes=" + offset + "-");

    return s3Client.getObject(builder.build());
  }

  public void putObject(String s3Key, String contentType, byte[] content) throws AwsServiceException,
          SdkClientException {
    PutObjectRequest request = PutObjectRequest.builder()
            .bucket(bucketName)
            .key(s3Key)
            .contentType(contentType)
            .build();

    s3Client.putObject(request, RequestBody.fromBytes(content));
  }

  public void putObject(String s3Key, String contentType, byte[] content, boolean gzip) throws IOException {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("contentType", contentType);

    PutObjectRequest.Builder builder = PutObjectRequest.builder()
            .bucket(bucketName)
            .key(s3Key);

    if (gzip) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(baos)) {
        gzipOutputStream.write(content);
      }

      metadata.put("contentEncoding","gzip");
      metadata.put("contentLength", String.valueOf(baos.size()));

      builder.metadata(metadata);

      s3Client.putObject(builder.build(), RequestBody.fromBytes(baos.toByteArray()));
    }
    else {
      metadata.put("contentLength", String.valueOf(content.length));
      s3Client.putObject(builder.build(), RequestBody.fromBytes(content));
    }
  }

  public void deleteFolder(String folderPath) {
    List<S3Object> objectsToDelete = scanFolder(folderPath);

    for (S3Object s3Object : objectsToDelete) {
      DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
              .bucket(bucketName)
              .key(s3Object.key())
              .build();

      s3Client.deleteObject(deleteRequest);
    }
  }

  public static String getBucketFromS3Uri(String s3Uri) {
    if (!s3Uri.startsWith("s3://"))
      return null;
    return s3Uri.substring(5, s3Uri.substring(5).indexOf("/") + 5);
  }

  public static String getKeyFromS3Uri(String s3Uri) {
    if (!s3Uri.startsWith("s3://"))
      return null;
    return s3Uri.substring(s3Uri.substring(5).indexOf("/") + 5 + 1);
  }
}