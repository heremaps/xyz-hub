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

import static com.amazonaws.HttpMethod.GET;
import static com.amazonaws.HttpMethod.PUT;

import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.util.service.aws.SecretManagerCredentialsProvider;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPOutputStream;
import software.amazon.awssdk.services.s3.S3Utilities;

public class S3Client {
  private static Map<String, S3Client> instances = new ConcurrentHashMap<>();
  private final String bucketName;
  protected static final int PRESIGNED_URL_EXPIRATION_SECONDS = 7 * 24 * 60 * 60;
  private static final S3Utilities S3_UTILS = software.amazon.awssdk.services.s3.S3Client.create().utilities();

  //TODO: Switch to AWS SDK2

  protected final AmazonS3 client;

  protected S3Client(String bucketName) {
    this.bucketName = bucketName;

    final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();

    if (Config.instance != null && Config.instance.LOCALSTACK_ENDPOINT != null) {
      builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
              Config.instance.LOCALSTACK_ENDPOINT.toString(), Config.instance.AWS_REGION))
          .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("localstack", "localstack")))
          .withPathStyleAccessEnabled(true);
    }
    else if (Config.instance.JOBS_S3_BUCKET.equals(bucketName)) {
      final String region = Config.instance != null ? Config.instance.AWS_REGION : "eu-west-1"; //TODO: Remove default value
      builder.setRegion(region);
    }
    else {
      String bucketRegion = getInstance().client.getBucketLocation(bucketName);
      if (Config.instance.forbiddenSourceRegions().contains(bucketRegion))
        throw new IllegalArgumentException("Source bucket region " + bucketRegion + " is not allowed.");
      builder.setRegion(bucketRegion);
    }

    if (Config.instance != null && Config.instance.JOB_BOT_SECRET_ARN != null) {
      synchronized (S3Client.class) {
        builder.setCredentials(new SecretManagerCredentialsProvider(Config.instance.AWS_REGION,
           Config.instance.LOCALSTACK_ENDPOINT == null ? null : Config.instance.LOCALSTACK_ENDPOINT.toString(),
                Config.instance.JOB_BOT_SECRET_ARN));
      }
    }
    client = builder.build();
  }

  public static S3Client getInstance() {
    return getInstance(Config.instance.JOBS_S3_BUCKET);
  }

  public static S3Client getInstance(String bucketName) {
    if (!instances.containsKey(bucketName))
      instances.put(bucketName, new S3Client(bucketName));
    return instances.get(bucketName);
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

  public byte[] loadObjectContent(String s3Key) throws IOException {
    return loadObjectContent(s3Key, -1, -1);
  }

  public byte[] loadObjectContent(String s3Key, long offset, long length) throws IOException {
    return streamObjectContent(s3Key, offset, length).readAllBytes();
  }

  public InputStream streamObjectContent(String s3Key) {
    return streamObjectContent(s3Key, -1, -1);
  }

  public InputStream streamObjectContent(String s3Key, long offset, long length) {
    GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, s3Key);

    if (offset >= 0 && length >= 0)
      getObjectRequest.setRange(offset, length);
    else if (offset >= 0)
      getObjectRequest.setRange(offset);
    return client.getObject(getObjectRequest).getObjectContent();
  }

  public void putObject(String s3Key, String contentType, String content) throws IOException {
    putObject(s3Key, contentType, content.getBytes());
  }
  public void putObject(String s3Key, String contentType, byte[] content) throws IOException {
    putObject(s3Key, contentType, content,false);
  }

  public void putObject(String s3Key, String contentType, byte[] content, boolean gzip) throws IOException {
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentType(contentType);

    if (gzip) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(baos)) {
        gzipOutputStream.write(content);
      }

      metadata.setContentEncoding("gzip");
      metadata.setContentLength(baos.size());

      client.putObject(new PutObjectRequest(bucketName, s3Key,
              new ByteArrayInputStream(baos.toByteArray()), metadata));
    }
    else {
      metadata.setContentLength(content.length);
      client.putObject(new PutObjectRequest(bucketName, s3Key, new ByteArrayInputStream(content), metadata));
    }
  }

  public ObjectMetadata loadMetadata(String key) {
    return client.getObjectMetadata(bucketName, key);
  }

  public void deleteFolder(String folderPath) {
    //TODO: Run partially in parallel in multiple threads
    for (S3ObjectSummary file : scanFolder(folderPath))
      //TODO: Delete multiple objects (batches of 1000) with one request instead
      client.deleteObject(bucketName, file.getKey());
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

  /**
   * Checks if the provided S3 key is a folder.
   * A key is considered a folder if it has other objects under it
   *
   * @return True if the key is a folder, otherwise false.
   */
  public boolean isFolder(String s3Key) {
    // enforce prefix formatting for "folders"
    if (!s3Key.endsWith("/")) {
      s3Key += "/";
    }

    ListObjectsV2Request request = new ListObjectsV2Request()
            .withBucketName(bucketName)
            .withPrefix(s3Key)
            .withMaxKeys(2); // fetch up to 2 to differentiate a single self object and siblings

    ListObjectsV2Result result = client.listObjectsV2(request);

    // more than one object means it's a folder
    if (result.getKeyCount() > 1) {
      return true;
    }

    // exactly one object - check if it matches the key itself
    if (result.getKeyCount() == 1) {
      String onlyKey = result.getObjectSummaries().get(0).getKey();
      return onlyKey.equals(s3Key);
    }

    return false;
  }

  /**
   * Lists all objects starting with the specified prefix (recursively).
   * Useful for traversing folders in S3.
   *
   * @param prefix The prefix or "folder path" to list objects for.
   * @return A list of object keys under the specified prefix.
   */
  public List<String> listObjects(String prefix) {
    List<String> objectKeys = new ArrayList<>();
    String continuationToken = null;

    do {
      ListObjectsV2Request request = new ListObjectsV2Request()
              .withBucketName(bucketName)
              .withPrefix(prefix)
              .withContinuationToken(continuationToken);

      ListObjectsV2Result result = client.listObjectsV2(request);

      for (S3ObjectSummary s3Object : result.getObjectSummaries()) {
        objectKeys.add(s3Object.getKey());
      }

      continuationToken = result.getNextContinuationToken();
    } while (continuationToken != null);

    return objectKeys;
  }

  public static class S3Uri {
    private String bucket;
    private String key;
    private URI uri;

    public S3Uri(URI uri) {
      this.uri = uri;
    }

    @JsonCreator
    public S3Uri(String uri) {
      this(URI.create(uri));
    }

    public S3Uri(String bucket, String key) {
      assert bucket != null;
      assert key != null;
      this.bucket = bucket;
      this.key = key;
    }

    public String bucket() {
      if (bucket == null)
        bucket = uri.getHost();
      return bucket;
    }

    public String key() {
      if (key == null)
        key = uri.getPath().startsWith("/") ? uri.getPath().substring(1) : uri.getPath();
      return key;
    }

    public String uri() {
      if (uri == null)
        uri = URI.create("s3://" + bucket + "/" + key);
      return uri.toString();
    }

    @JsonValue
    @Override
    public String toString() {
      return uri().toString();
    }
  }
}
