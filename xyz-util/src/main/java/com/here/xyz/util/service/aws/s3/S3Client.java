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

package com.here.xyz.util.service.aws.s3;

import com.amazonaws.regions.Regions;
import com.here.xyz.util.service.BaseConfig;
import com.here.xyz.util.service.aws.S3ObjectSummary;
import com.here.xyz.util.service.aws.S3Uri;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

public class S3Client {
  protected static final int PRESIGNED_URL_EXPIRATION_SECONDS = 7 * 24 * 60 * 60;
  private static Map<String, S3Client> instances = new ConcurrentHashMap<>();
  protected final software.amazon.awssdk.services.s3.S3Client client;
  protected final S3Presigner presigner;
  protected final String bucketName;
  protected String region;

  protected S3Client(String bucketName) {
    this.bucketName = bucketName;
    client = createClient(null);
    presigner = createPresigner();
  }

  protected String region() {
    if (region == null) {
      if (BaseConfig.instance != null && BaseConfig.instance.LOCALSTACK_ENDPOINT != null)
        region = BaseConfig.instance.AWS_REGION;
      else {
        String bucketRegion = identifyBucketRegion(bucketName);
        if (BaseConfig.instance.forbiddenSourceRegions().contains(bucketRegion))
          throw new IllegalArgumentException("Source bucket region " + bucketRegion + " is not allowed.");
        region = bucketRegion;
      }
    }

    return region;
  }

  protected String identifyBucketRegion(String bucketName) {
    String bucketRegion = Regions.US_EAST_1.getName();
    try {
      createClient(bucketRegion).headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
      return bucketRegion;
    }
    catch (S3Exception e) {
      bucketRegion = extractRegionFromHeadBucketRequestException(e);
    }
    return bucketRegion;
  }

  private static String extractRegionFromHeadBucketRequestException(S3Exception e) {
    SdkHttpResponse httpResponse = e.awsErrorDetails().sdkHttpResponse();
    if (httpResponse != null && httpResponse.firstMatchingHeader("x-amz-bucket-region").isPresent()) {
      String region = httpResponse.firstMatchingHeader("x-amz-bucket-region").get();
      return region;
    }
    else
      throw new IllegalArgumentException("The region of the bucket could not be identified.");
  }

  protected AwsCredentialsProvider credentialsProvider() {
    if (BaseConfig.instance != null && BaseConfig.instance.LOCALSTACK_ENDPOINT != null)
      return StaticCredentialsProvider.create(AwsBasicCredentials.create("localstack", "localstack"));
    return null; //Use the default one
  }

  private software.amazon.awssdk.services.s3.S3Client bucketLocationClient() {
    return software.amazon.awssdk.services.s3.S3Client.builder().build();
  }

  protected synchronized software.amazon.awssdk.services.s3.S3Client createClient(String region) {
    S3ClientBuilder builder = software.amazon.awssdk.services.s3.S3Client.builder();
    if (BaseConfig.instance != null && BaseConfig.instance.LOCALSTACK_ENDPOINT != null)
      builder.endpointOverride(BaseConfig.instance.LOCALSTACK_ENDPOINT).forcePathStyle(true);

    if (region == null)
      region = region();
    if (region != null)
      builder.region(Region.of(region));

    AwsCredentialsProvider credentialsProvider = credentialsProvider();
    if (credentialsProvider != null)
      builder.credentialsProvider(credentialsProvider);

    return builder.build();
  }

  protected synchronized S3Presigner createPresigner() {
    S3Presigner.Builder builder = S3Presigner.builder();

    if (BaseConfig.instance != null && BaseConfig.instance.LOCALSTACK_ENDPOINT != null)
      builder.endpointOverride(BaseConfig.instance.LOCALSTACK_ENDPOINT)
          .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build());

    String region = region();
    if (region != null)
      builder.region(Region.of(region));

    AwsCredentialsProvider credentialsProvider = credentialsProvider();
    if (credentialsProvider != null)
      builder.credentialsProvider(credentialsProvider);

    return builder.build();
  }

  public static S3Client getInstance(String bucketName) {
    if (!instances.containsKey(bucketName))
      instances.put(bucketName, new S3Client(bucketName));
    return instances.get(bucketName);
  }

  public static String getBucketFromS3Uri(String s3Uri) {
    return new S3Uri(s3Uri).bucket();
  }

  public static String getKeyFromS3Uri(String s3Uri) {
    return new S3Uri(s3Uri).key();
  }

  public URL generateDownloadURL(String key) {
    return S3ClientHelper.generateDownloadURL(presigner, bucketName, key, Duration.ofSeconds(PRESIGNED_URL_EXPIRATION_SECONDS));
  }

  public URL generateUploadURL(String key) {
    return S3ClientHelper.generateUploadURL(presigner, bucketName, key, Duration.ofSeconds(PRESIGNED_URL_EXPIRATION_SECONDS));
  }

  public byte[] loadObjectContent(String s3Key) throws IOException {
    return loadObjectContent(s3Key, -1, -1);
  }

  public byte[] loadObjectContent(String s3Key, long offset, long length) throws IOException {
    return streamObjectContent(s3Key, offset, length).readAllBytes();
  }

  public InputStream streamObjectContent(String s3Key, long offset, long length) {
    return S3ClientHelper.streamObjectContent(client, bucketName, s3Key, offset, length);
  }

  public InputStream streamObjectContent(String s3Key) {
    return streamObjectContent(s3Key, -1, -1);
  }

  public void putObject(String s3Key, String contentType, String content) throws IOException {
    putObject(s3Key, contentType, content.getBytes());
  }

  public void putObject(String s3Key, String contentType, byte[] content) throws IOException {
    putObject(s3Key, contentType, content, false);
  }

  public void putObject(String s3Key, String contentType, byte[] content, boolean gzip) throws IOException {
    byte[] finalContent = content;
    if (gzip) {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(content.length);
      try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
        gzipOutputStream.write(content);
      }
      finalContent = byteArrayOutputStream.toByteArray();
    }

    PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
        .bucket(bucketName)
        .key(s3Key)
        .contentLength((long) finalContent.length)
        .contentType(contentType);

    if (gzip)
      requestBuilder.contentEncoding("gzip");

    client.putObject(requestBuilder.build(), RequestBody.fromBytes(finalContent));
  }

  public HeadObjectResponse loadMetadata(String key) {
    return S3ClientHelper.loadMetadata(client, bucketName, key);
  }

  public void deleteFolder(String folderPath) {
    //TODO: Run partially in parallel in multiple threads (using an actual pool)
    listObjects(folderPath)
        .stream()
        .parallel()
        //TODO: Delete multiple objects (batches of 1000) with one request instead
        .forEach((key) -> S3ClientHelper.deleteObject(client, bucketName, key));
  }

  /**
   * Lists all objects starting with the specified prefix (recursively). Useful for traversing folders in S3.
   *
   * @param prefix The prefix or "folder path" to list objects for.
   * @return A list of object keys under the specified prefix.
   */
  public List<String> listObjects(String prefix) {
    List<S3ObjectSummary> objects = scanFolder(prefix);
    return objects.stream()
        .map(S3ObjectSummary::key)
        .collect(Collectors.toList());
  }

  public List<S3ObjectSummary> scanFolder(String folderPath) {
    return S3ClientHelper.scanFolder(client, bucketName, folderPath);
  }

  /**
   * Checks if the provided S3 key is a folder. A key is considered a folder if it has other objects under it
   *
   * @return True if the key is a folder, otherwise false.
   */
  public boolean isFolder(String s3Key) {
    return S3ClientHelper.checkIsFolder(client, bucketName, s3Key);
  }
}
