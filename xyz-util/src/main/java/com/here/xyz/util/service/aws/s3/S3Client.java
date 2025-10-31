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

import static com.here.xyz.Payload.isGzipped;
import static com.here.xyz.util.service.aws.AwsClientFactoryBase.s3;
import static com.here.xyz.util.service.aws.AwsClientFactoryBase.s3Presigner;

import com.amazonaws.regions.Regions;
import com.here.xyz.util.pagination.Page;
import com.here.xyz.util.service.BaseConfig;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;
import org.apache.http.ConnectionClosedException;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest.Builder;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
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

    S3ClientBuilder s3ClientBuilder = s3(region());
    S3Presigner.Builder s3PresignerBuilder = s3Presigner(region());

    //Check if there is an override for the credentials-provider from the subclass
    AwsCredentialsProvider credentialsProvider = credentialsProvider();
    if (credentialsProvider != null) {
      s3ClientBuilder.credentialsProvider(credentialsProvider);
      s3PresignerBuilder.credentialsProvider(credentialsProvider);
    }

    client = s3ClientBuilder.build();
    presigner = s3PresignerBuilder.build();
  }

  protected AwsCredentialsProvider credentialsProvider() {
    return null;
  }

  public String region() {
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
    try (software.amazon.awssdk.services.s3.S3Client awsS3Client = s3(bucketRegion).build()) {
      awsS3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
    }
    catch (S3Exception e) {
      SdkHttpResponse httpResponse = e.awsErrorDetails().sdkHttpResponse();
      if (httpResponse != null && httpResponse.firstMatchingHeader("x-amz-bucket-region").isPresent())
        bucketRegion = httpResponse.firstMatchingHeader("x-amz-bucket-region").get();
      else
        throw new IllegalArgumentException("The region of the bucket could not be identified.");
    }
    return bucketRegion;
  }

  public synchronized static S3Client getInstance(String bucketName) {
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

  public S3ObjectWithMetadata loadObjectContentWithMetadata(String s3Key) throws IOException {
    Builder builder = GetObjectRequest.builder()
        .bucket(bucketName)
        .key(s3Key);

    ResponseInputStream<GetObjectResponse> objectResponseStream = client.getObject(builder.build());
    try (objectResponseStream) {
      return new S3ObjectWithMetadata(objectResponseStream.readAllBytes(), objectResponseStream.response().contentType(),
          objectResponseStream.response().metadata());
    }
  }

  public byte[] loadObjectContent(String s3Key) throws IOException {
    return loadObjectContent(s3Key, -1, -1);
  }

  public byte[] loadObjectContent(String s3Key, long offset, long length) throws IOException {
    return streamObjectContent(s3Key, offset, length).readAllBytes();
  }

  /**
   * Read an S3 object using an {@link ResponseInputStream}.
   *
   * @see #abortS3Streaming(ResponseInputStream)
   *
   * @param s3Key The key of the object to be read
   * @param offset The start offset (byte position) from where to start reading
   * @param length The amount of bytes to read starting at the offset
   * @return A {@link ResponseInputStream} that can be used to read the data from the response-
   * NOTE: If reading that stream should be stopped before reaching its end, the method {@link #abortS3Streaming(ResponseInputStream)}
   * must be called. Only calling the stream's {@link ResponseInputStream#close()} is not sufficient, because that call would block
   * due to an internal task in the SDK that continues to read the whole object till the end.
   */
  public ResponseInputStream<GetObjectResponse> streamObjectContent(String s3Key, long offset, long length) {
    Builder builder = GetObjectRequest.builder()
            .bucket(bucketName)
            .key(s3Key);

    if (offset > 0 && length > 0)
        builder.range("bytes=" + offset + "-" + (offset + length - 1));

    return client.getObject(builder.build());
  }

  /**
   * Read an S3 object using an {@link ResponseInputStream}.
   *
   * @see #abortS3Streaming(ResponseInputStream)
   *
   * @param s3Key The key of the object to be read
   * @return A {@link ResponseInputStream} that can be used to read the data from the response-
   * NOTE: If reading that stream should be stopped before reaching its end, the method {@link #abortS3Streaming(ResponseInputStream)}
   * must be called. Only calling the stream's {@link ResponseInputStream#close()} is not sufficient, because that call would block
   * due to an internal task in the SDK that continues to read the whole object till the end.
   */
  public ResponseInputStream<GetObjectResponse> streamObjectContent(String s3Key) {
    return streamObjectContent(s3Key, -1, -1);
  }

  /**
   * Important: This method *must* be called when wanting to stop reading an {@link ResponseInputStream} before reaching its end.
   * Otherwise, calling the method {@link ResponseInputStream#close()} would block until the SDK read the full S3 object.
   *
   * @see #streamObjectContent(String)
   * @see #streamObjectContent(String, long, long)
   *
   * @param s3InputStream The input stream as it has been returned by {@link #streamObjectContent(String)}
   *  or {@link #streamObjectContent(String, long, long)}
   * @throws IOException
   */
  public static void abortS3Streaming(ResponseInputStream s3InputStream) throws IOException {
    //Abort the running HTTP request from the AWS SDK (That provokes a ConnectionClosedException however)
    s3InputStream.abort();
    try {
      s3InputStream.close();
    }
    catch (ConnectionClosedException ignore) {}
  }

  public void putObject(String s3Key, String contentType, String content) throws IOException {
    putObject(s3Key, contentType, content.getBytes());
  }

  public void putObject(String s3Key, String contentType, byte[] content) throws IOException {
    putObject(s3Key, contentType, content, false);
  }

  public void putObject(String s3Key, String contentType, byte[] content, boolean gzip) throws IOException {
    putObject(s3Key, contentType, content, gzip, null);
  }

  public void putObject(String s3Key, String contentType, byte[] content, boolean gzip, Map<String, String> metadata) throws IOException {
    if (gzip && !isGzipped(content)) {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(content.length);
      try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
        gzipOutputStream.write(content);
      }
      content = byteArrayOutputStream.toByteArray();
    }

    PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
        .bucket(bucketName)
        .key(s3Key)
        .contentLength((long) content.length)
        .contentType(contentType);

    if (gzip)
      requestBuilder.contentEncoding("gzip");

    if (metadata != null && !metadata.isEmpty())
      requestBuilder.metadata(metadata);

    client.putObject(requestBuilder.build(), RequestBody.fromBytes(content));
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

  /**
   * Retrieves all objects from a specified folder path in an S3 bucket
   * Note: This method automatically handles pagination
   */
  public List<S3ObjectSummary> scanFolder(String folderPath) {
    return S3ClientHelper.scanFolder(client, bucketName, folderPath);
  }

  /**
   * Retrieves objects from a specified folder path in an S3 bucket with pagination support
   * Note: This version allows for controlled pagination through the nextPageToken and limit parameters
   */
  public Page<S3ObjectSummary> scanFolder(String folderPath, String nextPageToken, int limit) {
    return S3ClientHelper.scanFolder(client, bucketName, folderPath, nextPageToken, limit);
  }

  /**
   * Checks if the provided S3 key is a folder. A key is considered a folder if it has other objects under it
   *
   * @return True if the key is a folder, otherwise false.
   */
  public boolean isFolder(String s3Key) {
    String normalizedKey = s3Key.endsWith("/") ? s3Key : s3Key + "/";

    ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
            .bucket(bucketName)
            .prefix(normalizedKey)
            .maxKeys(1)
            .build();

    ListObjectsV2Response response = client.listObjectsV2(listRequest);
    return !response.contents().isEmpty();
  }

  public static class S3ObjectWithMetadata {
    private final byte[] content;
    private final String contentType;
    private final Map<String, String> metadata;

    public S3ObjectWithMetadata(byte[] content, String contentType, Map<String, String> metadata) {
      this.content = content;
      this.contentType = contentType;
      this.metadata = metadata;
    }

    public byte[] content() {
      return content;
    }

    public String contentType() {
      return contentType;
    }

    public Map<String, String> metadata() {
      return metadata;
    }
  }
}
