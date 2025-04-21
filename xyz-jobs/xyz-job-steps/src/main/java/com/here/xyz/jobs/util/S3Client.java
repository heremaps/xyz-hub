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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.util.service.aws.S3ObjectSummary;
import com.here.xyz.util.service.aws.SecretManagerCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class S3Client {
    protected static final int PRESIGNED_URL_EXPIRATION_SECONDS = 7 * 24 * 60 * 60;
    private static Map<String, S3Client> instances = new ConcurrentHashMap<>();
    protected final software.amazon.awssdk.services.s3.S3Client client;
    protected final S3Presigner presigner;
    private final String bucketName;

    protected S3Client(String bucketName) {
        this.bucketName = bucketName;

        S3ClientBuilder builder = software.amazon.awssdk.services.s3.S3Client.builder();
        S3Presigner.Builder presignerBuilder = S3Presigner.builder();

        if (Config.instance != null && Config.instance.LOCALSTACK_ENDPOINT != null) {

            builder
                    .credentialsProvider(
                            software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                                    software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create("localstack", "localstack")
                            )
                    )
                    .endpointOverride(Config.instance.LOCALSTACK_ENDPOINT)
                    .forcePathStyle(true);
            presignerBuilder
                    .endpointOverride(Config.instance.LOCALSTACK_ENDPOINT)
                    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                    .credentialsProvider(
                            software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                                    software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create("localstack", "localstack")
                            ));
        } else if (Config.instance != null && Config.instance.JOBS_S3_BUCKET.equals(bucketName)) {
            final String region = Config.instance != null ? Config.instance.AWS_REGION : "eu-west-1"; //TODO: Remove default value
            builder.region(Region.of(region));
        } else {
            GetBucketLocationResponse bucketLocation = getInstance().client.getBucketLocation(GetBucketLocationRequest.builder().bucket(bucketName).build());
            String bucketRegion = bucketLocation.locationConstraintAsString();
            if (Config.instance.forbiddenSourceRegions().contains(bucketRegion))
                throw new IllegalArgumentException("Source bucket region " + bucketRegion + " is not allowed.");
            builder.region(Region.of(bucketRegion));
        }

        if (Config.instance != null && Config.instance.JOB_BOT_SECRET_ARN != null) {
            synchronized (S3Client.class) {
                builder.credentialsProvider(new SecretManagerCredentialsProvider(Config.instance.AWS_REGION,
                        Config.instance.LOCALSTACK_ENDPOINT == null ? null : Config.instance.LOCALSTACK_ENDPOINT.toString(),
                        Config.instance.JOB_BOT_SECRET_ARN));
                presignerBuilder.credentialsProvider(new SecretManagerCredentialsProvider(Config.instance.AWS_REGION,
                        Config.instance.LOCALSTACK_ENDPOINT == null ? null : Config.instance.LOCALSTACK_ENDPOINT.toString(),
                        Config.instance.JOB_BOT_SECRET_ARN));
            }
        }

        this.client = builder.build();
        this.presigner = presignerBuilder.build();
    }

    public static S3Client getInstance() {
        return getInstance(Config.instance.JOBS_S3_BUCKET);
    }

    public static S3Client getInstance(String bucketName) {
        if (!instances.containsKey(bucketName))
            instances.put(bucketName, new S3Client(bucketName));
        return instances.get(bucketName);
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

    private URL generatePresignedUrl(String key, SdkHttpMethod method) {
        if (method == SdkHttpMethod.GET) {
            return generateDownloadURL(key);
        } else if (method == SdkHttpMethod.PUT) {
            return generateUploadURL(key);
        } else {
            throw new IllegalArgumentException("Unsupported method: " + method);
        }
    }

    public URL generateDownloadURL(String key) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(Duration.ofSeconds(PRESIGNED_URL_EXPIRATION_SECONDS))
                .getObjectRequest(getObjectRequest)
                .build();

        PresignedGetObjectRequest presignedRequest = presigner.presignGetObject(presignRequest);
        return presignedRequest.url();
    }

    public URL generateUploadURL(String key) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                .signatureDuration(Duration.ofSeconds(PRESIGNED_URL_EXPIRATION_SECONDS))
                .putObjectRequest(putObjectRequest)
                .build();

        PresignedPutObjectRequest presignedRequest = presigner.presignPutObject(presignRequest);
        return presignedRequest.url();
    }

    public List<S3ObjectSummary> scanFolder(String folderPath) {
        ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder()
                .prefix(folderPath)
                .bucket(bucketName)
                .build();

        ListObjectsResponse listObjectsResponse = client.listObjects(listObjectsRequest);
        return listObjectsResponse.contents().stream()
                .map((it) -> S3ObjectSummary.fromS3Object(it, bucketName))
                .collect(Collectors.toList());
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
        GetObjectRequest.Builder builder = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key);

        if (offset > 0 && length > 0) {
            builder.range("bytes=" + offset + "-" + (offset + length - 1));
        }

        return client.getObject(builder.build());
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
            try (java.util.zip.GZIPOutputStream gzipOutputStream = new java.util.zip.GZIPOutputStream(byteArrayOutputStream)) {
                gzipOutputStream.write(content);
            }
            finalContent = byteArrayOutputStream.toByteArray();
        }

        PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .contentType(contentType);

        if (gzip) {
            requestBuilder.contentEncoding("gzip");
        }

        client.putObject(requestBuilder.build(), RequestBody.fromBytes(finalContent));
    }

    public HeadObjectResponse loadMetadata(String key) {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        return client.headObject(headObjectRequest);
    }

    public void deleteFolder(String folderPath) {
        //TODO: Run partially in parallel in multiple threads
        List<String> keys = listObjects(folderPath);

        for (String key : keys) {
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            client.deleteObject(deleteObjectRequest);
        }
    }

    /**
     * Checks if the provided S3 key is a folder.
     * A key is considered a folder if it has other objects under it
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

    /**
     * Lists all objects starting with the specified prefix (recursively).
     * Useful for traversing folders in S3.
     *
     * @param prefix The prefix or "folder path" to list objects for.
     * @return A list of object keys under the specified prefix.
     */
    public List<String> listObjects(String prefix) {
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .build();

        ListObjectsV2Response response = client.listObjectsV2(listRequest);
        return response.contents().stream()
                .map(S3Object::key)
                .collect(Collectors.toList());
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
