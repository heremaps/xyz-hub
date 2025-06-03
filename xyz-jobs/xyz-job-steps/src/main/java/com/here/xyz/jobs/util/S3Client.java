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
package com.here.xyz.jobs.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.util.service.aws.S3ObjectSummary;
import com.here.xyz.util.service.aws.SecretManagerCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

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
        final String defaultRegion = "eu-west-1";
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
                    .region(Region.of(defaultRegion))
                    .forcePathStyle(true);
            presignerBuilder
                    .endpointOverride(Config.instance.LOCALSTACK_ENDPOINT)
                    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                    .region(Region.of(defaultRegion))
                    .credentialsProvider(
                            software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                                    software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create("localstack", "localstack")
                            ));
        } else if (Config.instance != null && Config.instance.JOBS_S3_BUCKET.equals(bucketName)) {
            final String region = Config.instance != null ? Config.instance.AWS_REGION : defaultRegion; //TODO: Remove default value
            builder.region(Region.of(region));
            presignerBuilder.region(Region.of(region));
        } else {
            GetBucketLocationResponse bucketLocation = getInstance().client.getBucketLocation(GetBucketLocationRequest.builder().bucket(bucketName).build());
            String bucketRegion = bucketLocation.locationConstraintAsString();
            if (Config.instance.forbiddenSourceRegions().contains(bucketRegion))
                throw new IllegalArgumentException("Source bucket region " + bucketRegion + " is not allowed.");
            builder.region(Region.of(bucketRegion));
            presignerBuilder.region(Region.of(bucketRegion));
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

    public URL generateDownloadURL(String key) {
        return S3ClientHelper.generateDownloadURL(presigner, bucketName, key, Duration.ofSeconds(PRESIGNED_URL_EXPIRATION_SECONDS));
    }

    public URL generateUploadURL(String key) {
        return S3ClientHelper.generateUploadURL(presigner, bucketName, key, Duration.ofSeconds(PRESIGNED_URL_EXPIRATION_SECONDS));
    }

    public List<S3ObjectSummary> scanFolder(String folderPath) {
       return S3ClientHelper.scanFolder(client, bucketName, folderPath);
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
        return S3ClientHelper.streamObjectContent(client, bucketName, s3Key, offset, length);
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
                .contentLength((long) finalContent.length)
                .contentType(contentType);

        if (gzip) {
            requestBuilder.contentEncoding("gzip");
        }

        client.putObject(requestBuilder.build(), RequestBody.fromBytes(finalContent));
    }

    public HeadObjectResponse loadMetadata(String key) {
        return S3ClientHelper.loadMetadata(client, bucketName, key);
    }

    public void deleteFolder(String folderPath) {
        listObjects(folderPath)
                .stream()
                .parallel()
                .forEach((key) -> S3ClientHelper.deleteObject(client, bucketName, key));
    }

    /**
     * Checks if the provided S3 key is a folder.
     * A key is considered a folder if it has other objects under it
     *
     * @return True if the key is a folder, otherwise false.
     */
    public boolean isFolder(String s3Key) {
        return S3ClientHelper.checkIsFolder(client, bucketName, s3Key);
    }

    /**
     * Lists all objects starting with the specified prefix (recursively).
     * Useful for traversing folders in S3.
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

}
