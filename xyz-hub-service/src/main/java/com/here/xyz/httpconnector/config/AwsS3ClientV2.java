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
package com.here.xyz.httpconnector.config;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.util.service.aws.SecretManagerCredentialsProviderV2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * A client for reading and writing from and to S3 based on AWS v2
 */
public class AwsS3ClientV2 {
    protected static final int PRESIGNED_URL_EXPIRATION_SECONDS = 7 * 24 * 60 * 60;
    private static final Logger logger = LogManager.getLogger();
    protected final S3Client client;
    protected final S3Presigner presigner;

    public AwsS3ClientV2() {
        final S3ClientBuilder builder = S3Client.builder();
        S3Presigner.Builder presignerBuilder = S3Presigner.builder()
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build());

        final String region = CService.configuration != null ? CService.configuration.JOBS_REGION : "eu-west-1";

        if (isLocal()) {
            URI endpointUri = URI.create(CService.configuration.LOCALSTACK_ENDPOINT);

            builder.endpointOverride(endpointUri)
                    .region(Region.of(CService.configuration.JOBS_REGION))
                    .credentialsProvider(
                            StaticCredentialsProvider.create(
                                    AwsBasicCredentials.create("localstack", "localstack")
                            )
                    )
                    .forcePathStyle(true);

            presignerBuilder.endpointOverride(endpointUri)
                    .region(Region.of(CService.configuration.JOBS_REGION))
                    .credentialsProvider(
                            StaticCredentialsProvider.create(
                                    AwsBasicCredentials.create("localstack", "localstack")
                            )
                    );
        } else {
            builder.region(Region.of(region));
            presignerBuilder.region(Region.of(region));
        }

        if (CService.configuration != null && CService.configuration.JOB_BOT_SECRET_ARN != null) {
            synchronized (AwsS3Client.class) {
                SecretManagerCredentialsProviderV2 credentialsProvider = new SecretManagerCredentialsProviderV2(CService.configuration.JOBS_REGION,
                        CService.configuration.LOCALSTACK_ENDPOINT, CService.configuration.JOB_BOT_SECRET_ARN);
                builder.credentialsProvider(credentialsProvider);
                presignerBuilder.credentialsProvider(credentialsProvider);
            }
        }

        client = builder.build();
        presigner = presignerBuilder.build();
    }

    public URL generateDownloadURL(String bucketName, String key) {
        GetObjectPresignRequest getObjectPresignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(Duration.ofSeconds(PRESIGNED_URL_EXPIRATION_SECONDS))
                .getObjectRequest(req -> req.bucket(bucketName).key(key))
                .build();

        PresignedGetObjectRequest presignedRequest = presigner.presignGetObject(getObjectPresignRequest);
        return presignedRequest.url();
    }

    public URL generateUploadURL(String bucketName, String key) {
        PutObjectPresignRequest putObjectPresignRequest = PutObjectPresignRequest.builder()
                .signatureDuration(Duration.ofSeconds(PRESIGNED_URL_EXPIRATION_SECONDS))
                .putObjectRequest(req -> req.bucket(bucketName).key(key))
                .build();

        PresignedPutObjectRequest presignedRequest = presigner.presignPutObject(putObjectPresignRequest);
        return presignedRequest.url();
    }

    public URL generatePresignedUrl(String bucketName, String key, com.amazonaws.HttpMethod method) {
        if (method == com.amazonaws.HttpMethod.GET) {
            return generateDownloadURL(bucketName, key);
        } else if (method == com.amazonaws.HttpMethod.PUT) {
            return generateUploadURL(bucketName, key);
        } else {
            throw new UnsupportedOperationException("HTTP method " + method + " is not supported");
        }
    }

    public void deleteS3Folder(String bucketName, String folderPath) {
        List<S3Object> objectsToDelete = scanFolder(bucketName, folderPath);
        for (S3Object obj : objectsToDelete) {
            client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(obj.key())
                    .build());
        }
    }

    public void copyFolder(String bucketName, String sourceFolderPath, String targetFolderPath) {
        List<S3Object> objectsToCopy = scanFolder(bucketName, sourceFolderPath);
        for (S3Object obj : objectsToCopy) {
            String objectPath = obj.key();
            String targetObjectPath = objectPath.replace(sourceFolderPath, targetFolderPath);

            client.copyObject(CopyObjectRequest.builder()
                    .sourceBucket(bucketName)
                    .sourceKey(objectPath)
                    .destinationBucket(bucketName)
                    .destinationKey(targetObjectPath)
                    .build());
        }
    }

    public List<S3Object> scanFolder(String bucketName, String folderPath) {
        logger.info("Scanning folder for bucket {} and path {} ...", bucketName, folderPath);

        ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(folderPath)
                .build();

        List<S3Object> objects = new ArrayList<>();
        ListObjectsV2Response response;
        String continuationToken = null;

        do {
            if (continuationToken != null) {
                listObjectsRequest = listObjectsRequest.toBuilder()
                        .continuationToken(continuationToken)
                        .build();
            }

            response = client.listObjectsV2(listObjectsRequest);
            objects.addAll(response.contents());
            continuationToken = response.nextContinuationToken();
        } while (response.isTruncated());

        return objects;
    }

    public boolean isLocal() {
        if (CService.configuration.HUB_ENDPOINT.contains("localhost") ||
                CService.configuration.HUB_ENDPOINT.contains("xyz-hub:8080"))
            return true;
        return false;
    }
}