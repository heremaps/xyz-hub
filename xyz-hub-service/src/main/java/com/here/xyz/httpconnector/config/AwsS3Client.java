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

import com.here.xyz.util.service.aws.S3ObjectSummary;
import io.vertx.core.http.HttpMethod;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.*;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.util.service.aws.SecretManagerCredentialsProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A client for reading and writing from and to S3
 */
public class AwsS3Client {
    private static final Logger logger = LogManager.getLogger();
    protected static final Duration PRESIGNED_URL_EXPIRATION = Duration.ofDays(7);

    protected final S3Client client;
    protected final S3Presigner presigner;

    public AwsS3Client() {
        S3ClientBuilder builder = S3Client.builder();

        AwsCredentialsProvider credentialsProvider;
        Region region = Region.of(CService.configuration != null ? CService.configuration.JOBS_REGION : "eu-west-1");

        boolean local = isLocal();

        if (local) {
            builder.endpointOverride(URI.create(CService.configuration.LOCALSTACK_ENDPOINT))
                    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build());
            credentialsProvider = StaticCredentialsProvider.create(
                    AwsBasicCredentials.create("localstack", "localstack"));
        } else {
            credentialsProvider = DefaultCredentialsProvider.create();

            if (CService.configuration != null && CService.configuration.JOB_BOT_SECRET_ARN != null) {
                synchronized (AwsS3Client.class) {
                    credentialsProvider = new SecretManagerCredentialsProvider(
                            region.toString(),
                            CService.configuration.LOCALSTACK_ENDPOINT,
                            CService.configuration.JOB_BOT_SECRET_ARN);
                }
            }
        }

        builder.region(region);
        builder.credentialsProvider(credentialsProvider);
        client = builder.build();

        S3Presigner.Builder presignerBuilder = S3Presigner.builder()
                .region(region)
                .credentialsProvider(credentialsProvider);

        if (local) {
            presignerBuilder
                    .endpointOverride(URI.create(CService.configuration.LOCALSTACK_ENDPOINT))
                    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build());
        }

        presigner = presignerBuilder.build();
    }

    public URL generateDownloadURL(String bucketName, String key) {
        return generatePresignedUrl(bucketName, key, HttpMethod.GET);
    }

    public URL generateUploadURL(String bucketName, String key) {
        return generatePresignedUrl(bucketName, key, HttpMethod.PUT);
    }

    public URL generatePresignedUrl(String bucketName, String key, HttpMethod method) {
        if (method == HttpMethod.GET) {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            return presigner.presignGetObject(GetObjectPresignRequest.builder()
                    .signatureDuration(PRESIGNED_URL_EXPIRATION)
                    .getObjectRequest(getObjectRequest)
                    .build()).url();
        } else if (method == HttpMethod.PUT) {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            return presigner.presignPutObject(PutObjectPresignRequest.builder()
                    .signatureDuration(PRESIGNED_URL_EXPIRATION)
                    .putObjectRequest(putObjectRequest)
                    .build()).url();
        } else {
            throw new IllegalArgumentException("Unsupported HTTP method for presigned URL: " + method);
        }
    }

    public void deleteS3Folder(String bucketName, String folderPath) {
        try {
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(folderPath)
                    .build();

            ListObjectsV2Response listResponse;

            do {
                listResponse = client.listObjectsV2(listObjectsV2Request);

                List<ObjectIdentifier> toDelete = new ArrayList<>();
                listResponse.contents().forEach(s3Object -> {
                    toDelete.add(ObjectIdentifier.builder().key(s3Object.key()).build());
                });

                if (!toDelete.isEmpty()) {
                    DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                            .bucket(bucketName)
                            .delete(Delete.builder().objects(toDelete).build())
                            .build();
                    client.deleteObjects(deleteObjectsRequest);
                }

                listObjectsV2Request = listObjectsV2Request.toBuilder()
                        .continuationToken(listResponse.nextContinuationToken())
                        .build();

            } while (listResponse.isTruncated());

        } catch (Exception e) {
            logger.error("Failed to delete folder '{}' in bucket '{}': {}", folderPath, bucketName, e.getMessage(), e);
        }
    }

    public void copyFolder(String bucketName, String sourceFolderPath, String targetFolderPath) {
        try {
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(sourceFolderPath)
                    .build();

            ListObjectsV2Response listResponse;

            do {
                listResponse = client.listObjectsV2(listObjectsV2Request);

                for (S3Object s3Object : listResponse.contents()) {
                    String sourceKey = s3Object.key();
                    String targetKey = sourceKey.replace(sourceFolderPath, targetFolderPath);

                    CopyObjectRequest copyRequest = CopyObjectRequest.builder()
                            .copySource(bucketName + "/" + sourceKey)
                            .destinationBucket(bucketName)
                            .destinationKey(targetKey)
                            .build();

                    client.copyObject(copyRequest);
                }

                listObjectsV2Request = listObjectsV2Request.toBuilder()
                        .continuationToken(listResponse.nextContinuationToken())
                        .build();
            } while (listResponse.isTruncated());

        } catch (Exception e) {
            logger.error("Failed to copy folder from '{}' to '{}' in bucket '{}': {}", sourceFolderPath, targetFolderPath, bucketName, e.getMessage(), e);
        }
    }

    public List<S3ObjectSummary> scanFolder(String bucketName, String folderPath) {
        logger.info("Scanning folder for bucket {} and path {} ...", bucketName, folderPath);

        List<S3Object> summaries = new ArrayList<>();
        try {
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(folderPath)
                    .build();

            ListObjectsV2Response listResponse;

            do {
                listResponse = client.listObjectsV2(listObjectsV2Request);
                summaries.addAll(listResponse.contents());

                listObjectsV2Request = listObjectsV2Request.toBuilder()
                        .continuationToken(listResponse.nextContinuationToken())
                        .build();

            } while (listResponse.isTruncated());
        } catch (Exception e) {
            logger.error("Error scanning folder {} in bucket {}: {}", folderPath, bucketName, e.getMessage(), e);
        }
        return summaries.stream().map((it) -> S3ObjectSummary.fromS3Object(it, bucketName)).collect(Collectors.toList());
    }

    public boolean isLocal() {
        if (CService.configuration.HUB_ENDPOINT.contains("localhost") ||
                CService.configuration.HUB_ENDPOINT.contains("xyz-hub:8080"))
            return true;
        return false;
    }
}