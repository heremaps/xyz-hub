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

import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.util.service.aws.SecretManagerCredentialsProvider;
import java.net.URL;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A client for reading and writing from and to S3
 */
public class AwsS3Client {
    private static final Logger logger = LogManager.getLogger();
    protected static final int PRESIGNED_URL_EXPIRATION_SECONDS = 7 * 24 * 60 * 60;

    protected final AmazonS3 client;

    public AwsS3Client() {
        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();

        if (isLocal()) {
            builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                    CService.configuration.LOCALSTACK_ENDPOINT, CService.configuration.JOBS_REGION))
                    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("localstack", "localstack")))
                    .withPathStyleAccessEnabled(true);
        }
        else {
            final String region = CService.configuration != null ? CService.configuration.JOBS_REGION : "eu-west-1";
            builder.setRegion(region);
        }

        if (CService.configuration != null && CService.configuration.JOB_BOT_SECRET_ARN != null) {
            synchronized (AwsS3Client.class) {
                builder.setCredentials(new SecretManagerCredentialsProvider(CService.configuration.JOBS_REGION,
                    CService.configuration.LOCALSTACK_ENDPOINT, CService.configuration.JOB_BOT_SECRET_ARN));
            }
        }
        client = builder.build();
    }

    public URL generateDownloadURL(String bucketName, String key) {
        return generatePresignedUrl(bucketName, key, HttpMethod.GET);
    }

    public URL generateUploadURL(String bucketName, String key) {
        return generatePresignedUrl(bucketName, key, HttpMethod.PUT);
    }

    public URL generatePresignedUrl(String bucketName, String key, HttpMethod method) {
        GeneratePresignedUrlRequest generatePresignedUrlRequest =
                new GeneratePresignedUrlRequest(bucketName, key)
                    .withMethod(method)
                    .withExpiration(new Date(System.currentTimeMillis() + PRESIGNED_URL_EXPIRATION_SECONDS * 1000));

        return client.generatePresignedUrl(generatePresignedUrlRequest);
    }

    public void deleteS3Folder(String bucketName, String folderPath) {
        for (S3ObjectSummary file : client.listObjects(bucketName, folderPath).getObjectSummaries()){
            client.deleteObject(bucketName, file.getKey());
        }
    }

    public void copyFolder(String bucketName, String sourceFolderPath, String targetFolderPath) {
        for (S3ObjectSummary summary : scanFolder(bucketName, sourceFolderPath)) {
            String objectPath = summary.getKey();
            String targetObjectPath = objectPath.replace(sourceFolderPath, targetFolderPath);
            client.copyObject(bucketName, objectPath, bucketName, targetObjectPath);
        }
    }

    public List<S3ObjectSummary> scanFolder(String bucketName, String folderPath) {
        logger.info("Scanning folder for bucket {} and path {} ...", bucketName, folderPath);

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

    public boolean isLocal() {
        if(CService.configuration.HUB_ENDPOINT.contains("localhost") ||
                CService.configuration.HUB_ENDPOINT.contains("xyz-hub:8080"))
            return true;
        return false;
    }
}
