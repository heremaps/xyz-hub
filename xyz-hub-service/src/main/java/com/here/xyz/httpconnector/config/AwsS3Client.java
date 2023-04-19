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
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.here.xyz.httpconnector.CService;

import java.io.IOException;
import java.net.URL;
import java.util.Date;


/**
 * A client for reading and writing from and to S3
 */
public class AwsS3Client {
    protected static final int PRESIGNED_URL_EXPIRATION_SECONDS = 7 * 24 * 60 * 60;
    protected static AWSCredentialsProvider customCredentialsProvider;

    protected final AmazonS3 client;

    public AwsS3Client() {
        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        final String region = CService.configuration != null ? CService.configuration.JOBS_REGION : "eu-west-1";
        builder.setRegion(region);

        if (CService.configuration != null && CService.configuration.USE_AWS_INSTANCE_CREDENTIALS_WITH_REFRESH) {
            synchronized(AwsS3Client.class) {
                if (customCredentialsProvider == null) {
                    customCredentialsProvider = InstanceProfileCredentialsProvider.createAsyncRefreshingProvider(true);
                }
                builder.setCredentials(customCredentialsProvider);
            }
        }
        client = builder.build();
    }

    public URL generateDownloadURL(String bucketName, String key) throws IOException {
        return generatePresignedUrl(bucketName, key, HttpMethod.GET);
    }

    public URL generateUploadURL(String bucketName, String key) throws IOException {
        return generatePresignedUrl(bucketName, key, HttpMethod.PUT);
    }

    public URL generatePresignedUrl(String bucketName, String key, HttpMethod method) throws IOException {
        GeneratePresignedUrlRequest generatePresignedUrlRequest =
                new GeneratePresignedUrlRequest(bucketName, key)
                        .withMethod(method)
                        .withExpiration(new Date(System.currentTimeMillis() + PRESIGNED_URL_EXPIRATION_SECONDS * 1000));

        return client.generatePresignedUrl(generatePresignedUrlRequest);
    }

    void deleteS3Folder(String bucketName, String folderPath) {
        for (S3ObjectSummary file : client.listObjects(bucketName, folderPath).getObjectSummaries()){
            client.deleteObject(bucketName, file.getKey());
        }
    }
}
