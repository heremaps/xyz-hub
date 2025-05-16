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
package com.here.xyz.hub.cache;

import com.here.xyz.jobs.util.S3ClientHelper;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import com.here.xyz.hub.Service;
import com.here.xyz.util.service.Core;
import io.vertx.core.Future;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class S3CacheClient implements CacheClient {
    private static final String EXPIRES_AT = "expiresAt";
    private static final String LAST_ACCESSED_AT = "lastAccessedAt";
    private static final long ACCESS_UPDATE_TIME_THRESHOLD = TimeUnit.DAYS.toMillis(1);
    private static final Logger logger = LogManager.getLogger();
    private static final String prefix = "xyz-hub-cache/";
    private static CacheClient instance;
    private volatile S3Client s3client;
    private String bucket;

    private S3CacheClient() {
        if (Service.configuration.XYZ_HUB_S3_BUCKET == null)
            throw new RuntimeException("No S3 bucket defined. S3CacheClient can not be used.");
        bucket = Service.configuration.XYZ_HUB_S3_BUCKET;
        initS3Client();
    }

    public static synchronized CacheClient getInstance() {
        try {
            if (instance == null)
                instance = new S3CacheClient();
        } catch (Exception e) {
            logger.error("Error when trying to create the S3 client.", e);
            instance = new NoopCacheClient();
        }
        return instance;
    }

    private void initS3Client() {
        S3ClientBuilder builder = S3Client.builder()
                .credentialsProvider(DefaultCredentialsProvider.create());

        if (Service.configuration.LOCALSTACK_ENDPOINT != null) {
            builder
                    .region(Region.EU_WEST_1)
                    .endpointOverride(URI.create(Service.configuration.LOCALSTACK_ENDPOINT))
                    .credentialsProvider(
                            StaticCredentialsProvider.create(
                                    AwsBasicCredentials.create("localstack", "localstack")))
                    .forcePathStyle(true);
        }

        if (Service.configuration.AWS_REGION != null && !Service.configuration.AWS_REGION.isEmpty()) {
            builder.region(Region.of(Service.configuration.AWS_REGION));
        }

        s3client = builder.build();
    }

    @Override
    public Future<byte[]> get(String key) {
        return Core.vertx.executeBlocking(promise -> {
            try {
                GetObjectRequest request = GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(prefix + key)
                        .build();

                ResponseBytes<GetObjectResponse> payload = s3client.getObject(request, ResponseTransformer.toBytes());

                Map<String, String> metadata = payload.response().metadata();
                if (metadata.containsKey(LAST_ACCESSED_AT.toLowerCase())) {
                    // Update the "lastAccessedAt" metadata field asynchronously
                    updateLastAccessedAt(key, metadata, Core.currentTimeMillis());
                }

                promise.complete(payload.asByteArray());
            } catch (NoSuchKeyException e) {
                logger.warn("Cache miss: S3 key not found {}", key);
                promise.complete(null);
            } catch (Exception e) {
                logger.error("Exception trying to read S3 object with key {}.", key, e);
                promise.complete(null);
            }
        });
    }

    @Override
    public void set(String key, byte[] value, long ttl) {
        Core.vertx.executeBlocking(promise -> {
            try {
                final long now = Core.currentTimeMillis();
                Map<String, String> metadata = new HashMap<>();
                metadata.put(EXPIRES_AT.toLowerCase(), "" + (now + TimeUnit.SECONDS.toMillis(ttl)));
                metadata.put(LAST_ACCESSED_AT.toLowerCase(), "" + now);

                PutObjectRequest request = PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(prefix + key)
                        .contentLength((long) value.length)
                        .metadata(metadata)
                        .build();

                s3client.putObject(request, RequestBody.fromBytes(value));
                promise.complete();
            } catch (Exception e) {
                logger.error("Exception trying to write S3 object with key {}.", key, e);
                promise.fail(e);
            }
        }, false);
    }

    private void updateLastAccessedAt(String key, Map<String, String> existingMetadata, long lastAccessedAt) {
        // Only perform the update if the last update was not done too recently (to save requests)
        String oldAccessedAtStr = existingMetadata.get(LAST_ACCESSED_AT.toLowerCase());
        if (oldAccessedAtStr == null) return;

        long oldAccessedAt = Long.parseLong(oldAccessedAtStr) + ACCESS_UPDATE_TIME_THRESHOLD;
        if (lastAccessedAt - ACCESS_UPDATE_TIME_THRESHOLD < oldAccessedAt)
            return;

        Core.vertx.executeBlocking(promise -> {
            try {
                Map<String, String> newMetadata = new HashMap<>(existingMetadata);
                newMetadata.put(LAST_ACCESSED_AT.toLowerCase(), "" + lastAccessedAt);

                CopyObjectRequest copyRequest = CopyObjectRequest.builder()
                        .sourceBucket(bucket)
                        .sourceKey(prefix + key)
                        .destinationBucket(bucket)
                        .destinationKey(prefix + key)
                        .metadata(newMetadata)
                        .metadataDirective(software.amazon.awssdk.services.s3.model.MetadataDirective.REPLACE)
                        .build();

                s3client.copyObject(copyRequest);
                promise.complete();
            } catch (Exception e) {
                logger.error("Exception trying to update metadata for S3 object with key {}.", key, e);
                promise.fail(e);
            }
        }, false);
    }

    @Override
    public void remove(String key) {
        Core.vertx.executeBlocking(promise -> {
            S3ClientHelper.deleteObject(s3client, bucket, prefix + key);
            promise.complete();
        }, false);
    }

    @Override
    public void shutdown() {
        instance = null;
        s3client.close();
    }
}