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

import com.here.xyz.util.pagination.Page;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

//TODO: Move all methods into S3Client once AwsS3Client has been removed
@Deprecated
public class S3ClientHelper {

    public static URL generateDownloadURL(S3Presigner presigner, String bucketName, String key, Duration duration) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        return presigner.presignGetObject(GetObjectPresignRequest.builder()
                .signatureDuration(duration)
                .getObjectRequest(getObjectRequest)
                .build()).url();
    }

    public static URL generateUploadURL(S3Presigner presigner, String bucketName, String key, Duration duration) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        return presigner.presignPutObject(PutObjectPresignRequest.builder()
                .signatureDuration(duration)
                .putObjectRequest(putObjectRequest)
                .build()).url();
    }

    /**
     * Scan all contents of a folder in an S3 bucket.
     */
    public static List<S3ObjectSummary> scanFolder(S3Client client, String bucketName, String folderPath) {
        List<S3Object> summaries = new ArrayList<>();
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

        return summaries.stream().map((it) -> S3ObjectSummary.fromS3Object(it, bucketName)).collect(Collectors.toList());
    }

    /**
     * Scan a folder in an S3 bucket with pagination support.
     */
    public static Page<S3ObjectSummary> scanFolder(S3Client client, String bucketName, String folderPath,
        String nextPageToken, int limit) {
        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
            .bucket(bucketName)
            .prefix(folderPath);

        if (nextPageToken != null && !nextPageToken.isEmpty()) {
            requestBuilder.continuationToken(nextPageToken);
        }
        if (limit > 0) {
            requestBuilder.maxKeys(limit);
        }

        ListObjectsV2Response response = client.listObjectsV2(requestBuilder.build());
        List<S3Object> responseItems = response.contents();

        Page<S3ObjectSummary> summaries = new Page<>();
        summaries.setNextPageToken(response.isTruncated() ? response.nextContinuationToken() : null);
        summaries.setItems(
            responseItems.stream()
                .map((it) -> S3ObjectSummary.fromS3Object(it, bucketName))
                .collect(Collectors.toList()));

        return summaries;
    }

    public static void deleteObject(S3Client client, String bucketName, String key) {
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        client.deleteObject(deleteObjectRequest);
    }

    public static HeadObjectResponse loadMetadata(S3Client client, String bucketName, String key) {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        return client.headObject(headObjectRequest);
    }
}
