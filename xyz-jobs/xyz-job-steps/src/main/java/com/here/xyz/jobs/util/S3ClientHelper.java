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

import com.here.xyz.util.service.aws.S3ObjectSummary;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

    public static void deleteObject(S3Client client, String bucketName, String key) {
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        client.deleteObject(deleteObjectRequest);
    }

    public static InputStream streamObjectContent(S3Client client, String bucketName, String s3Key, long offset, long length) {
        GetObjectRequest.Builder builder = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key);

        if (offset > 0 && length > 0) {
            builder.range("bytes=" + offset + "-" + (offset + length - 1));
        }

        return client.getObject(builder.build());
    }

    public static boolean checkIsFolder(S3Client client, String bucketName, String s3Key) {
        String normalizedKey = s3Key.endsWith("/") ? s3Key : s3Key + "/";

        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(normalizedKey)
                .maxKeys(1)
                .build();

        ListObjectsV2Response response = client.listObjectsV2(listRequest);
        return !response.contents().isEmpty();
    }

    public static HeadObjectResponse loadMetadata(S3Client client, String bucketName, String key) {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        return client.headObject(headObjectRequest);
    }
}
