package com.here.xyz.util.service.aws;

import software.amazon.awssdk.services.s3.model.S3Object;

public record S3ObjectSummary(String key, String bucket, long size) {
    public boolean isEmpty() {
        return size == 0;
    }
    public static S3ObjectSummary fromS3Object(S3Object s3Object, String bucketName) {
        return new S3ObjectSummary(s3Object.key(), bucketName, s3Object.size());
    }
}
