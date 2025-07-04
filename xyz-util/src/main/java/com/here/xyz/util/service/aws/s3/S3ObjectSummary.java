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

import software.amazon.awssdk.services.s3.model.S3Object;

public record S3ObjectSummary(String key, String bucket, long size) {
    public boolean isEmpty() {
        return size == 0;
    }
    public static S3ObjectSummary fromS3Object(S3Object s3Object, String bucketName) {
        return new S3ObjectSummary(s3Object.key(), bucketName, s3Object.size());
    }
}
