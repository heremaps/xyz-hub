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

import com.here.xyz.jobs.steps.Config;
import com.here.xyz.util.service.BaseConfig;
import com.here.xyz.util.service.aws.iam.SecretManagerCredentialsProvider;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class S3Client extends com.here.xyz.util.service.aws.s3.S3Client {
  private static final String DEFAULT_REGION = "eu-west-1"; //TODO: Remove default value

  private static Map<String, S3Client> instances = new ConcurrentHashMap<>();
  private AwsCredentialsProvider credentialsProvider;

  protected S3Client(String bucketName) {
    super(bucketName);
  }

  @Override
  protected synchronized AwsCredentialsProvider credentialsProvider() {
    if (Config.instance != null && Config.instance.JOB_BOT_SECRET_ARN != null) {
      if (credentialsProvider == null) {
        credentialsProvider = new SecretManagerCredentialsProvider(BaseConfig.instance.AWS_REGION,
            BaseConfig.instance.LOCALSTACK_ENDPOINT == null ? null : BaseConfig.instance.LOCALSTACK_ENDPOINT.toString(),
            Config.instance.JOB_BOT_SECRET_ARN);
      }
      return credentialsProvider;
    }

    return super.credentialsProvider();
  }

  @Override
  protected String region() {
    if (BaseConfig.instance != null && BaseConfig.instance.LOCALSTACK_ENDPOINT == null && Config.instance.JOBS_S3_BUCKET.equals(bucketName))
      region = BaseConfig.instance != null ? BaseConfig.instance.AWS_REGION : DEFAULT_REGION;

    return super.region();
  }

  public static S3Client getInstance() {
    return getInstance(Config.instance.JOBS_S3_BUCKET);
  }

  public static S3Client getInstance(String bucketName) {
    if (!instances.containsKey(bucketName))
      instances.put(bucketName, new S3Client(bucketName));
    return instances.get(bucketName);
  }
}
