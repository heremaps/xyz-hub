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

import static com.here.xyz.util.service.aws.AwsClientFactoryBase.isLocal;

import com.here.xyz.jobs.steps.Config;
import com.here.xyz.util.service.BaseConfig;
import com.here.xyz.util.service.aws.iam.SecretManagerCredentialsProvider;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class S3Client extends com.here.xyz.util.service.aws.s3.S3Client {
  private static final String DEFAULT_REGION = "eu-west-1"; //TODO: Remove default value
  private static Map<String, S3Client> instances = new ConcurrentHashMap<>();

  protected S3Client(String bucketName) {
    super(bucketName);

  }

  @Override
  protected AwsCredentialsProvider credentialsProvider() {
    if (Config.instance != null && Config.instance.JOB_BOT_SECRET_ARN != null) {
      return new SecretManagerCredentialsProvider(BaseConfig.instance.AWS_REGION,
          isLocal() ? BaseConfig.instance.LOCALSTACK_ENDPOINT.toString() : null, Config.instance.JOB_BOT_SECRET_ARN);
    }
    return super.credentialsProvider();
  }

  @Override
  public String region() {
    if (Config.instance.JOBS_S3_BUCKET.equals(bucketName))
      return BaseConfig.instance != null && BaseConfig.instance.AWS_REGION != null ? BaseConfig.instance.AWS_REGION : DEFAULT_REGION;

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
