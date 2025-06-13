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

package com.here.xyz.util.service.aws;

import com.here.xyz.util.service.BaseConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

public class AwsClientFactoryBase {

  public static final StaticCredentialsProvider LOCAL_CREDENTIALS_PROVIDER = StaticCredentialsProvider.create(
      AwsBasicCredentials.create("localstack", "localstack"));

  protected static <T extends AwsClientBuilder> T prepareClient(T builder) {
    return prepareClient(builder, null);
  }

  protected static <T extends AwsClientBuilder> T prepareClient(T builder, String region) {
    if (AwsClientFactoryBase.isLocal()) {
      builder.endpointOverride(BaseConfig.instance.LOCALSTACK_ENDPOINT);
      builder.credentialsProvider(LOCAL_CREDENTIALS_PROVIDER);
      region = region == null ? BaseConfig.instance.AWS_REGION : region;
      if (region != null)
        builder.region(Region.of(region));
    }
    return builder;
  }

  public static boolean isLocal() {
    return BaseConfig.instance.LOCALSTACK_ENDPOINT != null;
  }

  public static S3ClientBuilder s3(String region) {
    S3ClientBuilder builder = prepareClient(S3Client.builder());
    if (isLocal())
      builder.forcePathStyle(true);
    return builder;
  }

  public static S3Presigner.Builder s3Presigner(String region) {
    S3Presigner.Builder builder = S3Presigner.builder();

    if (isLocal())
      builder
          .endpointOverride(BaseConfig.instance.LOCALSTACK_ENDPOINT)
          .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
          .credentialsProvider(LOCAL_CREDENTIALS_PROVIDER);

    region = region == null ? BaseConfig.instance.AWS_REGION : region;
    if (region != null)
      builder.region(Region.of(region));

    return builder;
  }
}
