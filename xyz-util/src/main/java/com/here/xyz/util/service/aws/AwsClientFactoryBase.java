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

public class AwsClientFactoryBase {

  protected static <T extends AwsClientBuilder> T prepareClient(T builder) {
    if (AwsClientFactoryBase.isLocal()) {
      builder.endpointOverride(BaseConfig.instance.LOCALSTACK_ENDPOINT);
      builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("localstack","localstack")));
      builder.region(Region.of(BaseConfig.instance.AWS_REGION));
    }
    return builder;
  }

  public static boolean isLocal() {
    return BaseConfig.instance.LOCALSTACK_ENDPOINT != null;
  }
}
