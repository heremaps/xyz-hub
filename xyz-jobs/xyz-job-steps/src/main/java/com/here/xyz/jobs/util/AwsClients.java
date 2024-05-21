/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchevents.CloudWatchEventsClient;
import software.amazon.awssdk.services.sfn.SfnClient;

public class AwsClients {
  private static SfnClient sfnClient;
  private static CloudWatchEventsClient cloudwatchEventsClient;

  private static <T extends AwsClientBuilder> T prepareClientForLocalStack(T builder) {
    if (isLocal()) {
      builder.endpointOverride(Config.instance.LOCALSTACK_ENDPOINT);
      builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("localstack",
          "localstack")));
      builder.region(Region.of(Config.instance.AWS_REGION));
    }
    return builder;
  }

  public static SfnClient sfnClient() {
    if (sfnClient == null)
      sfnClient = prepareClientForLocalStack(SfnClient.builder()).build();
    return sfnClient;
  }

  public static CloudWatchEventsClient cloudwatchEventsClient() {
    if (cloudwatchEventsClient == null)
      cloudwatchEventsClient = prepareClientForLocalStack(CloudWatchEventsClient.builder()).build();
    return cloudwatchEventsClient;
  }

  private static boolean isLocal() {
    return Config.instance.LOCALSTACK_ENDPOINT != null;
  }
}
