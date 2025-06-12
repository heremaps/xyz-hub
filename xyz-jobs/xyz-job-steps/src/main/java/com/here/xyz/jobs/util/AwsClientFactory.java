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

import com.here.xyz.util.service.aws.AwsClientFactoryBase;
import software.amazon.awssdk.services.cloudwatchevents.CloudWatchEventsClient;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.sfn.SfnAsyncClient;
import software.amazon.awssdk.services.sfn.SfnClient;

public class AwsClientFactory extends AwsClientFactoryBase {
  private static SfnClient sfnClient;
  private static SfnAsyncClient asyncSfnClient;
  private static CloudWatchEventsClient cloudwatchEventsClient;
  private static EmrServerlessClient emrServerlessClient;

  public static SfnClient sfnClient() {
    if (sfnClient == null)
      sfnClient = prepareClient(SfnClient.builder()).build();
    return sfnClient;
  }

  public static SfnAsyncClient asyncSfnClient() {
    if (asyncSfnClient == null)
      asyncSfnClient = prepareClient(SfnAsyncClient.builder()).build();
    return asyncSfnClient;
  }

  public static CloudWatchEventsClient cloudwatchEventsClient() {
    if (cloudwatchEventsClient == null)
      cloudwatchEventsClient = prepareClient(CloudWatchEventsClient.builder()).build();
    return cloudwatchEventsClient;
  }

  public static EmrServerlessClient emrServerlessClient() {
    if (emrServerlessClient == null)
      emrServerlessClient = prepareClient(EmrServerlessClient.builder()).build();
    return emrServerlessClient;
  }
}
