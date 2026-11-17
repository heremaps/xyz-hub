/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Iceberg 1.10 is built against a newer AWS SDK than the one of the Hub. These tests build the clients the connector uses (S3 and
 * Glue, directly and with an assumed role) with the Hub's SDK, to catch missing methods or fields. No request is sent.
 */
class AwsSdkCompatibilityTest {

  private static final Map<String, String> STATIC_CREDENTIALS = Map.of(
      "client.region", "eu-west-1",
      "s3.access-key-id", "test",
      "s3.secret-access-key", "test");

  @Test
  void buildsTheDefaultClients() {
    assertClientsBuild(AwsClientFactories.from(STATIC_CREDENTIALS));
  }

  @Test
  void buildsTheClientsWithAnAssumedRole() {
    //Iceberg's AssumeRoleAwsClientFactory builds its STS client with the SDK's default region chain (client.assume-role.region only
    //applies to the S3 and Glue clients). Deployments get the region from AWS_REGION; the test sets it explicitly, so it doesn't
    //depend on the environment of the machine running it.
    String previousRegion = System.getProperty("aws.region");
    System.setProperty("aws.region", "eu-west-1");
    try {
      assertClientsBuild(AwsClientFactories.from(Map.of(
          "client.factory", "org.apache.iceberg.aws.AssumeRoleAwsClientFactory",
          "client.assume-role.arn", "arn:aws:iam::123456789012:role/test",
          "client.assume-role.region", "eu-west-1")));
    }
    finally {
      if (previousRegion == null)
        System.clearProperty("aws.region");
      else
        System.setProperty("aws.region", previousRegion);
    }
  }

  @Test
  void initializesTheS3FileIO() {
    try (S3FileIO io = new S3FileIO()) {
      io.initialize(Map.of("client.region", "eu-west-1", "s3.access-key-id", "test", "s3.secret-access-key", "test",
          "s3.endpoint", "http://localhost:4566", "s3.path-style-access", "true"));
      assertThat(io.newInputFile("s3://bucket/key").location()).isEqualTo("s3://bucket/key");
    }
  }

  private static void assertClientsBuild(AwsClientFactory factory) {
    try (S3Client s3 = factory.s3(); GlueClient glue = factory.glue()) {
      assertThat(s3).isNotNull();
      assertThat(glue).isNotNull();
    }
  }
}
