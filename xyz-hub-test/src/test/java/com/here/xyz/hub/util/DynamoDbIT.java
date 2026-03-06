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

package com.here.xyz.hub.util;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.here.xyz.util.service.Core;
import io.vertx.core.Vertx;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;

abstract class DynamoDbIT {

  private static final String DEFAULT_DYNAMO_ENDPOINT = "http://localhost:8000";
  private static final String DYNAMO_ENDPOINT_PROPERTY = "xyz.test.dynamodb.endpoint";
  private static final String DYNAMO_ENDPOINT_ENV = "XYZ_TEST_DYNAMODB_ENDPOINT";

  protected static String endpoint;
  protected static AmazonDynamoDB rawDynamoClient;

  protected abstract String tableName();

  @BeforeAll
  static void beforeAll() {
    if (Core.vertx == null) {
      Core.vertx = Vertx.vertx();
    }

    endpoint = System.getProperty(
      DYNAMO_ENDPOINT_PROPERTY,
      System.getenv().getOrDefault(DYNAMO_ENDPOINT_ENV, DEFAULT_DYNAMO_ENDPOINT)
    );

    rawDynamoClient = rawDynamo(endpoint);
    assumeDynamoAvailable();
  }

  @AfterEach
  void tearDown() {
    try {
      rawDynamoClient.deleteTable(tableName());
    }
    catch (ResourceNotFoundException ignored) {
      // Table may already be deleted
    }
  }

  private static AmazonDynamoDB rawDynamo(String endpoint) {
    return AmazonDynamoDBClientBuilder
      .standard()
      .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "local"))
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("dummy", "dummy")))
      .build();
  }

  private static void assumeDynamoAvailable() {
    try {
      Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(100))
        .ignoreExceptions()
        .until(() -> {
          rawDynamoClient.listTables();
          return true;
        });
    }
    catch (ConditionTimeoutException e) {
      Assumptions.assumeTrue(false, "Skipping Dynamo integration tests: local DynamoDB is not reachable at " + endpoint);
    }
  }

  protected void awaitTableActive() {
    Awaitility.await()
      .atMost(Duration.ofSeconds(5))
      .pollInterval(Duration.ofMillis(50))
      .until(this::tableActive);
  }

  private boolean tableActive() {
    try {
      return "ACTIVE".equalsIgnoreCase(tableStatus());
    } catch (ResourceNotFoundException ex) {
      return false;
    }
  }

  private String tableStatus() {
    DescribeTableResult describeTableResult = rawDynamoClient.describeTable(tableName());
    return describeTableResult.getTable().getTableStatus();
  }

  protected void storeInDb(Map<String, Object> itemToStore) {
    rawDynamoClient.putItem(tableName(), ItemUtils.fromSimpleMap(itemToStore));
  }

  protected static List<Map<String, Object>> fetchFromDb(ScanRequest scanRequest) {
    return rawDynamoClient.scan(scanRequest)
      .getItems()
      .stream()
      .map(ItemUtils::toSimpleMapValue)
      .toList();
  }
}
