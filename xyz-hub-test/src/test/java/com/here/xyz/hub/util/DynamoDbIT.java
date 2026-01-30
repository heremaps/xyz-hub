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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;

// TODO: consider making DynamoBranchConfigClientIT extend this base class, too
@Testcontainers(disabledWithoutDocker = true)
abstract class DynamoDbIT {

  protected static final DockerImageName dynamoImage = DockerImageName.parse("amazon/dynamodb-local:2.5.2");

  @Container
  private static final GenericContainer<?> dynamoContainer =
    new GenericContainer<>(dynamoImage)
      .withExposedPorts(8000)
      .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb");

  protected static String endpoint;

  protected static AmazonDynamoDB rawDynamoClient;

  protected abstract String tableName();

  @BeforeAll
  static void beforeAll() {
    Core.vertx = Vertx.vertx();
    endpoint = "http://%s:%s".formatted(dynamoContainer.getHost(), dynamoContainer.getFirstMappedPort());
    rawDynamoClient = rawDynamo(endpoint);
  }

  @AfterEach
  void tearDown() {
    rawDynamoClient.deleteTable(tableName());
  }

  private static AmazonDynamoDB rawDynamo(String endpoint) {
    return AmazonDynamoDBClientBuilder
      .standard()
      .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "local"))
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("dummy", "dummy")))
      .build();
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
