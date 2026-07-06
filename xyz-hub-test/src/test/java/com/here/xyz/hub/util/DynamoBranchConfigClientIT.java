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

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.here.xyz.hub.Config;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.dynamo.DynamoBranchConfigClient;
import com.here.xyz.models.hub.Branch;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.DisplayName.class)
public class DynamoBranchConfigClientIT extends DynamoDbIT {

  private static final String TABLE_NAME = "branches";
  private static final String TABLE_ARN = "arn:aws:dynamodb:localhost:000000008000:table/" + TABLE_NAME;

  private DynamoBranchConfigClient client;

  @BeforeAll
  static void configureService() {
    Service.configuration = new Config();
    Service.configuration.BRANCHES_DYNAMODB_TABLE_ARN = TABLE_ARN;
    Service.configuration.ENTITIES_DYNAMODB_TABLE_ARN = "arn:aws:dynamodb:localhost:000000008000:table/xyz-hub-local-entities";
  }

  @BeforeEach
  void setUp() {
    client = new DynamoBranchConfigClient(TABLE_ARN);
    client.init().toCompletionStage().toCompletableFuture().join();
    awaitTableActive();
  }

  @Override
  protected String tableName() {
    return TABLE_NAME;
  }

  private boolean tableExists(String tableName) {
    try {
      rawDynamoClient.describeTable(tableName);
      return true;
    }
    catch (ResourceNotFoundException e) {
      return false;
    }
  }

  private void waitUntilDeleted(String tableName) {
    Awaitility.await()
      .atMost(Duration.ofSeconds(5))
      .pollInterval(Duration.ofMillis(50))
      .until(() -> {
        try {
          rawDynamoClient.describeTable(tableName);
          return false;
        }
        catch (ResourceNotFoundException e) {
          return true;
        }
      });
  }

  private static Branch newBranch(String id) {
    return new Branch()
      .withId(id)
      .withBranchPath(List.of());
  }

  @Test
  @DisplayName("1. init() creates table & GSI on local")
  void initCreatesTableOnLocal() {
    if (tableExists(TABLE_NAME)) {
      rawDynamoClient.deleteTable(TABLE_NAME);
      waitUntilDeleted(TABLE_NAME);
    }

    client.init().toCompletionStage().toCompletableFuture().join();
    awaitTableActive();

    TableDescription td = rawDynamoClient.describeTable(TABLE_NAME).getTable();
    assertThat(td.getTableName()).isEqualTo(TABLE_NAME);
    assertThat(td.getKeySchema())
      .extracting(KeySchemaElement::getAttributeName)
      .containsExactlyInAnyOrder("spaceId", "id");

    List<GlobalSecondaryIndexDescription> gsis = td.getGlobalSecondaryIndexes();
    assertThat(gsis).isNotNull();
    assertThat(gsis).anySatisfy(gsi ->
      assertThat(gsi.getKeySchema())
        .extracting(KeySchemaElement::getAttributeName)
        .contains("id"));
  }

  @Test
  @DisplayName("2. store/load/delete roundtrip")
  void storeLoadDeleteRoundTrip() {
    String spaceId = "space-" + ThreadLocalRandom.current().nextInt(1_000_000);
    String branchName = "b1";

    client.store(spaceId, newBranch(branchName), branchName).toCompletionStage().toCompletableFuture().join();

    var all = client.load(spaceId).toCompletionStage().toCompletableFuture().join();
    assertThat(all).extracting(Branch::getId).contains(branchName);

    Branch loaded = client.load(spaceId, branchName).toCompletionStage().toCompletableFuture().join();
    assertThat(loaded).isNotNull();
    assertThat(loaded.getId()).isEqualTo(branchName);

    client.delete(spaceId, branchName, true).toCompletionStage().toCompletableFuture().join();
    Branch afterDelete = client.load(spaceId, branchName).toCompletionStage().toCompletableFuture().join();
    assertThat(afterDelete).isNull();
  }

  @Test
  @DisplayName("3. store() with rename deletes old and creates new")
  void storeWithRenameDeletesOldAndCreatesNew() {
    String spaceId = "space-" + ThreadLocalRandom.current().nextInt(1_000_000);

    client.store(spaceId, newBranch("old"), "old").toCompletionStage().toCompletableFuture().join();
    client.store(spaceId, newBranch("new"), "old").toCompletionStage().toCompletableFuture().join();

    Branch oldB = client.load(spaceId, "old").toCompletionStage().toCompletableFuture().join();
    Branch newB = client.load(spaceId, "new").toCompletionStage().toCompletableFuture().join();
    assertThat(oldB).isNull();
    assertThat(newB).isNotNull();
    assertThat(newB.getId()).isEqualTo("new");
  }

  @Test
  @DisplayName("4. load(space) returns many items")
  void loadMultipleBranchesPagination() {
    String spaceId = "space-" + ThreadLocalRandom.current().nextInt(1_000_000);

    for (int i = 0; i < 35; i++) {
      client.store(spaceId, newBranch("b-" + i), "b-" + i).toCompletionStage().toCompletableFuture().join();
    }

    var all = client.load(spaceId).toCompletionStage().toCompletableFuture().join();
    assertThat(all.size()).isGreaterThanOrEqualTo(35);
  }
}
