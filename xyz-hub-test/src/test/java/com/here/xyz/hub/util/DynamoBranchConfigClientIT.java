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

package com.here.xyz.hub.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.here.xyz.hub.config.dynamo.DynamoBranchConfigClient;
import com.here.xyz.models.hub.Branch;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.aws.dynamo.DynamoClient;
import io.vertx.core.Vertx;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.DisplayName.class)
@Testcontainers(disabledWithoutDocker = true)
public class DynamoBranchConfigClientIT {

    private static final DockerImageName DDB_IMAGE =
            DockerImageName.parse("amazon/dynamodb-local:2.5.2");

    @Container
    public static final GenericContainer<?> dynamo =
            new GenericContainer<>(DDB_IMAGE)
                    .withExposedPorts(8000)
                    .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb");

    private Vertx vertx;
    private DynamoBranchConfigClient client;
    private AmazonDynamoDB rawDdb;
    private String endpoint;

    private static final String TABLE_NAME = "branches";
    private static final String TABLE_ARN = "arn:aws:dynamodb:local:000000000000:table/" + TABLE_NAME;

    @BeforeAll
    void beforeAll() {
        vertx = Vertx.vertx();
        Core.vertx = vertx;
    }

    @AfterAll
    void afterAll() {
        if (vertx != null) vertx.close();
        Core.vertx = null;
    }

    @BeforeEach
    void setUp() throws Exception {
        endpoint = "http://" + dynamo.getHost() + ":" + dynamo.getFirstMappedPort();

        rawDdb = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "local"))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("dummy", "dummy")))
                .build();

        client = new DynamoBranchConfigClient(TABLE_ARN);
        injectLocalClient(client, TABLE_ARN, endpoint);

        if (!tableExists(TABLE_NAME)) {
            client.init().toCompletionStage().toCompletableFuture().join();
            waitUntilActive(TABLE_NAME);
        }
    }

    private static void injectLocalClient(DynamoBranchConfigClient target, String arn, String localEndpoint) throws Exception {
        DynamoClient local = new DynamoClient(arn, localEndpoint);

        Field dc = DynamoBranchConfigClient.class.getDeclaredField("dynamoClient");
        dc.setAccessible(true);
        dc.set(target, local);

        Field table = DynamoBranchConfigClient.class.getDeclaredField("branchTable");
        table.setAccessible(true);
        table.set(target, local.getTable());
    }

    private boolean tableExists(String tableName) {
        try {
            rawDdb.describeTable(tableName);
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    private void waitUntilActive(String tableName) {
        for (int i = 0; i < 100; i++) {
            String status = rawDdb.describeTable(tableName).getTable().getTableStatus();
            if ("ACTIVE".equalsIgnoreCase(status)) return;
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {}
        }
        throw new AssertionError("Table never became ACTIVE");
    }

    private void waitUntilDeleted(String tableName) {
        for (int i = 0; i < 100; i++) {
            try {
                rawDdb.describeTable(tableName);
                try { Thread.sleep(50); } catch (InterruptedException ignored) {}
            } catch (ResourceNotFoundException e) {
                return;
            }
        }
        throw new AssertionError("Table was not deleted in time");
    }

    private static Branch newBranch(String id) {
        Branch b = new Branch();
        b.setId(id);
        return b;
    }

    @Test
    @DisplayName("1. init() creates table & GSI on local")
    void initCreatesTableOnLocal() {
        if (tableExists(TABLE_NAME)) {
            rawDdb.deleteTable(TABLE_NAME);
            waitUntilDeleted(TABLE_NAME);
        }

        client.init().toCompletionStage().toCompletableFuture().join();
        waitUntilActive(TABLE_NAME);

        TableDescription td = rawDdb.describeTable(TABLE_NAME).getTable();
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

        client.store(spaceId, newBranch("main"), "main").toCompletionStage().toCompletableFuture().join();

        var all = client.load(spaceId).toCompletionStage().toCompletableFuture().join();
        assertThat(all).extracting(Branch::getId).contains("main");

        Branch loaded = client.load(spaceId, "main").toCompletionStage().toCompletableFuture().join();
        assertThat(loaded).isNotNull();
        assertThat(loaded.getId()).isEqualTo("main");

        client.delete(spaceId, "main", false).toCompletionStage().toCompletableFuture().join();
        Branch afterDelete = client.load(spaceId, "main").toCompletionStage().toCompletableFuture().join();
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
