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

package com.here.xyz.util.service.aws.dynamo;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.*;
import com.here.xyz.util.ARN;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class DynamoClientV2 {

    private static final Logger logger = LogManager.getLogger();
    private static final Long READ_CAPACITY_UNITS = 5L;
    private static final Long WRITE_CAPACITY_UNITS = 5L;
    private static final String INDEX_SUFFIX = "-index";
    private static final String HYPHEN = "-";
    public final DynamoDbAsyncClient client; //TODO: Make private once DynamoSpaceConfigClient has been refactored
    public final String tableName;
    private final ARN arn;

    public DynamoClientV2(String tableArn, String endpointOverride) {
        arn = new ARN(tableArn);

        DynamoDbAsyncClientBuilder builder = DynamoDbAsyncClient.builder();

        if (isLocal()) {
            if (endpointOverride == null) {
                builder = builder
                        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
                        .endpointOverride(URI.create("http://" + arn.getRegion() + ":" + arn.getAccountId()))
                        .region(Region.US_WEST_1);
            } else {
                builder = builder
                        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("localstack", "localstack")))
                        .endpointOverride(URI.create(endpointOverride))
                        .region(Region.EU_WEST_1);
            }
        } else {
            builder = builder.region(Region.of(arn.getRegion()));
        }

        client = builder.build();
        tableName = arn.getResourceWithoutType();
    }

    public void createTable(String tableName, String attributes, String keys, List<IndexDefinition> indexes, String ttl) {
        try {
            // required
            List<AttributeDefinition> attList = new ArrayList<>();
            for (String s : attributes.split(",")) {
                attList.add(AttributeDefinition.builder().attributeName(s.split(":")[0]).attributeType(s.split(":")[1]).build());
            }

            // required
            List<KeySchemaElement> keyList = new ArrayList<>();
            for (String s : keys.split(",")) {
                keyList.add(KeySchemaElement.builder().attributeName(s).keyType(keyList.isEmpty() ? KeyType.HASH : KeyType.RANGE).build());
            }

            CreateTableRequest.Builder reqBuilder = CreateTableRequest.builder()
                    .tableName(tableName)
                    .attributeDefinitions(attList)
                    .keySchema(keyList)
                    .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(READ_CAPACITY_UNITS).writeCapacityUnits(WRITE_CAPACITY_UNITS).build());

            // optional
            if (indexes != null && !indexes.isEmpty()) {
                List<GlobalSecondaryIndex> gsiList = new ArrayList<>();
                for (IndexDefinition index : indexes) {
                    gsiList.add(createGSI(index));
                }
                reqBuilder.globalSecondaryIndexes(gsiList);
            }

            CreateTableRequest req = reqBuilder.build();
            client.createTable(req).join();

            if (ttl != null) {
                UpdateTimeToLiveRequest ttlRequest = UpdateTimeToLiveRequest.builder()
                        .tableName(tableName)
                        .timeToLiveSpecification(TimeToLiveSpecification.builder().attributeName(ttl).enabled(true).build())
                        .build();
                client.updateTimeToLive(ttlRequest).join();
            }
        } catch (ResourceInUseException e) {
            logger.info("Table {} already exists, skipping creation", tableName);
        }
    }

    private GlobalSecondaryIndex createGSI(IndexDefinition indexDefinition) {
        List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(KeySchemaElement.builder().attributeName(indexDefinition.getHashKey()).keyType(KeyType.HASH).build());
        if (indexDefinition.getRangeKey() != null) {
            keySchema.add(KeySchemaElement.builder().attributeName(indexDefinition.getRangeKey()).keyType(KeyType.RANGE).build());
        }
        String indexName = indexDefinition.getRangeKey() != null ?
                indexDefinition.getHashKey().concat(HYPHEN).concat(indexDefinition.getRangeKey()).concat(INDEX_SUFFIX) :
                indexDefinition.getHashKey().concat(INDEX_SUFFIX);

        return GlobalSecondaryIndex.builder()
                .indexName(indexName)
                .keySchema(keySchema)
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(READ_CAPACITY_UNITS).writeCapacityUnits(WRITE_CAPACITY_UNITS).build())
                .build();
    }

    public boolean isLocal() {
        return arn.getRegion().equals("local");
    }
}