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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ExecuteStatementRequest;
import com.amazonaws.services.dynamodbv2.model.ExecuteStatementResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.TimeToLiveSpecification;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveRequest;
import com.amazonaws.util.CollectionUtils;
import software.amazon.awssdk.regions.Region;
import com.here.xyz.util.ARN;
import com.here.xyz.util.service.Core;
import io.vertx.core.Future;
import io.vertx.core.WorkerExecutor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DynamoClient {

  private static final Logger logger = LogManager.getLogger();
  private static final Long READ_CAPACITY_UNITS = 5L;
  private static final Long WRITE_CAPACITY_UNITS = 5L;
  private static final String INDEX_SUFFIX = "-index";
  private static final String HYPHEN = "-";


  public static final WorkerExecutor dynamoWorkers = Core.vertx.createSharedWorkerExecutor(DynamoClient.class.getName(), 30);

  public final AmazonDynamoDBAsync client; //TODO: Make private once DynamoSpaceConfigClient has been refactored
  public final String tableName;
  public final DynamoDB db;
  private final ARN arn;

  public DynamoClient(String tableArn, String endpointOverride) {
    arn = new ARN(tableArn);

    final AmazonDynamoDBAsyncClientBuilder builder = AmazonDynamoDBAsyncClientBuilder.standard();

    if (isLocal()) {
      if (endpointOverride == null) {
        builder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("dummy", "dummy")));
        final String endpoint = "http://" + arn.getRegion() + ":" + Integer.parseInt(arn.getAccountId());
        builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "US-WEST-1"));
      }
      else
        builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpointOverride, "eu-west-1"))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("localstack", "localstack")));
    }
    else
      builder.setRegion(arn.getRegion());

    client = builder.build();
    db = new DynamoDB(client);
    tableName = new ARN(tableArn).getResourceWithoutType();
  }

  public Table getTable() {
    return db.getTable(tableName);
  }

  public void createTable(String tableName, String attributes, String keys, List<IndexDefinition> indexes, String ttl) {
    try {
      // required
      final List<AttributeDefinition> attList = new ArrayList<>();
      for (String s : attributes.split(",")) {
        attList.add(new AttributeDefinition(s.split(":")[0], s.split(":")[1]));
      }

      // required
      final List<KeySchemaElement> keyList = new ArrayList<>();
      for (String s : keys.split(",")) {
        keyList.add(new KeySchemaElement(s, keyList.isEmpty() ? KeyType.HASH : KeyType.RANGE));
      }


      CreateTableRequest req = new CreateTableRequest()
          .withTableName(tableName)
          .withAttributeDefinitions(attList)
          .withKeySchema(keyList)
          .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L));

      // optional
      if (!CollectionUtils.isNullOrEmpty(indexes)) {
        final List<GlobalSecondaryIndex> gsiList = indexes.stream().map(this::createGSI).toList();
        req = req.withGlobalSecondaryIndexes(gsiList);
      }

      db.createTable(req);

      if (ttl != null) {
        client.updateTimeToLive(new UpdateTimeToLiveRequest().withTableName(tableName)
            .withTimeToLiveSpecification(new TimeToLiveSpecification().withAttributeName(ttl).withEnabled(true)));
      }
    } catch (ResourceInUseException e) {
      logger.info("Table {} already exists, skipping creation", tableName);
    }
  }

  public boolean isLocal() {
    return Region.regions().stream().noneMatch(r -> r.id().equals(arn.getRegion()));
  }

  public Future<List<Map<String, AttributeValue>>> executeStatement(ExecuteStatementRequest request) {
    return DynamoClient.dynamoWorkers.executeBlocking(future -> {
      try {
        future.complete(executeStatementSync(request));
      }
      catch (Exception e) {
        future.fail(e);
      }
    });
  }

  public List<Map<String, AttributeValue>> executeStatementSync(ExecuteStatementRequest request) {
    ExecuteStatementResult result = client.executeStatement(request);
    List<Map<String, AttributeValue>> items = result.getItems();

    while (result.getNextToken() != null) {
      result = client.executeStatement(request.withNextToken(result.getNextToken()));
      items.addAll(result.getItems());
    }
    return items;
  }

  @FunctionalInterface
  public interface ThrowingSupplier<R> {
    R supply() throws Exception;
  }

  public <R> Future<R> executeQueryAsync(ThrowingSupplier<R> commandExecution) {
    return DynamoClient.dynamoWorkers.executeBlocking(
        promise -> {
          try {
              promise.complete(commandExecution.supply());
            }
            catch (Exception e) {
              promise.fail(e);
            }
        });
  }

  private GlobalSecondaryIndex createGSI(IndexDefinition indexDefinition) {
    List<KeySchemaElement> keySchema = new ArrayList<>();
    keySchema.add(new KeySchemaElement(indexDefinition.getHashKey(), KeyType.HASH));
    if(indexDefinition.getRangeKey() != null) {
      keySchema.add(new KeySchemaElement(indexDefinition.getRangeKey(), KeyType.RANGE));
    }
    String indexName = indexDefinition.getRangeKey() != null ?
            indexDefinition.getHashKey().concat(HYPHEN).concat(indexDefinition.getRangeKey()).concat(INDEX_SUFFIX) :
            indexDefinition.getHashKey().concat(INDEX_SUFFIX);

    return new GlobalSecondaryIndex()
            .withIndexName(indexName)
            .withKeySchema(keySchema)
            .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
            .withProvisionedThroughput(new ProvisionedThroughput(READ_CAPACITY_UNITS, WRITE_CAPACITY_UNITS));
  }
}
