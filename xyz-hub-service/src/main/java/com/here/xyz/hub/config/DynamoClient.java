/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.hub.config;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.Stream;
import com.amazonaws.services.dynamodbv2.model.TimeToLiveSpecification;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveRequest;
import com.amazonaws.services.s3.model.Region;
import com.here.xyz.hub.util.ARN;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DynamoClient {

  private static final Logger logger = LogManager.getLogger();

  protected final AmazonDynamoDBAsync client;
  protected final String tableName;
  protected final DynamoDB db;

  protected DynamoClient(String tableArn) {
    final ARN arn = new ARN(tableArn);

    final AmazonDynamoDBAsyncClientBuilder builder = AmazonDynamoDBAsyncClientBuilder.standard();
    if (Arrays.stream(Region.values()).anyMatch(r -> r.name().equals(arn.getRegion()))) {
      final String endpoint = String.format("http://%s:%s", arn.getRegion(), Integer.parseInt(arn.getAccountId()));
      builder.setEndpointConfiguration(new EndpointConfiguration(endpoint, "US-WEST-1"));
    }

    client = builder.build();
    db = new DynamoDB(client);
    tableName = new ARN(tableArn).getResourceWithoutType();
  }

  protected void createTable(String tableName, String attributes, String keys, String indexes, String ttl) {
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
          .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L))
          .withBillingMode(BillingMode.PAY_PER_REQUEST);

      // optional
      if (indexes != null) {
        final List<GlobalSecondaryIndex> gsiList = new ArrayList<>();
        for (String s : indexes.split(",")) {
          gsiList.add(
              new GlobalSecondaryIndex()
                  .withIndexName(s.concat("-index"))
                  .withKeySchema(new KeySchemaElement(s, KeyType.HASH))
                  .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                  .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L))
          );
        }

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
}
