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

package com.here.xyz.hub.config.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.ExecuteStatementRequest;
import com.amazonaws.services.dynamodbv2.model.ExecuteTransactionRequest;
import com.amazonaws.services.dynamodbv2.model.ParameterizedStatement;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.here.xyz.XyzSerializable;
import com.here.xyz.hub.config.TagConfigClient;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.util.service.aws.dynamo.DynamoClient;
import com.here.xyz.util.service.aws.dynamo.IndexDefinition;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import com.here.xyz.XyzSerializable.Static;

public class DynamoTagConfigClient extends TagConfigClient {

  private static final Logger logger = LogManager.getLogger();
  private final DynamoClient dynamoClient;
  private final Table tagTable;

  public DynamoTagConfigClient(String tableArn) {
    dynamoClient = new DynamoClient(tableArn, null);
    logger.debug("Instantiating a reference to Dynamo Table {}", dynamoClient.tableName);
    tagTable = dynamoClient.db.getTable(dynamoClient.tableName);
  }

  @Override
  public Future<Void> init() {
    if (dynamoClient.isLocal())
      dynamoClient.createTable(tagTable.getTableName(), "id:S,spaceId:S", "id,spaceId", List.of(new IndexDefinition("spaceId")), null);

    return Future.succeededFuture();
  }

  @Override
  public Future<Tag> getTag(Marker marker, String id, String spaceId) {
    try {
      //TODO: Replace PartiQL query by actual query
      final ExecuteStatementRequest request = new ExecuteStatementRequest()
          .withStatement("SELECT * FROM \"" + tagTable.getTableName() + "\" WHERE \"id\" = ? and \"spaceId\" = ?")
          .withParameters(new AttributeValue(id), new AttributeValue(spaceId));

      return dynamoClient.executeStatement(request)
          .map(DynamoTagConfigClient::tagDataToTags)
          .map(tags -> tags.size() == 1 ? tags.get(0) : null);
    }
    catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  @Override
  public Future<List<Tag>> getTags(Marker marker, String tagId, List<String> spaceIds) {
    if (spaceIds == null || spaceIds.isEmpty())
      return Future.succeededFuture(Collections.emptyList());

    return dynamoClient.executeQueryAsync(() -> {
      List<Map<String, AttributeValue>> responses = new ArrayList<>();
      int batches = (int) Math.ceil((double) spaceIds.size() / 100);
      for (int i = 0; i < batches; i++) {
        final TableKeysAndAttributes tableKeysAndAttributes = new TableKeysAndAttributes(tagTable.getTableName());
        String[] rangeKeys = spaceIds.stream().skip(i * 100L).limit(100).collect(ArrayList::new,
            (list, spaceId) -> {
              list.add(tagId);
              list.add(spaceId);
            },
            ArrayList::addAll).toArray(String[]::new);
        tableKeysAndAttributes.addHashAndRangePrimaryKeys("id", "spaceId", rangeKeys);

        BatchGetItemResult batchGetResult = dynamoClient.db.batchGetItem(tableKeysAndAttributes).getBatchGetItemResult();
        responses.addAll(batchGetResult.getResponses().get(tagTable.getTableName()));
      }
      return tagDataToTags(responses);
    })
        .onSuccess(tags -> logger.info(marker, "Number of tags retrieved from DynamoDB: {}", tags.size()))
        .onFailure(t -> logger.error(marker, "Failure getting tags", t));
  }

  public Future<List<Tag>> getTagsByTagId(Marker marker, String tagId) {
    try {
      //TODO: Replace PartiQL query by actual query
      ExecuteStatementRequest request = new ExecuteStatementRequest()
          .withStatement("SELECT * FROM \"" + tagTable.getTableName() + "\" WHERE \"id\" = ?")
          .withParameters(new AttributeValue(tagId));

      return dynamoClient.executeStatement(request)
          .map(DynamoTagConfigClient::tagDataToTags);
    }
    catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  @Override
  public Future<List<Tag>> getTags(Marker marker, String spaceId, boolean includeSystemTags) {
    try {
      final String includeSystemTagsQuery = includeSystemTags ? "" : " AND (\"system\" is MISSING OR \"system\" = false)";

      //TODO: Replace PartiQL query by actual query
      final ExecuteStatementRequest request = new ExecuteStatementRequest()
          .withStatement("SELECT * FROM \"" + tagTable.getTableName() + "\".\"spaceId-index\" WHERE \"spaceId\" = ?" + includeSystemTagsQuery)
          .withParameters(new AttributeValue(spaceId));

      return dynamoClient.executeStatement(request)
          .map(DynamoTagConfigClient::tagDataToTags);
    }
    catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  @Override
  public Future<List<Tag>> getTags(Marker marker, List<String> spaceIds) {
    try {
      String spaceParamsSt = String.join(",", Collections.nCopies(spaceIds.size(), "?"));
      List<AttributeValue> params = spaceIds.stream().map(AttributeValue::new).collect(Collectors.toList());

      //TODO: Replace PartiQL query by actual query
      final ExecuteStatementRequest request = new ExecuteStatementRequest()
          .withStatement("SELECT * FROM \"" + tagTable.getTableName() + "\" WHERE \"spaceId\" IN [" + spaceParamsSt + "]")
          .withParameters(params);

      return dynamoClient.executeStatement(request)
          .map(DynamoTagConfigClient::tagDataToTags);
    }
    catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  @Override
  public Future<List<Tag>> getAllTags(Marker marker) {
    try {
      //TODO: Replace PartiQL query by actual query
      final ExecuteStatementRequest request = new ExecuteStatementRequest()
          .withStatement("SELECT * FROM \"" + tagTable.getTableName() + "\"");

      return dynamoClient.executeStatement(request)
          .map(DynamoTagConfigClient::tagDataToTags);
    }
    catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  @Override
  public Future<Void> storeTag(Marker marker, Tag tag) {
    return dynamoClient.executeQueryAsync(() -> {
      tagTable.putItem(Item.fromMap(XyzSerializable.toMap(tag, Static.class)));
      return null;
    });
  }

  @Override
  public Future<Tag> deleteTag(Marker marker, String id, String spaceId) {
    return dynamoClient.executeQueryAsync(() -> {
      DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
          .withPrimaryKey("id", id, "spaceId", spaceId)
          .withReturnValues(ReturnValue.ALL_OLD);
      Item tagItem = tagTable.deleteItem(deleteItemSpec).getItem();
      return tagItem == null ? null : XyzSerializable.fromMap(tagItem.asMap(), Tag.class);
    });
  }

  @Override
  public Future<List<Tag>> deleteTagsForSpace(Marker marker, String spaceId) {
    return getTags(marker, spaceId, true)
        .compose(tags -> {
          try {
            if (tags.isEmpty())
              return Future.succeededFuture();

            List<ParameterizedStatement> statements = new ArrayList<>();
            tags.forEach(r -> statements.add(new ParameterizedStatement()
                //TODO: Replace PartiQL query by actual query
                .withStatement("DELETE FROM \"" + tagTable.getTableName() + "\" WHERE \"id\" = ? and \"spaceId\" = ?")
                .withParameters(new AttributeValue(r.getId()), new AttributeValue(r.getSpaceId()))));
            dynamoClient.client.executeTransaction(new ExecuteTransactionRequest().withTransactStatements(statements));
            return Future.succeededFuture(tags);
          }
          catch (Exception e) {
            return Future.failedFuture(e);
          }
        });
  }

  private static List<Tag> tagDataToTags(List<Map<String, AttributeValue>> items) {
    if (items == null || items.isEmpty())
      return Collections.emptyList();

    return items.stream().map(tagData -> new Tag()
        .withId(tagData.get("id").getS())
        .withSpaceId(tagData.get("spaceId").getS())
        .withVersion(Long.parseLong(tagData.get("version").getN()))
        .withSystem( tagData.get("system") != null ? tagData.get("system").getBOOL() : false )).collect(Collectors.toList());
  }
}
