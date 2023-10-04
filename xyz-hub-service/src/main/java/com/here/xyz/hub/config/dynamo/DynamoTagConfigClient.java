/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ExecuteStatementRequest;
import com.amazonaws.services.dynamodbv2.model.ExecuteTransactionRequest;
import com.amazonaws.services.dynamodbv2.model.ParameterizedStatement;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.util.CollectionUtils;
import com.here.xyz.hub.config.TagConfigClient;
import com.here.xyz.models.hub.Tag;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class DynamoTagConfigClient extends TagConfigClient {

  private static final Logger logger = LogManager.getLogger();
  private DynamoClient dynamoClient;
  private Table tagTable;

  public DynamoTagConfigClient(String tableArn) {
    dynamoClient = new DynamoClient(tableArn, null);
    logger.debug("Instantiating a reference to Dynamo Table {}", dynamoClient.tableName);
    tagTable = dynamoClient.db.getTable(dynamoClient.tableName);
  }

  @Override
  public Future<Void> init() {
    if (dynamoClient.isLocal()) {
      dynamoClient.createTable(tagTable.getTableName(), "id:S,spaceId:S", "id,spaceId", "spaceId", null);
    }

    return Future.succeededFuture();
  }

  @Override
  public Future<Tag> getTag(Marker marker, String id, String spaceId) {
    try {
      final ExecuteStatementRequest request = new ExecuteStatementRequest()
          .withStatement("SELECT * FROM \"" + tagTable.getTableName() + "\" WHERE \"id\" = ? and \"spaceId\" = ?")
          .withParameters(new AttributeValue(id), new AttributeValue(spaceId));

      return dynamoClient.executeStatement(request)
          .map(DynamoTagConfigClient::getTags)
          .map(tags -> tags.size() == 1 ? tags.get(0) : null);
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  @Override
  public Future<List<Tag>> getTags(Marker marker, String tagId, List<String> spaceIds) {
    if (CollectionUtils.isNullOrEmpty(spaceIds))
      return Future.succeededFuture(Collections.emptyList());

    try {
      if (spaceIds.size() < 100) {
        return Future.succeededFuture(batchGetTags(tagId, spaceIds));
      }

      String spaceParamsSt = StringUtils.join(Collections.nCopies(spaceIds.size(), "?"), ",");
      List<AttributeValue> params = Stream.concat(Stream.of(tagId), spaceIds.stream())
          .map(AttributeValue::new).collect(Collectors.toList());

      final ExecuteStatementRequest request = new ExecuteStatementRequest()
          .withStatement("SELECT * FROM \"" + tagTable.getTableName() + "\" WHERE \"id\" = ? AND \"spaceId\" IN [" + spaceParamsSt + "]")
          .withParameters(params);

      return dynamoClient.executeStatement(request)
          .map(DynamoTagConfigClient::getTags);
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  public Future<List<Tag>> getTagsByTagId(Marker marker, String tagId) {
    try {
      ExecuteStatementRequest request = new ExecuteStatementRequest()
          .withStatement("SELECT * FROM \"" + tagTable.getTableName() + "\" WHERE \"id\" = ?")
          .withParameters(new AttributeValue(tagId));

      return dynamoClient.executeStatement(request)
          .map(DynamoTagConfigClient::getTags);
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  @Override
  public Future<List<Tag>> getTags(Marker marker, String spaceId) {
    try {
      final ExecuteStatementRequest request = new ExecuteStatementRequest()
          .withStatement("SELECT * FROM \"" + tagTable.getTableName() + "\" WHERE \"spaceId\" = ?")
          .withParameters(new AttributeValue(spaceId));

      return dynamoClient.executeStatement(request)
          .map(DynamoTagConfigClient::getTags);
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  @Override
  public Future<List<Tag>> getTags(Marker marker, List<String> spaceIds) {
    try {
      String spaceParamsSt = StringUtils.join(Collections.nCopies(spaceIds.size(), "?"), ",");
      List<AttributeValue> params = spaceIds.stream().map(AttributeValue::new).collect(Collectors.toList());

      final ExecuteStatementRequest request = new ExecuteStatementRequest()
          .withStatement("SELECT * FROM \"" + tagTable.getTableName() + "\" WHERE \"spaceId\" IN [" + spaceParamsSt + "]")
          .withParameters(params);

      return dynamoClient.executeStatement(request)
          .map(DynamoTagConfigClient::getTags);
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  @Override
  public Future<List<Tag>> getAllTags(Marker marker) {
    try {
      final ExecuteStatementRequest request = new ExecuteStatementRequest()
          .withStatement("SELECT * FROM \"" + tagTable.getTableName() + "\"");

      return dynamoClient.executeStatement(request)
          .map(DynamoTagConfigClient::getTags);
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  @Override
  public Future<Void> storeTag(Marker marker, Tag tag) {
    return DynamoClient.dynamoWorkers.executeBlocking(
        future -> {
          try {
            tagTable.putItem(new Item()
                .withString("id", tag.getId())
                .withString("spaceId", tag.getSpaceId())
                .withLong("version", tag.getVersion()));
            future.complete();
          } catch (Exception e) {
            future.fail(e);
          }
        }
    );
  }

  @Override
  public Future<Tag> deleteTag(Marker marker, String id, String spaceId) {
    return DynamoClient.dynamoWorkers.executeBlocking(future -> {
          try {
            DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                .withPrimaryKey("id", id, "spaceId", spaceId)
                .withReturnValues(ReturnValue.ALL_OLD);
            DeleteItemOutcome response = tagTable.deleteItem(deleteItemSpec);
            if (response.getItem() != null) {
              future.complete(Json.decodeValue(response.getItem().toJSON(), Tag.class));
            } else {
              future.complete(null);
            }
          } catch (Exception e) {
            future.fail(e);
          }
        }
    );
  }

  @Override
  public Future<List<Tag>> deleteTagsForSpace(Marker marker, String spaceId) {
    return DynamoClient.dynamoWorkers.executeBlocking(future -> {
          try {
            getTags(marker, spaceId)
                .onSuccess(tags -> {
                  try {
                    if (tags.size() == 0) {
                      future.complete();
                      return;
                    }
                    List<ParameterizedStatement> statements = new ArrayList<>();
                    tags.forEach(r -> {
                      statements.add(new ParameterizedStatement()
                          .withStatement("DELETE FROM \"" + tagTable.getTableName() + "\" WHERE \"id\" = ? and \"spaceId\" = ?")
                          .withParameters(new AttributeValue(r.getId()), new AttributeValue(r.getSpaceId())));
                    });
                    dynamoClient.client.executeTransaction(new ExecuteTransactionRequest().withTransactStatements(statements));
                    future.complete(tags);
                  } catch (Exception e) {
                    future.fail(e);
                  }
                })
                .onFailure(future::fail);
          } catch (Exception e) {
            future.fail(e);
          }
        }
    );
  }

  private List<Tag> batchGetTags(String tagId, List<String> spaceIds) {
    TableKeysAndAttributes tableKeysAndAttributes = new TableKeysAndAttributes(tagTable.getTableName());
    String[] rangeKeys = spaceIds.stream().collect(ArrayList::new,
            (list, spaceId) -> {
              list.add(tagId);
              list.add(spaceId);
            },
            ArrayList::addAll).toArray(String[]::new);
    tableKeysAndAttributes.addHashAndRangePrimaryKeys("id", "spaceId", rangeKeys);

    List<Map<String, AttributeValue>> responses = dynamoClient.db.batchGetItem(tableKeysAndAttributes)
            .getBatchGetItemResult()
            .getResponses()
            .get(tagTable.getTableName());

    return getTags(responses);
  }

  private static List<Tag> getTags(List<Map<String, AttributeValue>> items) {
    if (items == null || items.size() == 0) {
      return new ArrayList<>();
    }
    return items.stream().map(i -> new Tag()
        .withId(i.get("id").getS())
        .withSpaceId(i.get("spaceId").getS())
        .withVersion(Long.parseLong(i.get("version").getN()))).collect(Collectors.toList());
  }
}
