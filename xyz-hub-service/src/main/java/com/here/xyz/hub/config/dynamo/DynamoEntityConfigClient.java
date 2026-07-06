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

package com.here.xyz.hub.config.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.hub.config.EntityConfigClient;
import com.here.xyz.util.service.aws.dynamo.DynamoClient;
import io.vertx.core.Future;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DynamoEntityConfigClient extends EntityConfigClient {

  private static final Logger logger = LogManager.getLogger();
  private DynamoClient dynamoClient;
  private Table entityTable;

  public DynamoEntityConfigClient(String tableArn) {
    dynamoClient = new DynamoClient(tableArn, null);
    logger.info("Instantiating a reference to Dynamo Table {}", dynamoClient.tableName);
    entityTable = dynamoClient.getTable();
  }

  private String typeName(Class<?> clazz) {
    //if (clazz.getDeclaringClass() != null)
    //  return clazz.getDeclaringClass().getSimpleName() + "$" + clazz.getSimpleName();
    return clazz.getSimpleName();
  }

  @Override
  protected <E extends Typed> Future<UUID> storeEntity(E entity) {
    return dynamoClient.executeQueryAsync(() -> {
      final Map<String, Object> entityItemData = entity.toMap(Static.class);
      entityItemData.put("uuid", UUID.randomUUID().toString());
      entityTable.putItem(Item.fromMap(entityItemData));
      return null;
    });
  }

  @Override
  protected <E extends Typed> Future<E> loadEntity(Class<E> entityType, String uuid) {
    return dynamoClient.executeQueryAsync(() -> {
      Item branchItem = entityTable.getItem("type", typeName(entityType), "uuid", uuid);
      return branchItem != null ? entityType.cast(XyzSerializable.fromMap(branchItem.asMap(), Typed.class)) : null;
    });
  }

  @Override
  protected <E extends Typed> Future<List<E>> loadEntities(Class<E> entityType) {
    return dynamoClient.executeQueryAsync(() -> {
      List<E> entities = new LinkedList<>();
      entityTable.query("type", typeName(entityType))
          .pages()
          .forEach(page -> page.forEach(entityItem -> entities.add(entityType.cast(XyzSerializable.fromMap(entityItem.asMap(), Typed.class)))));
      return entities;
    });
  }

  @Override
  protected Future<Void> deleteEntity(Class<? extends Typed> entityType, String uuid) {
    return dynamoClient.executeQueryAsync(() -> {
      entityTable.deleteItem(new DeleteItemSpec().withPrimaryKey("type", typeName(entityType), "uuid", uuid));
      return null;
    });
  }

  @Override
  public Future<Void> init() {
    if (dynamoClient.isLocal()) {
      try {
        dynamoClient.createTable(entityTable.getTableName(), "type:S,uuid:S", "type,uuid", null, null);
      }
      catch (AmazonDynamoDBException e) {
        logger.error("Failure during creating table on " + getClass().getSimpleName() + "#init()", e);
        return Future.failedFuture(e);
      }
    }
    return Future.succeededFuture();
  }
}
