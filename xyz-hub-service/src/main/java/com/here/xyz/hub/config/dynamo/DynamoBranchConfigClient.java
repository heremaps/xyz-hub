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

import static com.here.xyz.util.service.aws.dynamo.DynamoClient.queryIndex;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.hub.config.BranchConfigClient;
import com.here.xyz.models.hub.Branch;
import com.here.xyz.util.service.aws.dynamo.DynamoClient;
import com.here.xyz.util.service.aws.dynamo.IndexDefinition;
import io.vertx.core.Future;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DynamoBranchConfigClient extends BranchConfigClient {
  private static final Logger logger = LogManager.getLogger();
  public static final IndexDefinition ID_GSI = new IndexDefinition("id");
  private DynamoClient dynamoClient;
  private Table branchTable;

  public DynamoBranchConfigClient(String tableArn) {
    dynamoClient = new DynamoClient(tableArn, null);
    logger.info("Instantiating a reference to Dynamo Table {}", dynamoClient.tableName);
    branchTable = dynamoClient.getTable();
  }

  @Override
  public Future<Void> store(String spaceId, Branch branch, String branchId) {
    if (!branchId.equals(branch.getId()))
      //The branch was renamed, as the branch ID attribute is part of the primary key, the item has to be deleted & re-created
      //TODO: Instead use an insert query (with concurrently checking if the old ID exists) with deletion afterwards
      return delete(spaceId, branchId)
          .compose(v -> store(spaceId, branch));

    return dynamoClient.executeQueryAsync(() -> {
      final Map<String, Object> branchItemData = branch.toMap(Static.class);
      branchItemData.put("spaceId", spaceId);
      branchTable.putItem(Item.fromMap(branchItemData));
      return null;
    });
  }

  @Override
  public Future<List<Branch>> load(String spaceId) {
    return dynamoClient.executeQueryAsync(() -> {
      List<Branch> branches = new LinkedList<>();
      branchTable.query("spaceId", spaceId)
          .pages()
          .forEach(page -> page.forEach(branchItem -> branches.add(XyzSerializable.fromMap(branchItem.asMap(), Branch.class))));
      return branches;
    });
  }

  @Override
  public Future<Branch> load(String spaceId, String branchId) {
    return dynamoClient.executeQueryAsync(() -> {
      Item branchItem = branchTable.getItem("spaceId", spaceId, "id", branchId);
      return branchItem != null ? XyzSerializable.fromMap(branchItem.asMap(), Branch.class) : null;
    });
  }

  @Override
  public Future<List<Branch>> loadBranches(String branchId) {
    return dynamoClient.executeQueryAsync(() -> queryIndex(branchTable, ID_GSI, branchId)
        .stream()
        .map(branchItem -> XyzSerializable.fromMap(branchItem.asMap(), Branch.class))
        .toList());
  }

  @Override
  public Future<Void> delete(String spaceId, String branchId) {
    return dynamoClient.executeQueryAsync(() -> {
      branchTable.deleteItem(new DeleteItemSpec().withPrimaryKey("spaceId", spaceId, "id", branchId));
      return null;
    });
  }

  @Override
  public Future<Void> init() {
    if (dynamoClient.isLocal()) {
      try {
        dynamoClient.createTable(branchTable.getTableName(), "spaceId:S,id:S", "spaceId,id", List.of(ID_GSI), null);
      }
      catch (AmazonDynamoDBException e) {
        logger.error("Failure during creating table on " + getClass().getSimpleName() + "#init()", e);
        return Future.failedFuture(e);
      }
    }
    return Future.succeededFuture();
  }
}
