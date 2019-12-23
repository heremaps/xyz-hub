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

import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.util.CollectionUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.util.ARN;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Marker;

public class DynamoSpaceConfigClient extends SpaceConfigClient {

  private final DynamoClient dynamoClient;
  private Table spaces;
  private Table packages;

  public DynamoSpaceConfigClient(final String tableArn) {
    dynamoClient = new DynamoClient(tableArn);

    logger().info("Instantiating a reference to Dynamo Table {}", dynamoClient.tableName);
    spaces = dynamoClient.db.getTable(dynamoClient.tableName);
    packages = dynamoClient.db.getTable(new ARN(Service.configuration.PACKAGES_DYNAMODB_TABLE_ARN).getResourceWithoutType());
  }

  @Override
  public void init(Handler<AsyncResult<Void>> onReady) {
    dynamoClient.createTable(spaces.getTableName(), "id:S,owner:S,shared:N", "id", "owner,shared", "exp");
    dynamoClient.createTable(packages.getTableName(), "packageName:S,spaceId:S", "packageName,spaceId", null, null);

    onReady.handle(Future.succeededFuture());
  }

  @Override
  public void getSpace(Marker marker, String spaceId, Handler<AsyncResult<Space>> handler) {
    final Item item = spaces.getItem("id", spaceId);
    if (item == null) {
      handler.handle(Future.succeededFuture(null));
      return;
    }
    final Space space = Json.decodeValue(item.toJSON(), Space.class);
    handler.handle(Future.succeededFuture(space));
  }

  @Override
  public void storeSpace(Marker marker, Space space, Handler<AsyncResult<Space>> handler) {
    Map<String, Object> itemData = defaultMapper().convertValue(space, new TypeReference<Map<String, Object>>() {});
    itemData.put("shared", space.isShared() ? 1 : 0);
    spaces.putItem(Item.fromMap(itemData));

    deleteSpaceFromPackage(marker, space);
    storeSpaceIntoPackages(marker, space);

    handler.handle(Future.succeededFuture(space));
  }


  @Override
  public void deleteSpace(Marker marker, String spaceId, Handler<AsyncResult<Space>> handler) {
    get(marker, spaceId, ar -> {
      if (ar.succeeded()) {
        final Space space = ar.result();
        deleteSpaceFromPackage(marker, space);
        spaces.deleteItem("id", spaceId);
        handler.handle(Future.succeededFuture(space));
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });

  }

  /**
   * Stores the relation between package name and spaces in Dynamo
   *
   * @param marker used in logs
   * @param space the space which is being stored
   */
  private void storeSpaceIntoPackages(Marker marker, Space space) {
    if (space.getPackages() != null) {
      for (String packageName : space.getPackages()) {
        logger().info(marker, "Adding space: {} into package: {}", space.getId(), packageName);
        final Map<String, Object> packagesMap = new HashMap<>();
        packagesMap.put("packageName", packageName);
        packagesMap.put("spaceId", space.getId());

        packages.putItem(Item.fromMap(packagesMap));
      }
    }
  }
  /**
   * Deletes the relationship between a space and its packages
   * @param marker used in logs
   * @param space the spaceId which is being deleted
   */
  private void deleteSpaceFromPackage(Marker marker, Space space) {
    if (space == null) {
      return;
    }
    logger().info(marker, "Removing packages from spaceId: {}", space.getId());

    final List<String> packagesList = new ArrayList<>();
    if (space.getPackages() != null) {
      packagesList.addAll(space.getPackages());
    } else {
      final GetItemSpec spec = new GetItemSpec().withPrimaryKey("id", space.getId()).withProjectionExpression("packages");
      final Item item = spaces.getItem(spec);
      if (item != null && item.isPresent("packages")) {
        packagesList.addAll(item.getList("packages"));
      }
    }

    for (String packageName : packagesList) {
      packages.deleteItem("packageName", packageName, "spaceId", space.getId());
    }
  }

  @Override
  public void getSelectedSpaces(Marker marker, SpaceAuthorizationCondition authorizedCondition, SpaceSelectionCondition selectedCondition,
      Handler<AsyncResult<List<Space>>> handler) {
    final List<Space> result = new ArrayList<>();
    final Set<String> authorizedSpaces = getAuthorizedSpaces(authorizedCondition);

    // get all shared spaces if the selection for shared spaces is enabled
    if (selectedCondition.shared) {
      spaces.getIndex("shared-index").query(new QuerySpec().withHashKey("shared", 1).withProjectionExpression("id")).pages().forEach(p -> p.forEach(i -> {
        authorizedSpaces.add(i.getString("id"));
      }));
    }

    // filter out the ones not present in the selectedCondition (null or empty represents 'do not filter')
    if (!CollectionUtils.isNullOrEmpty(selectedCondition.spaceIds)) {
      authorizedSpaces.removeIf(i -> !selectedCondition.spaceIds.contains(i));
    }

    // now filter all spaceIds with the ones being selected in the selectedCondition (by checking the space's ownership) (
    if (!CollectionUtils.isNullOrEmpty(selectedCondition.ownerIds)) {
      final Set<String> ownersSpaces = new HashSet<>();
      selectedCondition.ownerIds.forEach(o ->
          spaces.getIndex("owner-index").query(new QuerySpec().withHashKey("owner", o).withProjectionExpression("id")).pages().forEach(p -> p.forEach(i -> ownersSpaces.add(i.getString("id")))));

      // HINT: A ^ TRUE == !A (negateOwnerIds: keep or remove the spaces contained in the owner's spaces list)
      authorizedSpaces.removeIf(i -> !selectedCondition.negateOwnerIds ^ ownersSpaces.contains(i));
    }

    // TODO selection per packages is not yet supported: selectedCondition.packages

    if (!authorizedSpaces.isEmpty()) {
      final TableKeysAndAttributes keys = new TableKeysAndAttributes(dynamoClient.tableName);

      for (final String spaceId : authorizedSpaces) {
        keys.addHashOnlyPrimaryKey("id", spaceId);
      }

      BatchGetItemOutcome outcome = dynamoClient.db.batchGetItem(keys);
      processOutcome(outcome, result);

      while (!outcome.getUnprocessedKeys().isEmpty()) {
        outcome = dynamoClient.db.batchGetItemUnprocessed(outcome.getUnprocessedKeys());
        processOutcome(outcome, result);
      }
    }

    handler.handle(Future.succeededFuture(result));
  }

  private Set<String> getAuthorizedSpaces(SpaceAuthorizationCondition authorizedCondition) {
    final Set<String> authorizedSpaces = new HashSet<>();

    // get the space ids which are authorized by the authorizedCondition
    if (authorizedCondition.spaceIds != null) {
      authorizedSpaces.addAll(authorizedCondition.spaceIds);
    }

    // then get the owners which are authorized by the authorizedCondition
    if (authorizedCondition.ownerIds != null) {
      authorizedCondition.ownerIds.forEach(owner ->
          spaces.getIndex("owner-index").query("owner", owner).pages().forEach(p -> p.forEach(i -> {
            authorizedSpaces.add(i.getString("id"));
          }))
      );
    }

    // then get the packages which are authorized by the authorizedCondition
    if (authorizedCondition.packages != null) {
      authorizedCondition.packages.forEach(packageName ->
          packages.query("packageName", packageName).pages().forEach(p -> p.forEach(i -> {
            authorizedSpaces.add(i.getString("spaceId"));
          }))
      );
    }

    // then get the "empty" case, when no spaceIds or ownerIds os packages are provided, meaning select ALL spaces
    if (CollectionUtils.isNullOrEmpty(authorizedCondition.spaceIds)
        && CollectionUtils.isNullOrEmpty(authorizedCondition.ownerIds)
        && CollectionUtils.isNullOrEmpty(authorizedCondition.packages)) {
      spaces.scan(new ScanSpec().withProjectionExpression("id")).pages().forEach(p -> p.forEach(i -> authorizedSpaces.add(i.getString("id"))));
    }

    return authorizedSpaces;
  }

  /**
   * Fills the result list transforming the raw elements from the outcome into real Space objects
   * @param outcome the query result
   * @param result the transformed resulting elements
   */
  private void processOutcome(BatchGetItemOutcome outcome, List<Space> result) {
    outcome.getTableItems().get(dynamoClient.tableName).forEach(i -> result.add(Json.decodeValue(i.toJSON(), Space.class)));
  }
}
