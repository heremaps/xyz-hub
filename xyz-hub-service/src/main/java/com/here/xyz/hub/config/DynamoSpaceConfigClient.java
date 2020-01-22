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
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.util.CollectionUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class DynamoSpaceConfigClient extends SpaceConfigClient {

  private static final Logger logger = LogManager.getLogger();

  private final DynamoClient dynamoClient;
  private Table spaces;
  private Table packages;

  public DynamoSpaceConfigClient(final String tableArn) {
    dynamoClient = new DynamoClient(tableArn);

    logger.info("Instantiating a reference to Dynamo Table {}", dynamoClient.tableName);
    spaces = dynamoClient.db.getTable(dynamoClient.tableName);
    packages = dynamoClient.db.getTable(new ARN(Service.configuration.PACKAGES_DYNAMODB_TABLE_ARN).getResourceWithoutType());
  }

  @Override
  public void init(Handler<AsyncResult<Void>> onReady) {
    if (dynamoClient.isLocal()) {
      logger.info("DynamoDB running locally, initializing tables.");

      try {
        dynamoClient.createTable(spaces.getTableName(), "id:S,owner:S,shared:N", "id", "owner,shared", "exp");
        dynamoClient.createTable(packages.getTableName(), "packageName:S,spaceId:S", "packageName,spaceId", null, null);
      } catch (AmazonDynamoDBException e) {
        logger.error("Failure during creating tables on DynamoSpaceConfigClient init", e);
        onReady.handle(Future.failedFuture(e));
        return;
      }
    }

    onReady.handle(Future.succeededFuture());
  }

  @Override
  public void getSpace(Marker marker, String spaceId, Handler<AsyncResult<Space>> handler) {
    try {
      logger.info(marker, "Getting space with ID: {}", spaceId);
      final Item item = spaces.getItem("id", spaceId);

      if (item == null) {
        logger.info(marker, "Getting space with ID: {} returned null", spaceId);
        handler.handle(Future.succeededFuture(null));
        return;
      }

      final Space space = Json.decodeValue(item.toJSON(), Space.class);
      if (space != null) {
        logger.info(marker, "Space ID: {} with title: \"{}\" has been decoded", spaceId, space.getTitle());
      } else {
        logger.info(marker, "Space ID: {} has been decoded to null", spaceId);
      }
      handler.handle(Future.succeededFuture(space));
    } catch (AmazonDynamoDBException e) {
      logger.error(marker, "Failure during getting a space from DynamoDB", e);
      handler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void storeSpace(Marker marker, Space space, Handler<AsyncResult<Space>> handler) {
    try {
      logger.info(marker, "Storing space with ID: {}", space.getId());
      Map<String, Object> itemData = XyzSerializable.STATIC_MAPPER.get().convertValue(space, new TypeReference<Map<String, Object>>() {});
      itemData.put("shared", space.isShared() ? 1 : 0);
      spaces.putItem(Item.fromMap(itemData));

      deleteSpaceFromPackage(marker, space);
      storeSpaceIntoPackages(marker, space);

      logger.info(marker, "Space with ID: {} has been successfully stored", space.getId());
      handler.handle(Future.succeededFuture(space));
    } catch (AmazonDynamoDBException e) {
      logger.error(marker, "Failure storing a space into DynamoDB", e);
      handler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void deleteSpace(Marker marker, String spaceId, Handler<AsyncResult<Space>> handler) {
    logger.info(marker, "Deleting space ID: {}", spaceId);
    get(marker, spaceId, ar -> {
      if (ar.succeeded()) {
        try {
          final Space space = ar.result();
          logger.info(marker, "Space ID: {} has been retrieved", spaceId);

          deleteSpaceFromPackage(marker, space);
          spaces.deleteItem("id", spaceId);

          handler.handle(Future.succeededFuture(space));
        } catch (AmazonDynamoDBException e) {
          logger.info(marker, "Failure to delete space ID: {}", spaceId);
          handler.handle(Future.failedFuture(ar.cause()));
        }
      } else {
        logger.info(marker, "Failure to get space ID: {} during space deletion", spaceId);
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
  private void storeSpaceIntoPackages(Marker marker, Space space) throws AmazonDynamoDBException {
    if (space == null) {
      return;
    }

    try {
      logger.info(marker, "Inserting packages {} into the packages table for space ID: {}", space.getPackages(), space.getId());
      if (space.getPackages() != null) {
        for (String packageName : space.getPackages()) {
          logger.info(marker, "Adding space ID: {} into package: {}", space.getId(), packageName);
          final Map<String, Object> packagesMap = new HashMap<>();
          packagesMap.put("packageName", packageName);
          packagesMap.put("spaceId", space.getId());

          packages.putItem(Item.fromMap(packagesMap));
        }
      }
    } catch (AmazonDynamoDBException e) {
      logger.error(marker, "Failure storing space ID: {} into the packages table", space.getId(), e);
      throw e;
    }
  }

  /**
   * Deletes the relationship between a space and their packages
   *
   * @param marker used in logs
   * @param space the spaceId which is being deleted
   */
  private void deleteSpaceFromPackage(Marker marker, Space space) throws AmazonDynamoDBException {
    if (space == null) {
      return;
    }

    try {
      logger.info(marker, "Removing packages from space ID: {}", space.getId());
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

      logger.info(marker, "Packages {} to be removed from space ID: {}", packagesList, space.getId());
      for (String packageName : packagesList) {
        packages.deleteItem("packageName", packageName, "spaceId", space.getId());
      }
    } catch (AmazonDynamoDBException e) {
      logger.error(marker, "Failure deleting space ID: {} from the packages table", space.getId(), e);
      throw e;
    }
  }

  @Override
  public void getSelectedSpaces(Marker marker, SpaceAuthorizationCondition authorizedCondition, SpaceSelectionCondition selectedCondition,
      Handler<AsyncResult<List<Space>>> handler) {
    logger.info(marker, "Getting selected spaces");

    if (authorizedCondition == null || selectedCondition == null) {
      throw new NullPointerException("authorizedCondition and selectedCondition are required");
    }

    final List<Space> result = new ArrayList<>();
    logger.info(marker, "authorizedCondition: spaceIds: {}, ownerIds {}, packages: {}", authorizedCondition.spaceIds, authorizedCondition.ownerIds, authorizedCondition.packages);
    logger.info(marker, "selectedCondition: spaceIds: {}, ownerIds {}, packages: {}, shared: {}, negateOwnerIds: {}", selectedCondition.spaceIds, selectedCondition.ownerIds, selectedCondition.packages, selectedCondition.shared, selectedCondition.negateOwnerIds);

    try {
      final Set<String> authorizedSpaces = getAuthorizedSpaces(marker, authorizedCondition);

      // get all shared spaces if the selection for shared spaces is enabled
      if (selectedCondition.shared) {
        spaces.getIndex("shared-index").query(new QuerySpec().withHashKey("shared", 1).withProjectionExpression("id")).pages()
            .forEach(p -> p.forEach(i -> {
              authorizedSpaces.add(i.getString("id"));
            }));
        logger.info(marker, "List of space IDs after addition of shared spaces: {}", authorizedSpaces);
      }

      // filter out the ones not present in the selectedCondition (null or empty represents 'do not filter')
      if (!CollectionUtils.isNullOrEmpty(selectedCondition.spaceIds)) {
        authorizedSpaces.removeIf(i -> !selectedCondition.spaceIds.contains(i));
        logger.info(marker, "List of space IDs after removal of the ones not selected by ID: {}", authorizedSpaces);
      }

      // now filter all spaceIds with the ones being selected in the selectedCondition (by checking the space's ownership) (
      if (!CollectionUtils.isNullOrEmpty(selectedCondition.ownerIds)) {
        final Set<String> ownersSpaces = new HashSet<>();
        selectedCondition.ownerIds.forEach(o ->
            spaces.getIndex("owner-index").query(new QuerySpec().withHashKey("owner", o).withProjectionExpression("id")).pages()
                .forEach(p -> p.forEach(i -> ownersSpaces.add(i.getString("id")))));

        // HINT: A ^ TRUE == !A (negateOwnerIds: keep or remove the spaces contained in the owner's spaces list)
        authorizedSpaces.removeIf(i -> !selectedCondition.negateOwnerIds ^ ownersSpaces.contains(i));
        logger.info(marker, "List of space IDs after removal of the ones not selected by owner: {}", authorizedSpaces);
      }

      // TODO selection per packages is not yet supported: selectedCondition.packages

      logger.info(marker, "Final list of space IDs to be retrieved from DynamoDB: {}", authorizedSpaces);
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

      logger.info(marker, "Number of spaces retrieved from DynamoDB: {}", result.size());
      handler.handle(Future.succeededFuture(result));
    } catch (AmazonDynamoDBException e) {
      logger.error(marker, "Failure getting authorized spaces", e);
      handler.handle(Future.failedFuture(e));
    }
  }

  private Set<String> getAuthorizedSpaces(Marker marker, SpaceAuthorizationCondition authorizedCondition) throws AmazonDynamoDBException {
    final Set<String> authorizedSpaces = new HashSet<>();

    logger.info(marker, "Getting authorized spaces by condition: {}", authorizedCondition);

    try {
      // get the space ids which are authorized by the authorizedCondition
      if (authorizedCondition.spaceIds != null) {
        authorizedSpaces.addAll(authorizedCondition.spaceIds);
        logger.info(marker, "List of space IDs after addition from authorized condition space IDs: {}", authorizedSpaces);
      }

      // then get the owners which are authorized by the authorizedCondition
      if (authorizedCondition.ownerIds != null) {
        authorizedCondition.ownerIds.forEach(owner ->
            spaces.getIndex("owner-index").query("owner", owner).pages().forEach(p -> p.forEach(i -> {
              authorizedSpaces.add(i.getString("id"));
            }))
        );
        logger.info(marker, "List of space IDs after addition from owners: {}", authorizedSpaces);
      }

      // then get the packages which are authorized by the authorizedCondition
      if (authorizedCondition.packages != null) {
        authorizedCondition.packages.forEach(packageName ->
            packages.query("packageName", packageName).pages().forEach(p -> p.forEach(i -> {
              authorizedSpaces.add(i.getString("spaceId"));
            }))
        );
        logger.info(marker, "List of space IDs after addition from packages: {}", authorizedSpaces);
      }

      // then get the "empty" case, when no spaceIds or ownerIds os packages are provided, meaning select ALL spaces
      if (CollectionUtils.isNullOrEmpty(authorizedCondition.spaceIds)
          && CollectionUtils.isNullOrEmpty(authorizedCondition.ownerIds)
          && CollectionUtils.isNullOrEmpty(authorizedCondition.packages)) {
        spaces.scan(new ScanSpec().withProjectionExpression("id")).pages()
            .forEach(p -> p.forEach(i -> authorizedSpaces.add(i.getString("id"))));
      }
    } catch (AmazonDynamoDBException e) {
      logger.error(marker, "Failure to get the authorized spaces", e);
      throw e;
    }

    logger.info(marker, "Returning the list of authorized spaces: {}", authorizedSpaces);
    return authorizedSpaces;
  }

  /**
   * Fills the result list transforming the raw elements from the outcome into real Space objects
   *
   * @param outcome the query result
   * @param result the transformed resulting elements
   */
  private void processOutcome(BatchGetItemOutcome outcome, List<Space> result) {
    outcome.getTableItems().get(dynamoClient.tableName).forEach(i -> result.add(Json.decodeValue(i.toJSON(), Space.class)));
  }
}
