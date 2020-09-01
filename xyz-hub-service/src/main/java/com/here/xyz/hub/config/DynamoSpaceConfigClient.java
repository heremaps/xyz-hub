/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.util.CollectionUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.XyzSerializable;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.util.ARN;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
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
    packages = dynamoClient.db
        .getTable(new ARN(Service.configuration.PACKAGES_DYNAMODB_TABLE_ARN).getResourceWithoutType());
  }

  @Override
  public void init(Handler<AsyncResult<Void>> onReady) {
    if (dynamoClient.isLocal()) {
      logger.info("DynamoDB running locally, initializing tables.");

      try {
        dynamoClient.createTable(spaces.getTableName(), "id:S,owner:S,shared:N", "id", "owner,shared", "exp");
        dynamoClient.createTable(packages.getTableName(), "packageName:S,spaceId:S", "packageName,spaceId", null, null);
      }
      catch (AmazonDynamoDBException e) {
        logger.error("Failure during creating tables on DynamoSpaceConfigClient init", e);
        onReady.handle(Future.failedFuture(e));
        return;
      }
    }

    onReady.handle(Future.succeededFuture());
  }

  @Override
  public void getSpace(Marker marker, String spaceId, Handler<AsyncResult<Space>> handler) {
    logger.info(marker, "Getting space with ID: {}", spaceId);

    DynamoClient.dynamoWorkers.executeBlocking(
        future -> {
          try {
            future.complete(spaces.getItem("id", spaceId));
          }
          catch (Exception e) {
            future.fail(e);
          }
        },
        ar -> {
          if (ar.failed()) {
            logger.error(marker, "Failure during getting a space from DynamoDB", ar.cause());
            handler.handle(Future.failedFuture(ar.cause()));
          }
          else {
            Item item = (Item) ar.result();
            if (item == null) {
              logger.info(marker, "Getting space with ID: {} returned null", spaceId);
              handler.handle(Future.succeededFuture(null));
              return;
            }

            final Space space = Json.decodeValue(item.toJSON(), Space.class);
            if (space != null)
              logger.info(marker, "Space ID: {} with title: \"{}\" has been decoded", spaceId, space.getTitle());
            else
              logger.info(marker, "Space ID: {} has been decoded to null", spaceId);
            handler.handle(Future.succeededFuture(space));
          }
        }
    );
  }

  @Override
  public void storeSpace(Marker marker, Space space, Handler<AsyncResult<Space>> handler) {
    logger.info(marker, "Storing space with ID: {}", space.getId());

    final Map<String, Object> itemData = XyzSerializable.STATIC_MAPPER.get().convertValue(space, new TypeReference<Map<String, Object>>() {});
    itemData.put("shared", space.isShared() ? 1 : 0); //Shared value must be a number because it's also used as index

    sanitize(itemData);
    DynamoClient.dynamoWorkers.executeBlocking(
        future -> {
          try {
            spaces.putItem(Item.fromMap(itemData));
            future.complete();
          }
          catch (Exception e) {
            future.fail(e);
          }
        },
        ar -> {
          if (ar.failed()) {
            logger.error(marker, "Failure storing a space into DynamoDB", ar.cause());
            handler.handle(Future.failedFuture(ar.cause()));
          }
          else {
            deleteSpaceFromPackage(marker, space, delFromPackageResult -> {
              if (delFromPackageResult.failed())
                handler.handle(Future.failedFuture(delFromPackageResult.cause()));
              else {
                storeSpaceIntoPackages(marker, space, storeInPackageResult -> {
                  if (storeInPackageResult.failed())
                    handler.handle(Future.failedFuture(storeInPackageResult.cause()));
                  else {
                    logger.info(marker, "Space with ID: {} has been successfully stored", space.getId());
                    handler.handle(Future.succeededFuture(space));
                  }
                });
              }
            });
          }
        }
    );
  }

  @Override
  public void deleteSpace(Marker marker, String spaceId, Handler<AsyncResult<Space>> handler) {
    logger.info(marker, "Deleting space ID: {}", spaceId);
    get(marker, spaceId, ar -> {
      if (ar.succeeded()) {
        final Space space = ar.result();
        logger.info(marker, "Space ID: {} has been retrieved", space.getId());

        deleteSpaceFromPackage(marker, space, delFromPackageResult -> {
          if (delFromPackageResult.failed())
            handler.handle(Future.failedFuture(delFromPackageResult.cause()));
          else {
            DeleteItemRequest req = new DeleteItemRequest()
                .withKey(Collections.singletonMap("id", new AttributeValue(space.getId())))
                .withTableName(spaces.getTableName());

            dynamoClient.client.deleteItemAsync(req, new AsyncHandler<DeleteItemRequest, DeleteItemResult>() {
              @Override
              public void onError(Exception exception) {
                logger.error(marker, "Failure to delete space ID: {}", space.getId());
                handler.handle(Future.failedFuture(exception));
              }

              @Override
              public void onSuccess(DeleteItemRequest request, DeleteItemResult deleteItemResult) {
                handler.handle(Future.succeededFuture(space));
              }
            });
          }
        });
      }
      else {
        logger.error(marker, "Failure to get space ID: {} during space deletion", spaceId);
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
  private void storeSpaceIntoPackages(Marker marker, Space space, Handler<AsyncResult<Void>> handler) {
    logger.info(marker, "Inserting packages {} into the packages table for space ID: {}", space.getPackages(), space.getId());
    if (space.getPackages() != null) {
      for (String packageName : space.getPackages()) {
        logger.info(marker, "Adding space ID: {} into package: {}", space.getId(), packageName);
        final Map<String, AttributeValue> packagesMap = new HashMap<>();
        packagesMap.put("packageName", new AttributeValue(packageName));
        packagesMap.put("spaceId", new AttributeValue(space.getId()));

        PutItemRequest req = new PutItemRequest()
            .withTableName(packages.getTableName())
            .withItem(packagesMap);
        dynamoClient.client.putItemAsync(req, new AsyncHandler<PutItemRequest, PutItemResult>() {
          @Override
          public void onError(Exception exception) {
            logger.error(marker, "Failure storing space ID: {} into the packages table", space.getId(), exception);
            handler.handle(Future.failedFuture(exception));
          }

          @Override
          public void onSuccess(PutItemRequest request, PutItemResult putItemResult) {
            handler.handle(Future.succeededFuture());
          }
        });
      }
    }
    handler.handle(Future.succeededFuture());
  }

  /**
   * Deletes the relationship between a space and their packages
   *
   * @param marker used in logs
   * @param space the spaceId which is being deleted
   */
  private void deleteSpaceFromPackage(Marker marker, Space space, Handler<AsyncResult<Void>> handler) {
    //TODO: Transform to async
    try {
      logger.info(marker, "Removing packages from space ID: {}", space.getId());
      final List<String> packagesList = new ArrayList<>();

      if (space.getPackages() != null) {
        packagesList.addAll(space.getPackages());
      }
      else {
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
      handler.handle(Future.succeededFuture());
    }
    catch (AmazonDynamoDBException e) {
      logger.error(marker, "Failure deleting space ID: {} from the packages table", space.getId(), e);
      handler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void getSelectedSpaces(Marker marker, SpaceAuthorizationCondition authorizedCondition, SpaceSelectionCondition selectedCondition,
      PropertiesQuery propsQuery, Handler<AsyncResult<List<Space>>> handler) {
    logger.info(marker, "Getting selected spaces");

    if (authorizedCondition == null || selectedCondition == null)
      throw new NullPointerException("authorizedCondition and selectedCondition are required");

    logger.debug(marker, "authorizedCondition: spaceIds: {}, ownerIds {}, packages: {}", authorizedCondition.spaceIds, authorizedCondition.ownerIds, authorizedCondition.packages);
    logger.debug(marker, "selectedCondition: spaceIds: {}, ownerIds {}, packages: {}, shared: {}, negateOwnerIds: {}", selectedCondition.spaceIds, selectedCondition.ownerIds, selectedCondition.packages, selectedCondition.shared, selectedCondition.negateOwnerIds);

    DynamoClient.dynamoWorkers.executeBlocking(
        future -> {
          final List<Space> result = new ArrayList<>();
          try {
            final Set<String> authorizedSpaces = getAuthorizedSpaces(marker, authorizedCondition);

            // get all shared spaces if the selection for shared spaces is enabled
            if (selectedCondition.shared) {
              spaces.getIndex("shared-index").query(new QuerySpec().withHashKey("shared", 1).withProjectionExpression("id")).pages()
                  .forEach(p -> p.forEach(i -> {
                    authorizedSpaces.add(i.getString("id"));
                  }));
              logger.debug(marker, "Number of space IDs after addition of shared spaces: {}", authorizedSpaces.size());
            }
            
            if (propsQuery != null) {
              final Set<String> contentUpdatedSpaces = new HashSet<>();

              propsQuery.forEach(conjunctions -> {
                List<String> contentUpdatedAtConjunctions = new ArrayList<>();
                Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
                conjunctions.forEach(conj -> {
                    conj.getValues().forEach(v -> {
                      int size = contentUpdatedAtConjunctions.size();
                      contentUpdatedAtConjunctions.add("contentUpdatedAt " + SQLQuery.getOperation(conj.getOperation()) + " :" + size);
                      expressionAttributeValues.put(":" + size, v );
                    });
                });

                // filter out spaces with contentUpatedAt property
                spaces.scan(
                  new ScanSpec()
                  .withProjectionExpression("id, contentUpdatedAt")
                  .withFilterExpression(StringUtils.join(contentUpdatedAtConjunctions, " OR "))
                  .withValueMap(expressionAttributeValues)
                ).pages()
                .forEach(p -> p.forEach(i -> contentUpdatedSpaces.add(i.getString("id"))));
              });

              // filter out spaces which are not present in contentUpdateSpaces
              authorizedSpaces.removeIf(i -> !contentUpdatedSpaces.contains(i));
              logger.debug(marker, "Number of space IDs after removal of the ones filtered by contentUpdatedAt: {}", authorizedSpaces.size());
            }
            
            // filter out the ones not present in the selectedCondition (null or empty represents 'do not filter')
            if (!CollectionUtils.isNullOrEmpty(selectedCondition.spaceIds)) {
              authorizedSpaces.removeIf(i -> !selectedCondition.spaceIds.contains(i));
              logger.debug(marker, "Number of space IDs after removal of the ones not selected by ID: {}", authorizedSpaces.size());
            }

            // now filter all spaceIds with the ones being selected in the selectedCondition (by checking the space's ownership) (
            if (!CollectionUtils.isNullOrEmpty(selectedCondition.ownerIds)) {
              final Set<String> ownersSpaces = new HashSet<>();
              selectedCondition.ownerIds.forEach(o ->
                  spaces.getIndex("owner-index").query(new QuerySpec().withHashKey("owner", o).withProjectionExpression("id")).pages()
                      .forEach(p -> p.forEach(i -> ownersSpaces.add(i.getString("id")))));

              // HINT: A ^ TRUE == !A (negateOwnerIds: keep or remove the spaces contained in the owner's spaces list)
              authorizedSpaces.removeIf(i -> !selectedCondition.negateOwnerIds ^ ownersSpaces.contains(i));
              logger.debug(marker, "Number of space IDs after removal of the ones not selected by owner: {}", authorizedSpaces.size());
            }

            // TODO selection per packages is not yet supported: selectedCondition.packages

            logger.info(marker, "Final number of space IDs to be retrieved from DynamoDB: {}", authorizedSpaces.size());
            if (!authorizedSpaces.isEmpty()) {
              int batches = (int) Math.ceil((double) authorizedSpaces.size() / 100);
              for (int i = 0; i < batches; i++) {
                final TableKeysAndAttributes keys = new TableKeysAndAttributes(dynamoClient.tableName);
                authorizedSpaces.stream().skip(i * 100).limit(100).forEach(id -> keys.addHashOnlyPrimaryKey("id", id));

                BatchGetItemOutcome outcome = dynamoClient.db.batchGetItem(keys);
                processOutcome(outcome, result);

                while (!outcome.getUnprocessedKeys().isEmpty()) {
                  outcome = dynamoClient.db.batchGetItemUnprocessed(outcome.getUnprocessedKeys());
                  processOutcome(outcome, result);
                }
              }
            }
            future.complete(result);
          }
          catch (Exception e) {
            future.fail(e);
          }
        },
        ar -> {
          if (ar.failed()) {
            logger.error(marker, "Failure getting authorized spaces", ar.cause());
            handler.handle(Future.failedFuture(ar.cause()));
          }
          else {
            final List<Space> result = (List<Space>) ar.result();
            logger.info(marker, "Number of spaces retrieved from DynamoDB: {}", result.size());
            handler.handle(Future.succeededFuture(result));
          }
        }
    );
  }

  private Set<String> getAuthorizedSpaces(Marker marker, SpaceAuthorizationCondition authorizedCondition) throws AmazonDynamoDBException {
    final Set<String> authorizedSpaces = new LinkedHashSet<>();

    logger.info(marker, "Getting authorized spaces by condition");

    try {
      // get the space ids which are authorized by the authorizedCondition
      if (authorizedCondition.spaceIds != null) {
        authorizedSpaces.addAll(authorizedCondition.spaceIds);
        logger.debug(marker, "Number of space IDs after addition from authorized condition space IDs: {}", authorizedSpaces.size());
      }

      // then get the owners which are authorized by the authorizedCondition
      if (authorizedCondition.ownerIds != null) {
        authorizedCondition.ownerIds.forEach(owner ->
            spaces.getIndex("owner-index").query("owner", owner).pages().forEach(p -> p.forEach(i -> {
              authorizedSpaces.add(i.getString("id"));
            }))
        );
        logger.debug(marker, "Number of space IDs after addition from owners: {}", authorizedSpaces.size());
      }

      // then get the packages which are authorized by the authorizedCondition
      if (authorizedCondition.packages != null) {
        authorizedCondition.packages.forEach(packageName ->
            packages.query("packageName", packageName).pages().forEach(p -> p.forEach(i -> {
              authorizedSpaces.add(i.getString("spaceId"));
            }))
        );
        logger.debug(marker, "Number of space IDs after addition from packages: {}", authorizedSpaces.size());
      }

      // then get the "empty" case, when no spaceIds or ownerIds os packages are provided, meaning select ALL spaces
      if (CollectionUtils.isNullOrEmpty(authorizedCondition.spaceIds)
          && CollectionUtils.isNullOrEmpty(authorizedCondition.ownerIds)
          && CollectionUtils.isNullOrEmpty(authorizedCondition.packages)) {
        spaces.scan(new ScanSpec().withProjectionExpression("id")).pages()
            .forEach(p -> p.forEach(i -> authorizedSpaces.add(i.getString("id"))));
      }
    }
    catch (AmazonDynamoDBException e) {
      logger.error(marker, "Failure to get the authorized spaces", e);
      throw e;
    }

    logger.info(marker, "Returning the list of authorized spaces with size of: {}", authorizedSpaces.size());
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

  /**
   * Deep search removing values which contains empty string.
   * @param obj a Map or a List to suffer the transformation
   * @return the size of the resulting map or list after sanitization
   */
  private static int sanitize(Object obj) {
    if (!(obj instanceof Map || obj instanceof List)) return -1;

    final Collection values = obj instanceof Map ? ((Map) obj).values() : (List) obj;
    final Iterator i = values.iterator();
    int size = values.size();

    while (i.hasNext()) {
      Object value = i.next();
      if ("".equals(value)) {
        i.remove();
        size--;
      } else if (value instanceof List || value instanceof Map) {
        if (sanitize(value) == 0) {
          i.remove();
          size--;
        }
      }
    }

    return size;
  }
}
