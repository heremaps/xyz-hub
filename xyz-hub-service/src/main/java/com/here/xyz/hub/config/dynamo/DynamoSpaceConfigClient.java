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

import static com.here.xyz.hub.Service.configuration;
import static io.vertx.core.json.jackson.DatabindCodec.mapper;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.util.CollectionUtils;
import com.google.common.base.Strings;
import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.hub.config.SpaceConfigClient;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.util.ARN;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.aws.dynamo.DynamoClient;
import com.here.xyz.util.service.aws.dynamo.IndexDefinition;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;

import java.net.http.HttpResponse;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class DynamoSpaceConfigClient extends SpaceConfigClient {

  private static final Logger logger = LogManager.getLogger();

  private final DynamoClient dynamoClient;
  private Table spaces;
  private Table packages;

  public DynamoSpaceConfigClient(final String tableArn) {
    dynamoClient = new DynamoClient(tableArn, null);

    logger.info("Instantiating a reference to Dynamo Table {}", dynamoClient.tableName);
    spaces = dynamoClient.db.getTable(dynamoClient.tableName);
    packages = dynamoClient.db.getTable(new ARN(configuration.PACKAGES_DYNAMODB_TABLE_ARN).getResourceWithoutType());
  }

  public static class Provider extends SpaceConfigClient.Provider {
    @Override
    public boolean chooseMe() {
      return configuration.SPACES_DYNAMODB_TABLE_ARN != null && !"test".equals(System.getProperty("scope"));
    }

    @Override
    protected SpaceConfigClient getInstance() {
      return new DynamoSpaceConfigClient(configuration.SPACES_DYNAMODB_TABLE_ARN);
    }
  }

  @Override
  public Future<Void> init() {
    if (dynamoClient.isLocal()) {
      logger.info("DynamoDB running locally, initializing tables.");

      try {
        List<IndexDefinition> indexes = List.of(
                new IndexDefinition("extendsFrom"),
                new IndexDefinition("owner"),
                new IndexDefinition("shared"),
                new IndexDefinition("region"),
                new IndexDefinition("type", "contentUpdatedAt")
        );
        dynamoClient.createTable(spaces.getTableName(), "id:S,owner:S,shared:N,region:S,type:S,contentUpdatedAt:N,extendsFrom:S", "id", indexes, "exp");
        dynamoClient.createTable(packages.getTableName(), "packageName:S,spaceId:S", "packageName,spaceId", null, null);
      }
      catch (AmazonDynamoDBException e) {
        logger.error("Failure during creating tables on DynamoSpaceConfigClient init", e);
        return Future.failedFuture(e);
      }
    }

    return Future.succeededFuture();
  }

  @Override
  public Future<Space> getSpace(Marker marker, String spaceId) {
    return dynamoClient.executeQueryAsync(() -> {
      logger.info(marker, "Getting space with ID: {}", spaceId);

      Item spaceItem = spaces.getItem("id", spaceId);
      if (spaceItem == null) {
        logger.info(marker, "Getting space with ID: {} returned null", spaceId);
        return null;
      }
      else {
        Map<String, Object> itemData = spaceItem.asMap();
        itemData.put("shared", ((Number) itemData.get("shared")).intValue() == 1);
        //NOTE: The following is a temporary implementation to keep backwards compatibility for non-versioned spaces
        itemData.putIfAbsent("versionsToKeep", 0);
        final Space space = mapper().convertValue(itemData, Space.class);
        if (space != null)
          logger.info(marker, "Space ID: {} with title: \"{}\" has been decoded", spaceId, space.getTitle());
        else
          logger.info(marker, "Space ID: {} has been decoded to null", spaceId);
        return space;
      }
    });
  }

  @Override
  public Future<Void> storeSpace(Marker marker, Space space) {
    logger.info(marker, "Storing space with ID: {}", space.getId());
    return get(marker, space.getId())
        .compose(originalSpace -> storeSpace(marker, space, originalSpace));
  }

  private Future<Void> storeSpace(Marker marker, Space space, Space originalSpace) {
    boolean deletePackages, insertPackages;

    if (originalSpace == null) {
      //This is a space creation
      deletePackages = false;
      insertPackages = space.getPackages() != null && !space.getPackages().isEmpty();
    }
    else
      //This is a space update
      deletePackages = insertPackages = !packagesEqual(originalSpace.getPackages(), space.getPackages());

    final boolean delPackages = deletePackages, insPackages = insertPackages;

    return dynamoClient.executeQueryAsync(() -> {
      final Map<String, Object> itemData = XyzSerializable.toMap(space, Static.class);
      itemData.put("shared", space.isShared() ? 1 : 0); //Shared value must be a number because it's also used as index
      itemData.put("type", "SPACE");

      if (space.getExtension() != null && !Strings.isNullOrEmpty(space.getExtension().getSpaceId()))
        itemData.put("extendsFrom", space.getExtension().getSpaceId());

      sanitize(itemData);
      spaces.putItem(Item.fromMap(itemData));
      return null;
    })
        .onFailure(t -> logger.error(marker, "Failure storing a space into DynamoDB", t))
        .compose(v -> delPackages ? deleteSpaceFromPackages(marker, originalSpace) : Future.succeededFuture())
        .compose(v -> insPackages ? storeSpaceIntoPackages(marker, space) : Future.succeededFuture())
        .onSuccess(v -> logger.info(marker, "Space with ID: {} has been successfully stored", space.getId()));
  }

  private boolean packagesEqual(List<String> packages1, List<String> packages2) {
    return Objects.equals(packages1 != null ? new HashSet<>(packages1) : null, packages2 != null ? new HashSet<>(packages2) : null);
  }

  @Override
  public Future<Space> deleteSpace(Marker marker, String spaceId) {
    logger.info(marker, "Deleting space ID: {}", spaceId);
    return get(marker, spaceId)
        .onFailure(t -> logger.error(marker, "Failure to get space ID: {} during space deletion", spaceId, t))
        .compose(space -> {
          if (space == null) {
              String errMsg = String.format("Space ID '%s' - space is null during space deletion", spaceId);
              return Future.failedFuture(new HttpException(HttpResponseStatus.NOT_FOUND,errMsg));
          } else {
              logger.info(marker, "Space ID: {} has been retrieved", space.getId());
              return Future.succeededFuture(space);
          }
        })
        .compose(space -> deleteSpaceFromPackages(marker, space).map(space))
        .compose(space -> {
          Promise<Space> p = Promise.promise();
          //Finally delete the space
          DeleteItemRequest req = new DeleteItemRequest()
              .withKey(Collections.singletonMap("id", new AttributeValue(space.getId())))
              .withTableName(spaces.getTableName());

          dynamoClient.client.deleteItemAsync(req, new AsyncHandler<DeleteItemRequest, DeleteItemResult>() {
            @Override
            public void onError(Exception exception) {
              logger.error(marker, "Failure to delete space ID: {}", space.getId());
              p.fail(exception);
            }

            @Override
            public void onSuccess(DeleteItemRequest request, DeleteItemResult deleteItemResult) {
              p.complete(space);
            }
          });
          return p.future();
        });
  }

  /**
   * Stores the relation between package name and spaces in Dynamo
   *
   * @param marker used in logs
   * @param space the space which is being stored
   */
  private Future<Void> storeSpaceIntoPackages(Marker marker, Space space) {
    logger.info(marker, "Inserting packages {} into the packages table for space ID: {}", space.getPackages(), space.getId());
    Future<Void> f = Future.succeededFuture();
    if (space.getPackages() != null) {
      for (String packageName : space.getPackages()) {
        logger.info(marker, "Adding space ID: {} into package: {}", space.getId(), packageName);
        f.compose(v -> storeSpaceIntoPackage(space, packageName)
            .onFailure(t -> logger.error(marker, "Failure storing space ID: {} into the packages table", space.getId(), t)));
      }
    }
    return f;
  }

  private Future<Void> storeSpaceIntoPackage(Space space, String packageName) {
    Promise<Void> p = Promise.promise();
    PutItemRequest req = new PutItemRequest()
        .withTableName(packages.getTableName())
        .withItem(new HashMap<String, AttributeValue>() {{
          put("packageName", new AttributeValue(packageName));
          put("spaceId", new AttributeValue(space.getId()));
        }});
    dynamoClient.client.putItemAsync(req, new AsyncHandler<PutItemRequest, PutItemResult>() {
      @Override
      public void onError(Exception exception) {
        p.fail(exception);
      }

      @Override
      public void onSuccess(PutItemRequest request, PutItemResult putItemResult) {
        p.complete();
      }
    });
    return p.future();
  }

  /**
   * Deletes the relationship between a space and their packages
   *
   * @param marker used in logs
   * @param space the spaceId which is being deleted
   */
  private Future<Void> deleteSpaceFromPackages(Marker marker, Space space) {
    logger.info(marker, "Removing packages from space ID: {}", space.getId());
    return getPackages(space)
        .onSuccess(packages -> logger.info(marker, "Packages {} to be removed from space ID: {}", packages, space.getId()))
        .compose(packages -> {
          Future<Void> f = Future.succeededFuture();
          for (String packageName : packages) {
            f.compose(v -> deleteSpaceFromPackage(space, packageName)
                .onFailure(t -> logger.error(marker, "Failure deleting space ID: {} from the packages table", space.getId(), t)));
          }
          return f;
        });
  }

  private Future<Void> deleteSpaceFromPackage(Space space, String packageName) {
    Promise<Void> p = Promise.promise();
    DeleteItemRequest req = new DeleteItemRequest()
        .withTableName(packages.getTableName())
        .withKey(
            new AbstractMap.SimpleEntry<>("packageName", new AttributeValue(packageName)),
            new AbstractMap.SimpleEntry<>("spaceId", new AttributeValue(space.getId()))
        );
    dynamoClient.client.deleteItemAsync(req, new AsyncHandler<DeleteItemRequest, DeleteItemResult>() {
      @Override
      public void onError(Exception exception) {
        p.fail(exception);
      }

      @Override
      public void onSuccess(DeleteItemRequest request, DeleteItemResult deleteItemResult) {
        p.complete();
      }
    });
    return p.future();
  }

  private Future<List<String>> getPackages(Space space) {
    Promise<List<String>> p = Promise.promise();
    if (space.getPackages() != null)
      p.complete(space.getPackages());
    else {
      GetItemRequest req = new GetItemRequest()
          .withTableName(spaces.getTableName())
          .withKey(Collections.singletonMap("id", new AttributeValue(space.getId())))
          .withProjectionExpression("packages");
      dynamoClient.client.getItemAsync(req, new AsyncHandler<GetItemRequest, GetItemResult>() {
        @Override
        public void onError(Exception exception) {
          p.fail(exception);
        }

        @Override
        public void onSuccess(GetItemRequest request, GetItemResult getItemResult) {
          if (getItemResult.getItem() != null && getItemResult.getItem().containsKey("packages"))
            p.complete(getItemResult.getItem().get("packages").getL().stream().map(AttributeValue::getS).collect(Collectors.toList()));
          else
            p.complete(Collections.emptyList());
        }
      });
    }
    return p.future();
  }

  @Override
  protected Future<List<Space>> getSelectedSpaces(Marker marker, SpaceAuthorizationCondition authorizedCondition,
      SpaceSelectionCondition selectedCondition, PropertiesQuery propsQuery) {
    logger.info(marker, "Getting selected spaces");

    if (authorizedCondition == null || selectedCondition == null)
      throw new NullPointerException("authorizedCondition and selectedCondition are required");

    if (selectedCondition.packages != null && !selectedCondition.packages.isEmpty())
      throw new NotImplementedException("Space selection by package(s) is not implemented.");

    logger.debug(marker, "authorizedCondition: spaceIds: {}, ownerIds {}, packages: {}", authorizedCondition.spaceIds,
        authorizedCondition.ownerIds, authorizedCondition.packages);
    logger.debug(marker, "selectedCondition: spaceIds: {}, ownerIds {}, packages: {}, shared: {}, negateOwnerIds: {}, region: {}",
        selectedCondition.spaceIds, selectedCondition.ownerIds, selectedCondition.packages, selectedCondition.shared,
        selectedCondition.negateOwnerIds, selectedCondition.region);

    return getSelectedSpacesSync(marker, authorizedCondition, selectedCondition, propsQuery)
        .onSuccess(spaces -> logger.info(marker, "Number of spaces retrieved from DynamoDB: {}", spaces.size()))
        .onFailure(t -> logger.error(marker, "Failure getting authorized spaces", t));
  }

  @Override
  public Future<List<Space>> getSpacesFromSuper(Marker marker, String superSpaceId) {
    return dynamoClient.executeQueryAsync(() -> {
      final List<Space> resultSpaces = new ArrayList<>();
      spaces.getIndex("extendsFrom-index")
          .query(new QuerySpec().withHashKey("extendsFrom", superSpaceId))
          .pages()
          .forEach(page -> page.forEach(spaceItem -> resultSpaces.add(mapItemToSpace(spaceItem))));
      return resultSpaces;
    });
  }

  private Future<List<Space>> getSelectedSpacesSync(Marker marker, SpaceAuthorizationCondition authorizedCondition,
      SpaceSelectionCondition selectedCondition, PropertiesQuery propsQuery) {

    if (!CollectionUtils.isNullOrEmpty(selectedCondition.spaceIds))
      return getSpace(marker, selectedCondition.spaceIds.iterator().next())
          .compose(space -> Future.succeededFuture(space != null ? Collections.singletonList(space) : Collections.emptyList()));

    boolean hasFullAccess = CollectionUtils.isNullOrEmpty(authorizedCondition.spaceIds) &&
            CollectionUtils.isNullOrEmpty(authorizedCondition.ownerIds) &&
            CollectionUtils.isNullOrEmpty(authorizedCondition.packages);

    if (hasFullAccess)
      return getSpacesWithFullAccess(selectedCondition, propsQuery);
    else
      return getSpacesWithoutFullAccess(marker, authorizedCondition, selectedCondition, propsQuery);
  }

  private Future<List<Space>> getSpacesWithFullAccess(SpaceSelectionCondition selectedCondition, PropertiesQuery propsQuery) {
    //TODO: Improve method to not use PropertiesQuery here anymore, but more specific predicate types
    return dynamoClient.executeQueryAsync(() -> {
      Map<String, Object> valueMap = new HashMap<>();
      List<Space> resultSpaces = new ArrayList<>();
      if (propsQuery != null) {
        valueMap.put(":typeValue", "SPACE");
        valueMap.put(":contentUpdatedAtValue", propsQuery.get(0).get(0).getValues().get(0));
        String operator = QueryOperation.getOutputRepresentation(propsQuery.get(0).get(0).getOperation());

        spaces.getIndex("type-contentUpdatedAt-index")
            .query(new QuerySpec()
                .withKeyConditionExpression("#type = :typeValue and contentUpdatedAt " + operator + " :contentUpdatedAtValue")
                .withNameMap(Map.of("#type", "type"))
                .withValueMap(valueMap)
            )
            .pages()
            .forEach(page -> page.forEach(spaceItem -> resultSpaces.add(mapItemToSpace(spaceItem))));
        //TODO: Extract & re-use page iteration / transformation of item list
      }
      else if (!CollectionUtils.isNullOrEmpty(selectedCondition.ownerIds) && !selectedCondition.negateOwnerIds) {
        selectedCondition.ownerIds.forEach(ownerId ->
            spaces.getIndex("owner-index")
                .query(new QuerySpec().withHashKey("owner", ownerId))
                .pages()
                .forEach(page -> page.forEach(spaceItem -> resultSpaces.add(mapItemToSpace(spaceItem)))));
      }
      else if (selectedCondition.region != null) {
        spaces.getIndex("region-index")
            .query(new QuerySpec().withHashKey("region", selectedCondition.region))
            .pages()
            .forEach(page -> page.forEach(spaceItem -> resultSpaces.add(mapItemToSpace(spaceItem))));
      }
      else {
        //If there are no filtering conditions we need to fetch all spaces, we can do that with type index + contentUpdated > 0
        valueMap.put(":typeValue", "SPACE");
        valueMap.put(":contentUpdatedAtValue", 0L);

        spaces
            .getIndex("type-contentUpdatedAt-index")
            .query(new QuerySpec()
                .withKeyConditionExpression("#type = :typeValue and contentUpdatedAt > :contentUpdatedAtValue")
                .withNameMap(Map.of("#type", "type"))
                .withValueMap(valueMap))
            .pages()
            .forEach(page -> page.forEach(spaceItem -> resultSpaces.add(mapItemToSpace(spaceItem))));
      }

      //Filter by region
      if (selectedCondition.region != null)
        resultSpaces.removeIf(space -> space.getRegion() == null || !space.getRegion().equals(selectedCondition.region));

      //Filter by owner
      if (!CollectionUtils.isNullOrEmpty(selectedCondition.ownerIds)) {
        Predicate<Space> condition = selectedCondition.negateOwnerIds ?
            space -> selectedCondition.ownerIds.contains(space.getOwner()) :
            space -> !selectedCondition.ownerIds.contains(space.getOwner());
        resultSpaces.removeIf(condition);
      }

      //Filter by prefix
      if (selectedCondition.prefix != null)
        resultSpaces.removeIf(space -> !space.getId().startsWith(selectedCondition.prefix));

      //Filter by selected spaces
      if (!CollectionUtils.isNullOrEmpty(selectedCondition.spaceIds))
        resultSpaces.removeIf(space -> !selectedCondition.spaceIds.contains(space.getId()));

      return resultSpaces;
    });
  }

  private Future<List<Space>> getSpacesWithoutFullAccess(Marker marker, SpaceAuthorizationCondition authorizedCondition,
      SpaceSelectionCondition selectedCondition, PropertiesQuery propsQuery) {
    return getAllSpaceIds(authorizedCondition, selectedCondition)
        .compose(allSpaceIds -> {
          //Filter by prefix
          if (selectedCondition.prefix != null)
            allSpaceIds.removeIf(i -> !i.startsWith(selectedCondition.prefix));
          return Future.succeededFuture(allSpaceIds);
        })
        .compose(allSpaceIds -> filterByRegion(selectedCondition, allSpaceIds).map(allSpaceIds))
        .compose(allSpaceIds -> filterByOwner(selectedCondition, allSpaceIds).map(allSpaceIds))
        .compose(allSpaceIds -> filterByContentUpdatedAt(propsQuery, allSpaceIds).map(allSpaceIds))
        .onSuccess(allSpaceIds -> logger.debug(marker, "Final number of space IDs to be retrieved from DynamoDB: {}", allSpaceIds.size()))
        .compose(allSpaceIds -> getSpacesByIdsAndUpdate(allSpaceIds));
  }

  private Future<Set<String>> getAllSpaceIds(SpaceAuthorizationCondition authorizedCondition, SpaceSelectionCondition selectedCondition) {
    return dynamoClient.executeQueryAsync(() -> {
      Set<String> result = new HashSet<>();
      result.addAll(authorizedCondition.spaceIds);
      result.addAll(getSharedSpaceIds(selectedCondition));
      result.addAll(getOwnersSpaceIds(authorizedCondition));
      result.addAll(getPackageSpaceIds(authorizedCondition));
      return result;
    });
  }

  private Set<String> getSharedSpaceIds(SpaceSelectionCondition selectedCondition) {
    var sharedSpaceIds = new HashSet<String>();
    if (selectedCondition.shared) {
      spaces.getIndex("shared-index")
              .query(new QuerySpec().withHashKey("shared", 1)
                      .withProjectionExpression("id"))
              .pages()
              .forEach(page -> page.forEach(i -> sharedSpaceIds.add(i.getString("id"))));
    }
    return sharedSpaceIds;
  }

  private Set<String> getOwnersSpaceIds(SpaceAuthorizationCondition authorizedCondition) {
    var ownersSpaceIds = new HashSet<String>();
    authorizedCondition.ownerIds.forEach(ownerId ->
            spaces.getIndex("owner-index")
                    .query(new QuerySpec().withHashKey("owner", ownerId).withProjectionExpression("id"))
                    .pages()
                    .forEach(page -> page.forEach(i -> ownersSpaceIds.add(i.getString("id")))));
    return ownersSpaceIds;
  }

  private Set<String> getPackageSpaceIds(SpaceAuthorizationCondition authorizedCondition) {
    var packageSpaceIds = new HashSet<String>();
    if (!CollectionUtils.isNullOrEmpty(authorizedCondition.packages)) {
      authorizedCondition.packages.forEach(packageName ->
              packages.query("packageName", packageName)
                      .pages()
                      .forEach(page -> page.forEach(i -> packageSpaceIds.add(i.getString("spaceId"))))
      );
    }
    return packageSpaceIds;
  }

  private Future<List<Space>> getSpacesByIdsAndUpdate(Set<String> ids) {
    if (ids.isEmpty())
      return Future.succeededFuture(Collections.emptyList());

    return dynamoClient.executeQueryAsync(() -> {
      List<Space> spaces = new LinkedList<>();
      int batches = (int) Math.ceil((double) ids.size() / 100);
      for (int i = 0; i < batches; i++) {
        final TableKeysAndAttributes keys = new TableKeysAndAttributes(dynamoClient.tableName);
        ids.stream().skip(i * 100L).limit(100).forEach(id -> keys.addHashOnlyPrimaryKey("id", id));

        BatchGetItemOutcome outcome = dynamoClient.db.batchGetItem(keys);
        spaces.addAll(processOutcome(outcome));

        while (!outcome.getUnprocessedKeys().isEmpty()) {
          outcome = dynamoClient.db.batchGetItemUnprocessed(outcome.getUnprocessedKeys());
          spaces.addAll(processOutcome(outcome));
        }
      }
      return spaces;
    });
  }

  private Future<Void> filterByRegion(SpaceSelectionCondition selectedCondition, Set<String> allSpaceIds) {
    if (selectedCondition.region == null)
      return Future.succeededFuture();

    return dynamoClient.executeQueryAsync(() -> {
      Set<String> regionSpaceIds = new HashSet<>();
      spaces.getIndex("region-index")
          .query(new QuerySpec().withHashKey("region", selectedCondition.region).withProjectionExpression("id"))
          .pages()
          .forEach(page -> page.forEach(i -> regionSpaceIds.add(i.getString("id"))));
      allSpaceIds.removeIf(id -> !regionSpaceIds.contains(id));
      return null;
    });
  }

  private Future<Void> filterByOwner(SpaceSelectionCondition selectedCondition, Set<String> allSpaceIds) {
    if (CollectionUtils.isNullOrEmpty(selectedCondition.ownerIds))
      return Future.succeededFuture();

    return dynamoClient.executeQueryAsync(() -> {
      Set<String> selectedOwnersSpaceIds = new HashSet<>();
      selectedCondition.ownerIds.forEach(ownerId ->
          spaces.getIndex("owner-index")
              .query(new QuerySpec().withHashKey("owner", ownerId).withProjectionExpression("id"))
              .pages()
              .forEach(page -> page.forEach(i -> selectedOwnersSpaceIds.add(i.getString("id")))));

      // HINT: A ^ TRUE == !A (negateOwnerIds: keep or remove the spaces contained in the owner's spaces list)
      allSpaceIds.removeIf(i -> !selectedCondition.negateOwnerIds ^ selectedOwnersSpaceIds.contains(i));
      return null;
    });
  }

  private Future<Void> filterByContentUpdatedAt(PropertiesQuery propsQuery, Set<String> allSpaceIds) {
    //TODO: Improve method to not use PropertiesQuery here anymore, but more specific predicate types
    if (propsQuery == null)
      return Future.succeededFuture();

    return dynamoClient.executeQueryAsync(() -> {
      Map<String, Object> valueMap = new HashMap<>();
      valueMap.put(":typeValue", "SPACE");
      valueMap.put(":contentUpdatedAtValue", propsQuery.get(0).get(0).getValues().get(0));
      String operator = QueryOperation.getOutputRepresentation(propsQuery.get(0).get(0).getOperation());
      var contentUpdatedAtSpaceIds = new HashSet<String>();
      spaces.getIndex("type-contentUpdatedAt-index").query(new QuerySpec()
              .withKeyConditionExpression("#type = :typeValue and contentUpdatedAt " +  operator + " :contentUpdatedAtValue")
              .withNameMap(Map.of("#type", "type"))
              .withValueMap(valueMap)
              .withProjectionExpression("id")
          ).pages()
          .forEach(page -> page.forEach(i -> contentUpdatedAtSpaceIds.add(i.getString("id"))));
      allSpaceIds.removeIf(id -> !contentUpdatedAtSpaceIds.contains(id));
      return null;
    });
  }

  private Space mapItemToSpace(Item item) {
    var itemData = item.asMap();
    itemData.put("shared", ((Number) itemData.get("shared")).intValue() == 1);
    //NOTE: The following is a temporary implementation to keep backwards compatibility for non-versioned spaces
    itemData.putIfAbsent("versionsToKeep", 0);
    return DatabindCodec.mapper().convertValue(itemData, Space.class);
  }

  /**
   * Fills the result list transforming the raw elements from the outcome into real Space objects
   *
   * @param outcome the query result
   */
  private List<Space> processOutcome(BatchGetItemOutcome outcome) {
    List<Space> spaces = new ArrayList<>();
    outcome.getTableItems().get(dynamoClient.tableName).forEach(spaceItem -> spaces.add(Json.decodeValue(spaceItem.toJSON(), Space.class)));
    return spaces;
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
      }
      else if (value instanceof List || value instanceof Map) {
        if (sanitize(value) == 0) {
          i.remove();
          size--;
        }
      }
    }

    return size;
  }
}
