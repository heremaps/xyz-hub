/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryFilter;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.hub.config.DataReferenceConfigClient;
import com.here.xyz.models.hub.DataReference;
import com.here.xyz.util.service.aws.dynamo.DynamoClient;
import com.here.xyz.util.service.aws.dynamo.IndexDefinition;
import io.vertx.core.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.here.xyz.XyzSerializable.fromMap;
import static com.here.xyz.util.service.aws.dynamo.DynamoClient.queryIndex;
import static java.util.Optional.ofNullable;

public final class DynamoDataReferenceConfigClient extends DataReferenceConfigClient {

  private static final Logger logger = LogManager.getLogger();

  private static final String PARTITION_KEY_NAME = "entityId";

  private static final String SORT_KEY_NAME = "endVersion";

  private static final String ID_INDEX_ATTRIBUTE_NAME = "id";

  private static final IndexDefinition idIndex = new IndexDefinition(ID_INDEX_ATTRIBUTE_NAME);

  private final DynamoClient dynamoClient;

  private final Table dataReferenceTable;

  public DynamoDataReferenceConfigClient(String tableArn) {
    this(tableArn, null);
  }

  public DynamoDataReferenceConfigClient(String tableArn, String endpoint) {
    this.dynamoClient = new DynamoClient(tableArn, endpoint);
    logger.info("Instantiating a reference to Dynamo Table {}", dynamoClient.tableName);
    dataReferenceTable = dynamoClient.getTable();
  }

  @Override
  protected Future<UUID> doStore(DataReference dataReference) {
    return dynamoClient.executeQueryAsync(() -> {
      if (dataReference.getId() == null) {
        dataReference.setId(UUID.randomUUID());
      }

      dataReferenceTable.putItem(Item.fromMap(dataReference.toMap()));

      return dataReference.getId();
    });
  }

  @Override
  protected Future<Optional<DataReference>> doLoad(UUID id) {
    return dynamoClient.executeQueryAsync(() ->
      queryIndex(dataReferenceTable, idIndex, id.toString())
        .stream()
        .findFirst()
        .map(item -> fromMap(item.asMap(), DataReference.class))
    );
  }

  @Override
  protected Future<List<DataReference>> doLoad(
    @Nonnull String entityId,
    @Nullable Integer startVersion,
    @Nullable Integer endVersion,
    @Nullable String contentType,
    @Nullable String objectType,
    @Nullable String sourceSystem,
    @Nullable String targetSystem
  ) {
    return dynamoClient.executeQueryAsync(() -> {
      QuerySpec query = new QuerySpec()
        .withHashKey(PARTITION_KEY_NAME, entityId);

      if (endVersion != null) {
        query.withRangeKeyCondition(new RangeKeyCondition(SORT_KEY_NAME).eq(endVersion));
      }

      QueryFilter[] filters = remainingFilters(
        startVersion,
        contentType,
        objectType,
        sourceSystem,
        targetSystem
      );
      query.withQueryFilters(filters);

      return itemCollectionToList(dataReferenceTable.query(query), DataReference.class);
    });
  }

  private static QueryFilter[] remainingFilters(
    @Nullable Integer startVersion,
    @Nullable String contentType,
    @Nullable String objectType,
    @Nullable String sourceSystem,
    @Nullable String targetSystem
  ) {
    return Stream.of(
      toOptionalEntry("startVersion", startVersion),
      toOptionalEntry("contentType", contentType),
      toOptionalEntry("objectType", objectType),
      toOptionalEntry("sourceSystem", sourceSystem),
      toOptionalEntry("targetSystem", targetSystem)
    ).flatMap(Optional::stream)
      .map(DynamoDataReferenceConfigClient::toQueryFilter)
      .toList()
      .toArray(new QueryFilter[0]);
  }

  private static Optional<? extends Entry<String, Object>> toOptionalEntry(@Nonnull String key, @Nullable Object maybeValue) {
    return ofNullable(maybeValue).map(value -> new SimpleImmutableEntry<>(key, value));
  }

  private static QueryFilter toQueryFilter(Entry<String, Object> filteringParameter) {
    return new QueryFilter(filteringParameter.getKey()).eq(filteringParameter.getValue());
  }

  private static <T> List<T> itemCollectionToList(ItemCollection<QueryOutcome> itemCollection, Class<T> resultItemClass) {
    return StreamSupport.stream(itemCollection.spliterator(), false)
      .map(Item::asMap)
      .map(itemAsMap -> XyzSerializable.fromMap(itemAsMap, resultItemClass))
      .toList();
  }

  @Override
  protected Future<Void> doDelete(UUID id) {
    return doLoad(id)
      .compose(maybeReference ->
        dynamoClient.executeQueryAsync(() -> {
          maybeReference.ifPresent(item ->
            dataReferenceTable.deleteItem(
              PARTITION_KEY_NAME,
              item.getEntityId(),
              SORT_KEY_NAME,
              item.getEndVersion()
            )
          );
          return null;
        })
      );
  }

  @Override
  public Future<Void> init() {
    if (dynamoClient.isLocal()) {
      try {
        String attributes = new StringJoiner(",")
          .add("%s:S".formatted(PARTITION_KEY_NAME))
          .add("%s:N".formatted(SORT_KEY_NAME))
          .add("%s:S".formatted(ID_INDEX_ATTRIBUTE_NAME))
          .toString();

        String keys = new StringJoiner(",")
          .add(PARTITION_KEY_NAME)
          .add(SORT_KEY_NAME)
          .toString();

        List<IndexDefinition> globalSecondaryIndices = List.of(idIndex);

        dynamoClient.createTable(dataReferenceTable.getTableName(), attributes, keys, globalSecondaryIndices, null);
      }
      catch (AmazonDynamoDBException e) {
        logger.error("Failure during creating table on {}#init()", getClass().getSimpleName(), e);
        return Future.failedFuture(e);
      }
    }
    return Future.succeededFuture();
  }

}
