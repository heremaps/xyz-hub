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
package com.here.xyz.hub.config;

/**
 * A client for reading and editing xyz space and connector definitions.
 */
public class JDBCSpaceConfigClient {
  //
  //  private static JDBCSpaceConfigClient instance;
  //  private final SQLClient client;
  //
  //  private JDBCSpaceConfigClient() {
  //    client = JDBCConfig.getClient();
  //  }
  //
  //  public static JDBCSpaceConfigClient getInstance() {
  //    if (instance == null) {
  //      instance = new JDBCSpaceConfigClient();
  //    }
  //    return instance;
  //  }
  //
  //  @Override
  //  public void init(Handler<AsyncResult<Void>> onReady) {
  //    JDBCConfig.init(onReady);
  //  }
  //
  //
  //  private Future<Void> updateWithParams(Space modifiedObject, SQLQueryExt query) {
  //    Promise<Void> p = Promise.promise();
  //    client.updateWithParams(query.text(), new JsonArray(query.parameters()), out -> {
  //      if (out.succeeded())
  //        p.complete();
  //      else
  //        p.fail(out.cause());
  //    });
  //    return p.future();
  //  }
  //
  //  @Override
  //  public Future<Space> getSpace(Marker marker, String spaceId) {
  //    Promise<Space> p = Promise.promise();
  //    SQLQueryExt query = new SQLQueryExt("SELECT config FROM " + SPACE_TABLE + " WHERE id = ?", spaceId);
  //    client.queryWithParams(query.text(), new JsonArray(query.parameters()), out -> {
  //      if (out.succeeded()) {
  //        Optional<String> config = out.result().getRows().stream().map(r -> r.getString("config")).findFirst();
  //        if (config.isPresent()) {
  //          Space space = Json.decodeValue(config.get(), Space.class);
  //          p.complete(space);
  //        }
  //        else
  //          p.complete();
  //      }
  //      else
  //        p.fail(out.cause());
  //    });
  //    return p.future();
  //  }
  //
  //  @Override
  //  protected Future<Void> storeSpace(Marker marker, Space space) {
  //    SQLQueryExt query = null;
  //    try {
  //      query = new SQLQueryExt(
  //          "INSERT INTO " + SPACE_TABLE + " (id, owner, cid, config) VALUES (?, ?, ?, cast(? as JSONB)) ON CONFLICT
  // (id) DO UPDATE SET owner = excluded.owner, cid = excluded.cid, config = excluded.config",
  //          space.getId(), space.getOwner(), space.getCid(),
  // XyzSerializable.STATIC_MAPPER.get().writeValueAsString(space));
  //      return updateWithParams(space, query).mapEmpty();
  //    }
  //    catch (JsonProcessingException e) {
  //      return Future.failedFuture(new EncodeException("Failed to encode as JSON: " + e.getMessage(), e));
  //    }
  //  }
  //
  //  @Override
  //  protected Future<Space> deleteSpace(Marker marker, String spaceId) {
  //    SQLQueryExt query = new SQLQueryExt("DELETE FROM " + SPACE_TABLE + " WHERE id = ?", spaceId);
  //    return get(marker, spaceId).compose(space -> updateWithParams(space, query).map(space));
  //  }
  //
  //  @Override
  //  protected Future<List<Space>> getSelectedSpaces(Marker marker, SpaceAuthorizationCondition authorizedCondition,
  //      SpaceSelectionCondition selectedCondition, PropertyQueryOr propsQuery) {
  //    //BUILD THE QUERY
  //    List<String> whereConjunctions = new ArrayList<>();
  //    String baseQuery = "SELECT config FROM " + SPACE_TABLE;
  //    List<String> authorizationWhereClauses = generateWhereClausesFor(authorizedCondition);
  //    if (!authorizationWhereClauses.isEmpty()) {
  //      authorizationWhereClauses.add("config->'shared' = 'true'");
  //    }
  //
  //    List<String> selectionWhereClauses = generateWhereClausesFor(selectedCondition);
  //    if (!selectedCondition.shared && selectionWhereClauses.isEmpty()) {
  //      selectionWhereClauses.add("config->'shared' != 'true'");
  //    }
  //
  //    if (!authorizationWhereClauses.isEmpty()) {
  //      whereConjunctions.add("(" + StringUtils.join(authorizationWhereClauses, " OR ") + ")");
  //    }
  //    if (!selectionWhereClauses.isEmpty()) {
  //      whereConjunctions.add("(" + StringUtils.join(selectionWhereClauses, " OR ") + ")");
  //    }
  //    if (propsQuery != null) {
  //      propsQuery.forEach(conjunctions -> {
  //        List<String> contentUpdatedAtConjunctions = new ArrayList<>();
  //        conjunctions.forEach(conj -> {
  //            conj.getValues().forEach(v -> {
  //              contentUpdatedAtConjunctions.add("(cast(config->>'contentUpdatedAt' AS TEXT) "+
  // SQLQueryExt.getOperation(conj.getOperation()) + "'" +v + "' )");
  //            });
  //        });
  //        whereConjunctions.add(StringUtils.join(contentUpdatedAtConjunctions, " OR "));
  //      });
  //    }
  //
  //    String query = baseQuery + (whereConjunctions.isEmpty() ? "" :
  //        " WHERE " + StringUtils.join(whereConjunctions, " AND "));
  //
  //    return querySpaces(query);
  //  }
  //
  //  private List<String> generateWhereClausesFor(SpaceAuthorizationCondition condition) {
  //    List<String> whereClauses = new ArrayList<>();
  //    if (condition.spaceIds != null && !condition.spaceIds.isEmpty()) {
  //      whereClauses.add("id IN ('" + StringUtils.join(condition.spaceIds, "','") + "')");
  //    }
  //    if (condition.ownerIds != null && !condition.ownerIds.isEmpty()) {
  //      String negator = "";
  //      if (condition instanceof SpaceSelectionCondition && ((SpaceSelectionCondition) condition).negateOwnerIds) {
  //        negator = "NOT ";
  //      }
  //      whereClauses.add("owner " + negator + "IN ('" + StringUtils.join(condition.ownerIds, "','") + "')");
  //    }
  //    if (condition.packages != null && !condition.packages.isEmpty()) {
  //      whereClauses.add("config->'packages' ??| array['" + StringUtils.join(condition.packages, "','") + "']");
  //    }
  //    return whereClauses;
  //  }
  //
  //
  //  private Future<List<Space>> querySpaces(String query) {
  //    Promise<List<Space>> p = Promise.promise();
  //    client.query(query, out -> {
  //      if (out.succeeded()) {
  //        List<Space> configs = out.result().getRows().stream()
  //            .map(r -> r.getString("config"))
  //            .map(json -> Json.decodeValue(json, Space.class))
  //            .collect(Collectors.toList());
  //        p.complete(configs);
  //      }
  //      else
  //        p.fail(out.cause());
  //    });
  //    return p.future();
  //  }
}
