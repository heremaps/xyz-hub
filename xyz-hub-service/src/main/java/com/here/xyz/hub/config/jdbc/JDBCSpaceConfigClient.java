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

package com.here.xyz.hub.config.jdbc;

import static com.here.xyz.hub.Service.configuration;
import static com.here.xyz.hub.config.jdbc.JDBCConfigClient.SCHEMA;
import static com.here.xyz.hub.config.jdbc.JDBCConfigClient.configListParser;
import static com.here.xyz.hub.config.jdbc.JDBCConfigClient.configParser;

import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.SpaceConfigClient;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.util.db.SQLQuery;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

/**
 * A client for reading and editing xyz space and connector definitions.
 */
public class JDBCSpaceConfigClient extends SpaceConfigClient {
  private static final Logger logger = LogManager.getLogger();
  private static JDBCSpaceConfigClient instance;
  private static final String SPACE_TABLE = "xyz_space";
  private final JDBCConfigClient client = new JDBCConfigClient(SCHEMA, SPACE_TABLE, Service.configuration);

  public static class Provider extends SpaceConfigClient.Provider {
    @Override
    public boolean chooseMe() {
      return configuration.SPACES_DYNAMODB_TABLE_ARN == null && !"test".equals(System.getProperty("scope"));
    }


    @Override
    protected SpaceConfigClient getInstance() {
      return new JDBCSpaceConfigClient();
    }
  }

  @Override
  public Future<Void> init() {
    return client.init()
        .compose(v -> initTable());
  }

  private Future<Void> initTable() {
    return client.write(client.getQuery("CREATE TABLE IF NOT EXISTS ${schema}.${table} "
            + "(id TEXT primary key, owner TEXT, cid TEXT, config JSONB, region TEXT)"))
        .onFailure(e -> logger.error("Can not create table {}!", SPACE_TABLE, e))
        .mapEmpty();
  }

  @Override
  public Future<Space> getSpace(Marker marker, String spaceId) {
    SQLQuery query = client.getQuery("SELECT config FROM ${schema}.${table} WHERE id = #{spaceId}")
        .withNamedParameter("spaceId", spaceId);

    return client.run(query, configParser(Space.class));
  }

  @Override
  protected Future<Void> storeSpace(Marker marker, Space space) {
    final Map<String, Object> itemData = XyzSerializable.toMap(space, Static.class);
    SQLQuery query = client.getQuery(
        "INSERT INTO ${schema}.${table} (id, owner, cid, config, region) VALUES (#{spaceId}, #{owner}, #{cid}, cast(#{spaceJson} as JSONB), #{region}) ON CONFLICT (id) DO UPDATE SET owner = excluded.owner, cid = excluded.cid, config = excluded.config, region = excluded.region")
        .withNamedParameter("spaceId", space.getId())
        .withNamedParameter("owner", space.getOwner())
        .withNamedParameter("cid", space.getCid())
        .withNamedParameter("spaceJson", XyzSerializable.serialize(itemData, Static.class))
        .withNamedParameter("region", space.getRegion());
    return client.write(query).mapEmpty();
  }

  @Override
  protected Future<Space> deleteSpace(Marker marker, String spaceId) {
    SQLQuery query = client.getQuery("DELETE FROM ${schema}.${table} WHERE id = #{spaceId}")
        .withNamedParameter("spaceId", spaceId);
    return get(marker, spaceId).compose(space -> client.write(query).map(space));
  }

  @Override
  protected Future<List<Space>> getSelectedSpaces(Marker marker, SpaceAuthorizationCondition authorizedCondition,
      SpaceSelectionCondition selectedCondition, PropertiesQuery propsQuery) {
    //TODO: Use SQLQuery with named parameters
    //BUILD THE QUERY
    List<String> whereConjunctions = new ArrayList<>();
    String baseQuery = "SELECT config FROM ${schema}.${table}";
    List<String> authorizationWhereClauses = generateWhereClausesFor(authorizedCondition);
    if (!authorizationWhereClauses.isEmpty())
      authorizationWhereClauses.add("config->'shared' = 'true'");

    List<String> selectionWhereClauses = generateWhereClausesFor(selectedCondition);
    if (!selectedCondition.shared && selectionWhereClauses.isEmpty())
      selectionWhereClauses.add("config->'shared' != 'true'");

    if (!authorizationWhereClauses.isEmpty())
      whereConjunctions.add("(" + String.join(" OR ", authorizationWhereClauses) + ")");
    if (!selectionWhereClauses.isEmpty())
      whereConjunctions.add("(" + String.join(" OR ", selectionWhereClauses) + ")");

    if (propsQuery != null) {
      propsQuery.forEach(conjunctions -> {
        List<String> contentUpdatedAtConjunctions = new ArrayList<>();
        conjunctions.forEach(conj -> {
            conj.getValues().forEach(v -> {
              contentUpdatedAtConjunctions.add("(cast(config->>'contentUpdatedAt' AS TEXT) "+ QueryOperation.getOutputRepresentation(conj.getOperation()) + "'" +v + "' )");
            });
        });
        whereConjunctions.add(String.join(" OR ", contentUpdatedAtConjunctions));
      });
    }

    if (selectedCondition.region != null)
      whereConjunctions.add("region = '" + selectedCondition.region + "'");

    if (selectedCondition.prefix != null)
      whereConjunctions.add("id like '" + selectedCondition.prefix + "%'");

    String query = baseQuery + (whereConjunctions.isEmpty() ? "" :
        " WHERE " + String.join(" AND ", whereConjunctions));

    return client.run(client.getQuery(query), configListParser(Space.class));
  }

  @Override
  public Future<List<Space>> getSpacesFromSuper(Marker marker, String superSpaceId) {
    return client.run(client.getQuery("SELECT config FROM ${schema}.${table} WHERE config->'extends'->>'spaceId' = #{superSpaceId}").withNamedParameter("superSpaceId", superSpaceId), configListParser(Space.class));
  }

  private List<String> generateWhereClausesFor(SpaceAuthorizationCondition condition) {
    List<String> whereClauses = new ArrayList<>();
    if (condition.spaceIds != null && !condition.spaceIds.isEmpty())
      whereClauses.add("id IN ('" + String.join("','", condition.spaceIds) + "')");

    if (condition.ownerIds != null && !condition.ownerIds.isEmpty()) {
      String negator = "";
      if (condition instanceof SpaceSelectionCondition && ((SpaceSelectionCondition) condition).negateOwnerIds)
        negator = "NOT ";
      whereClauses.add("owner " + negator + "IN ('" + String.join("','", condition.ownerIds) + "')");
    }
    if (condition.packages != null && !condition.packages.isEmpty())
      whereClauses.add("config->'packages' ??| array['" + String.join("','", condition.packages) + "']");
    return whereClauses;
  }
}
