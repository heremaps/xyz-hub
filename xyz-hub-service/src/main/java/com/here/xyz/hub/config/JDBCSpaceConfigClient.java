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

import static com.here.xyz.hub.config.JDBCConfig.SPACE_TABLE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.models.hub.Space.WithConnectors;
import com.here.xyz.psql.SQLQuery;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.EncodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Marker;

/**
 * A client for reading and editing xyz space and connector definitions.
 */
public class JDBCSpaceConfigClient extends SpaceConfigClient {

  private static JDBCSpaceConfigClient instance;
  private final SQLClient client;

  private JDBCSpaceConfigClient() {
    this.client = JDBCConfig.getClient();
  }

  public static JDBCSpaceConfigClient getInstance() {
    if (instance == null) {
      instance = new JDBCSpaceConfigClient();
    }
    return instance;
  }

  @Override
  public void init(Handler<AsyncResult<Void>> onReady) {
    JDBCConfig.init(onReady);
  }


  private void updateWithParams(Space modifiedObject, SQLQuery query, Handler<AsyncResult<Space>> handler) {
    client.updateWithParams(query.text(), new JsonArray(query.parameters()), out -> {
      if (out.succeeded()) {
        handler.handle(Future.succeededFuture(modifiedObject));
      } else {
        handler.handle(Future.failedFuture(out.cause()));
      }
    });
  }

  @Override
  public void getSpace(Marker marker, String spaceId, Handler<AsyncResult<Space>> handler) {
    SQLQuery query = new SQLQuery(String.format("SELECT config FROM %s WHERE id = ?", SPACE_TABLE), spaceId);
    client.queryWithParams(query.text(), new JsonArray(query.parameters()), out -> {
      if (out.succeeded()) {
        Optional<String> config = out.result().getRows().stream().map(r -> r.getString("config")).findFirst();
        if (config.isPresent()) {
          Space space = Json.decodeValue(config.get(), Space.class);
          handler.handle(Future.succeededFuture(space));
        } else {
          handler.handle(Future.succeededFuture(null));
        }
      } else {
        handler.handle(Future.failedFuture(out.cause()));
      }
    });
  }

  @Override
  protected void storeSpace(Marker marker, Space space, Handler<AsyncResult<Space>> handler) {
    SQLQuery query = null;
    try {
      query = new SQLQuery(String.format(
          "INSERT INTO %s(id, owner, cid, config) VALUES (?, ?, ?, cast(? as JSONB)) ON CONFLICT (id) DO UPDATE SET owner = excluded.owner, cid = excluded.cid, config = excluded.config",
          SPACE_TABLE), space.getId(), space.getOwner(), space.getCid(),
          defaultMapper().writerWithView(WithConnectors.class).writeValueAsString(space));
    } catch (JsonProcessingException e) {
      handler.handle(Future.failedFuture(new EncodeException("Failed to encode as JSON: " + e.getMessage(), e)));
    }

    updateWithParams(space, query, handler);
  }

  @Override
  protected void deleteSpace(Marker marker, String spaceId, Handler<AsyncResult<Space>> handler) {
    SQLQuery query = new SQLQuery(String.format("DELETE FROM %s WHERE id = ?", SPACE_TABLE), spaceId);
    get(marker, spaceId, ar -> {
      if (ar.succeeded()) {
        Space space = ar.result();
        if (space == null) {
          handler.handle(Future.failedFuture(new Exception("The space doesn't exist.")));
        }
        updateWithParams(space, query, handler);
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  @Override
  protected void getSelectedSpaces(Marker marker, SpaceAuthorizationCondition authorizedCondition,
      SpaceSelectionCondition selectedCondition,
      Handler<AsyncResult<List<Space>>> handler) {
    //BUILD THE QUERY
    List<String> whereConjunctions = new ArrayList<>();
    String baseQuery = String.format("SELECT config FROM %s", SPACE_TABLE);
    List<String> authorizationWhereClauses = generateWhereClausesFor(authorizedCondition);
    if (!authorizationWhereClauses.isEmpty()) {
      authorizationWhereClauses.add("config->'shared' = 'true'");
    }

    List<String> selectionWhereClauses = generateWhereClausesFor(selectedCondition);
    if (!selectedCondition.shared && selectionWhereClauses.isEmpty()) {
      selectionWhereClauses.add("config->'shared' != 'true'");
    }

    if (!authorizationWhereClauses.isEmpty()) {
      whereConjunctions.add("(" + StringUtils.join(authorizationWhereClauses, " OR ") + ")");
    }
    if (!selectionWhereClauses.isEmpty()) {
      whereConjunctions.add("(" + StringUtils.join(selectionWhereClauses, " OR ") + ")");
    }

    String query = baseQuery + (whereConjunctions.isEmpty() ? "" :
        " WHERE " + StringUtils.join(whereConjunctions, " AND "));

    querySpaces(handler, query);
  }

  private List<String> generateWhereClausesFor(SpaceAuthorizationCondition condition) {
    List<String> whereClauses = new ArrayList<>();
    if (condition.spaceIds != null && !condition.spaceIds.isEmpty()) {
      whereClauses.add("id IN ('" + StringUtils.join(condition.spaceIds, "','") + "')");
    }
    if (condition.ownerIds != null && !condition.ownerIds.isEmpty()) {
      String negator = "";
      if (condition instanceof SpaceSelectionCondition && ((SpaceSelectionCondition) condition).negateOwnerIds) {
        negator = "NOT ";
      }
      whereClauses.add("owner " + negator + "IN ('" + StringUtils.join(condition.ownerIds, "','") + "')");
    }
    if (condition.packages != null && !condition.packages.isEmpty()) {
      whereClauses.add("config->'packages' ??| array['" + StringUtils.join(condition.packages, "','") + "']");
    }
    return whereClauses;
  }


  private void querySpaces(Handler<AsyncResult<List<Space>>> handler, String query) {
    client.query(query, out -> {
      if (out.succeeded()) {
        List<Space> configs = out.result().getRows().stream()
            .map(r -> r.getString("config"))
            .map(json -> Json.decodeValue(json, Space.class))
            .collect(Collectors.toList());
        handler.handle(Future.succeededFuture(configs));
      } else {
        handler.handle(Future.failedFuture(out.cause()));
      }
    });
  }
}
