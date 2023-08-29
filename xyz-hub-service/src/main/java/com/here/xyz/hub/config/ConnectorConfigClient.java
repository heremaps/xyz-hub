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

import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.dynamo.DynamoConnectorConfigClient;
import com.here.xyz.hub.config.jdbc.JDBCConnectorConfigClient;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Embedded;
import com.here.xyz.hub.rest.admin.messages.RelayedMessage;
import com.here.xyz.psql.tools.ECPSTool;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.JacksonCodec;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public abstract class ConnectorConfigClient implements Initializable {

  private static final Logger logger = LogManager.getLogger();

  private static final String ANONYMOUS_OWNER = "ANONYMOUS";

  public static final ExpiringMap<String, Connector> cache = ExpiringMap.builder()
      .expirationPolicy(ExpirationPolicy.CREATED)
      .expiration(1, TimeUnit.MINUTES)
      .build();

  public static ConnectorConfigClient getInstance() {
    if (Service.configuration.CONNECTORS_DYNAMODB_TABLE_ARN != null) {
      return new DynamoConnectorConfigClient(Service.configuration.CONNECTORS_DYNAMODB_TABLE_ARN);
    } else {
      return JDBCConnectorConfigClient.getInstance();
    }
  }

  /**
   * Returns a future which may or may not contain a Connector
   * @param marker the marker for log
   * @param connectorId the connector id
   * @return the future containing the connector or an empty future when the connector is not found.
   */
  public Future<Connector> get(Marker marker, String connectorId) {
    if (StringUtils.isEmpty(connectorId))
      return Future.succeededFuture();

    Promise<Connector> p = Promise.promise();
    get(marker, connectorId, handler -> {
      if (handler.succeeded()) {
        p.complete(handler.result());
      } else {
        p.fail(handler.cause());
      }
    });
    return p.future();
  }

  public void get(Marker marker, String connectorId, Handler<AsyncResult<Connector>> handler) {
    final Connector connectorFromCache = cache.get(connectorId);

    if (connectorFromCache != null) {
      logger.info(marker, "storageId: {} - The connector was loaded from cache", connectorId);
      handler.handle(Future.succeededFuture(connectorFromCache));
      return;
    }

    getConnector(marker, connectorId, ar -> {
      if (ar.succeeded()) {
        final Connector connector = ar.result();
        if (connector.owner != null && connector.owner.equals(ANONYMOUS_OWNER))
          connector.owner = null;
        cache.put(connectorId, connector);
        handler.handle(Future.succeededFuture(connector));
      }
      else {
        logger.warn(marker, "storageId[{}]: Connector not found", connectorId);
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  public void getByOwner(Marker marker, String ownerId, Handler<AsyncResult<List<Connector>>> handler) {
    getConnectorsByOwner(marker, ownerId, ar -> {
      if (ar.succeeded()) {
        final List<Connector> connectors = ar.result();
        connectors.forEach(c -> {
          if (c.owner != null && c.owner.equals(ANONYMOUS_OWNER))
            c.owner = null;
          cache.put(c.id, c);
        });
        handler.handle(Future.succeededFuture(connectors));
      }
      else {
        logger.warn(marker, "storageId[{}]: Connectors for owner not found", ownerId);
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  public void store(Marker marker, Connector connector, Handler<AsyncResult<Connector>> handler) {
    store(marker, connector, handler, true);
  }

  private void store(Marker marker, Connector connector, Handler<AsyncResult<Connector>> handler, boolean withInvalidation) {
    if (connector.id == null) {
      connector.id = RandomStringUtils.randomAlphanumeric(10);
    }
    if (connector.owner == null) {
      connector.owner = ANONYMOUS_OWNER;
    }

    storeConnector(marker, connector, ar -> {
      if (ar.succeeded()) {
        final Connector connectorResult = ar.result();
        if (connectorResult.owner != null && connectorResult.owner.equals(ANONYMOUS_OWNER))
          connectorResult.owner = null;
        if (withInvalidation) {
          invalidateCache(connector.id);
        }
        handler.handle(Future.succeededFuture(connectorResult));
      } else {
        logger.error(marker, "storageId[{}]: Failed to store connector configuration, reason: ", connector.id, ar.cause());
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  public void delete(Marker marker, String connectorId, Handler<AsyncResult<Connector>> handler) {
    deleteConnector(marker, connectorId, ar -> {
      if (ar.succeeded()) {
        final Connector connectorResult = ar.result();
        if (connectorResult.owner != null && connectorResult.owner.equals(ANONYMOUS_OWNER))
          connectorResult.owner = null;
        invalidateCache(connectorId);
        handler.handle(Future.succeededFuture(connectorResult));
      } else {
        logger.error(marker, "storageId[{}]: Failed to delete connector configuration, reason: ", connectorId, ar.cause());
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  public void getAll(Marker marker, Handler<AsyncResult<List<Connector>>> handler) {
    getAllConnectors(marker, ar -> {
      if (ar.succeeded()) {
        final List<Connector> connectors = ar.result();
        connectors.forEach(c -> {
          if (c.owner != null && c.owner.equals(ANONYMOUS_OWNER))
            c.owner = null;
          cache.put(c.id, c);
        });
        handler.handle(Future.succeededFuture(connectors));
      } else {
        logger.error(marker, "Failed to load connectors, reason: ", ar.cause());
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  public CompletableFuture<Void> insertLocalConnectors() {
    if (!Service.configuration.INSERT_LOCAL_CONNECTORS) {
      return CompletableFuture.completedFuture(null);
    }

    final InputStream input = ConnectorConfigClient.class.getResourceAsStream("/connectors.json");
    try (BufferedReader buffer = new BufferedReader(new InputStreamReader(input))) {
      final String connectorsFile = buffer.lines().collect(Collectors.joining("\n"));
      final List<Connector> connectors = JacksonCodec.decodeValue(connectorsFile, new TypeReference<>() {});
      final List<CompletableFuture<Void>> futures = new ArrayList<>();

      connectors.forEach(c -> {
        replaceConnectorVars(c);

        final CompletableFuture<Void> future = new CompletableFuture<>();
        futures.add(future);

        storeConnectorIfNotExists(null, c, r -> {
          if (r.failed()) {
            future.completeExceptionally(r.cause());
          } else {
            future.complete(null);
          }
        });
      });

      return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    } catch (IOException e) {
      logger.error("Unable to insert the local connectors.");
      return CompletableFuture.failedFuture(e);
    }
  }

  private void replaceConnectorVars(final Connector connector) {
    if (connector == null) {
      return;
    }

    replaceVarsInMap(connector.params, ecpsJson -> {
      String ecpsPhrase = Service.configuration.DEFAULT_ECPS_PHRASE;
      if (ecpsPhrase == null) return null;
      //Replace vars in the ECPS JSON
      JsonObject ecpsValues = new JsonObject(ecpsJson);
      Map<String, String> varValues = getPsqlVars();
      replaceVarsInMap(ecpsValues.getMap(), varName -> varValues.get(varName), "${", "}");
      ecpsJson = ecpsValues.toString();
      //Encrypt the ECPS JSON
      try {
        return ECPSTool.encrypt(ecpsPhrase, ecpsJson);
      }
      catch (GeneralSecurityException | UnsupportedEncodingException e) {
        return null;
      }
    }, "$encrypt(", ")");

    if (connector.getRemoteFunction() instanceof Embedded) {
      Map<String, String> varValues = getPsqlVars();
      replaceVarsInMap(((Embedded) connector.getRemoteFunction()).env, varName -> varValues.get(varName), "${", "}");
    }
    else if(connector.getRemoteFunction().id.equalsIgnoreCase("psql-http")){
     try {
       //Replace HOST in http-connector config
        ((Connector.RemoteFunctionConfig.Http)connector.getRemoteFunction()).url =
                new URL(Service.configuration.HTTP_CONNECTOR_ENDPOINT+"/event");
      } catch (MalformedURLException e) {}
    }
  }

  private static Map<String, String> getPsqlVars() {
    Map<String, String> psqlVars = new HashMap<>();

    if (Service.configuration.STORAGE_DB_URL != null) {
      URI uri = URI.create(Service.configuration.STORAGE_DB_URL.substring(5));
      psqlVars.put("PSQL_HOST", uri.getHost());
      psqlVars.put("PSQL_PORT", String.valueOf(uri.getPort() == -1 ? 5432 : uri.getPort()));
      psqlVars.put("PSQL_USER", Service.configuration.STORAGE_DB_USER);
      psqlVars.put("PSQL_PASSWORD", Service.configuration.STORAGE_DB_PASSWORD);
      String[] pathComponent = uri.getPath() == null ? null : uri.getPath().split("/");
      if (pathComponent != null && pathComponent.length > 1)
        psqlVars.put("PSQL_DB", pathComponent[1]);
    }

    return psqlVars;
  }

  private <V> void replaceVarsInMap(Map<String, V> map, Function<String, V> resolve, String prefix, String suffix) {
    if (map == null || map.isEmpty()) return;
    final Map<String, V> replacement = new HashMap<>();

    map.entrySet().stream()
        .filter(e -> e.getValue() instanceof String && ((String) e.getValue()).startsWith(prefix) && ((String) e.getValue()).endsWith(suffix))
        .forEach(e -> {
          final String placeholder = StringUtils.substringBetween((String) e.getValue(), prefix, suffix);
          replacement.put(e.getKey(), resolve.apply(placeholder));
        });

    map.putAll(replacement);
  }

  private void storeConnectorIfNotExists(Marker marker, Connector connector, Handler<AsyncResult<Connector>> handler) {
    get(marker, connector.id, r -> {
      if (r.failed()) {
        logger.info("Connector with ID {} does not exist. Creating it ...", connector.id);
        store(marker, connector, handler, false);
      }
      else {
        //Do nothing, just succeed
        logger.info("Connector with ID " + connector.id + " already exists. Not creating it.");
        handler.handle(Future.succeededFuture(r.result()));
      }
    });
  }


  protected abstract void getConnector(Marker marker, String connectorId, Handler<AsyncResult<Connector>> handler);

  protected abstract void getConnectorsByOwner(Marker marker, String ownerId, Handler<AsyncResult<List<Connector>>> handler);

  protected abstract void storeConnector(Marker marker, Connector connector, Handler<AsyncResult<Connector>> handler);

  protected abstract void deleteConnector(Marker marker, String connectorId, Handler<AsyncResult<Connector>> handler);

  protected abstract void getAllConnectors(Marker marker, Handler<AsyncResult<List<Connector>>> handler);

  public void invalidateCache(String id) {
    new InvalidateConnectorCacheMessage().withId(id).withBroadcastIncludeLocalNode(true).broadcast();
  }

  public static class InvalidateConnectorCacheMessage extends RelayedMessage {

    String id;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public InvalidateConnectorCacheMessage withId(String id) {
      this.id = id;
      return this;
    }

    @Override
    protected void handleAtDestination() {
      cache.remove(id);
    }
  }
}
