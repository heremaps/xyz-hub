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

import com.here.xyz.hub.Service;
import com.here.xyz.psql.SQLQuery;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import java.util.Arrays;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JDBCConfig {

  private static final Logger logger = LogManager.getLogger();
  public static final String SCHEMA = "xyz_config";
  static final String CONNECTOR_TABLE = SCHEMA + ".xyz_storage";
  static final String SPACE_TABLE = SCHEMA + ".xyz_space";
  static final String SUBSCRIPTION_TABLE = SCHEMA + ".xyz_subscription";
  static final String TAG_TABLE = "xyz_tags";
  private static SQLClient client;
  private static boolean initialized = false;
  private static final String CHECK_SCHEMA_EXISTS_QUERY = "SELECT schema_name FROM information_schema.schemata WHERE schema_name='xyz_config'";
  private static final List<String> batchQueries = Arrays.asList(
      "CREATE SCHEMA " + SCHEMA,
      "CREATE TABLE  " + CONNECTOR_TABLE + " (id VARCHAR(50) primary key, owner VARCHAR (50), config JSONB)",
      "CREATE TABLE  " + SPACE_TABLE + " (id VARCHAR(255) primary key, owner VARCHAR (50), cid VARCHAR (255), config JSONB, region VARCHAR (50))",
      "CREATE TABLE  " + SUBSCRIPTION_TABLE + " (id VARCHAR(255) primary key, source VARCHAR (255), config JSONB)",
      "CREATE TABLE  " + SCHEMA + "." +TAG_TABLE + " (id VARCHAR(255), space VARCHAR (255), version BIGINT, PRIMARY KEY(id, space))"
  );

  private static class SchemaAlreadyExistsException extends Exception {}

  public static SQLClient getClient() {
    if (client != null) {
      return client;
    }

    synchronized (CONNECTOR_TABLE) {
      if (client == null) {
        String db_url = Service.configuration.STORAGE_DB_URL;
        db_url += (db_url.contains("?") ? "&" : "?") + "ApplicationName=XYZ-Hub";
        JsonObject config = new JsonObject()
            .put("url", db_url)
            .put("user", Service.configuration.STORAGE_DB_USER)
            .put("password", Service.configuration.STORAGE_DB_PASSWORD)
            .put("min_pool_size", 1)
            .put("max_pool_size", 4)
            .put("acquire_retry_attempts", 1);
        client = io.vertx.ext.jdbc.JDBCClient.createShared(Service.vertx, config);
      }
      return client;
    }
  }

  private static Future<SQLConnection> getConnection() {
    Promise<SQLConnection> promise = Promise.promise();
    client.getConnection(res -> {
      if (res.failed()) {
        logger.error("Initializing of the config table failed.", res.cause());
        promise.fail(res.cause());
      } else {
        promise.complete(res.result());
      }
    });

    return promise.future();
  }

  private static Future<SQLConnection> checkSchemaExists(SQLConnection connection) {
    Promise<SQLConnection> promise = Promise.promise();
    connection.query(CHECK_SCHEMA_EXISTS_QUERY, out -> {
      if (out.failed()) {
        connection.close();
        promise.fail(out.cause());
      } else if (out.result().getNumRows() > 0) {
        connection.close();
        promise.fail(new SchemaAlreadyExistsException());
      } else {
        promise.complete(connection);
      }
    });
    return promise.future();
  }

  private static Future<SQLConnection> setAutoCommit(SQLConnection connection, boolean autoCommit) {
    Promise<Void> promise = Promise.promise();
    connection.setAutoCommit(autoCommit, promise);
    return promise.future().map(connection);
  }

  private static Future<SQLConnection> disableAutoCommit(SQLConnection connection) {
    return setAutoCommit(connection, false);
  }

  private static Future<SQLConnection> enableAutoCommit(SQLConnection connection) {
    return setAutoCommit(connection, true);
  }

  private static Future<SQLConnection> executeBachQueries(SQLConnection connection) {
    Promise<List<Integer>> promise = Promise.promise();
    connection.batch(batchQueries, promise);
    return promise.future().map(connection);
  }

  public static synchronized Future<Void> init() {
    if (initialized) {
      return Future.succeededFuture();
    }

    initialized = true;

    return getConnection()
        .flatMap(JDBCConfig::checkSchemaExists)
        .flatMap(JDBCConfig::disableAutoCommit)
        .flatMap(JDBCConfig::executeBachQueries)
        .flatMap(JDBCConfig::enableAutoCommit)
        .<Void>mapEmpty()
        .recover(t -> t instanceof SchemaAlreadyExistsException ? Future.succeededFuture() : Future.failedFuture(t))
        .onSuccess(connection -> logger.info("Initializing of the config table was successful."))
        .onFailure(t -> logger.error("Initializing of the config table failed.", t));
  }

  protected static Future<Void> updateWithParams(SQLQuery query) {
    query.substitute();
    Promise<Void> p = Promise.promise();
    client.updateWithParams(query.text(), new JsonArray(query.parameters()), out -> {
      if (out.succeeded()) {
        p.complete();
      } else {
        p.fail(out.cause());
      }
    });
    return p.future();
  }
}
