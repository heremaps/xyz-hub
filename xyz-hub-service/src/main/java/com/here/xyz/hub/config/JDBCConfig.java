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

import com.here.xyz.hub.Service;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import java.util.Arrays;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JDBCConfig {

  private static final Logger logger = LogManager.getLogger();

  private static final String SCHEMA = "xyz_config";
  static final String CONNECTOR_TABLE = SCHEMA + ".xyz_storage";
  static final String SPACE_TABLE = SCHEMA + ".xyz_space";
  private static SQLClient client;
  private static boolean initialized = false;

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

  public static synchronized void init(Handler<AsyncResult<Void>> onReady) {
    if (initialized) {
      onReady.handle(Future.succeededFuture());
      return;
    }

    initialized = true;

    client.getConnection(res -> {
      if (res.failed()) {
        logger.error("Initializing of the config table failed.", res.cause());
        onReady.handle(Future.failedFuture(res.cause()));
        return;
      }
      SQLConnection connection = res.result();

      String query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name='xyz_config'";
      connection.query(query, out -> {
        if (out.succeeded() && out.result().getNumRows() > 0) {
          logger.info("schema already created");
          onReady.handle(Future.succeededFuture());
          connection.close();
          return;
        }
        List<String> batchQueries = Arrays.asList(
            String.format("CREATE SCHEMA %s", SCHEMA),
            String.format("CREATE table  %s (id VARCHAR(50) primary key, owner VARCHAR (50), config JSONB)", CONNECTOR_TABLE),
            String.format("CREATE table  %s (id VARCHAR(255) primary key, owner VARCHAR (50), cid VARCHAR (255), config JSONB)", SPACE_TABLE)
        );

        Promise<Void> onComplete = Promise.promise();
        Promise<Void> step1Completer = Promise.promise();

        //Step 1
        Runnable step1 = () -> connection.setAutoCommit(false, step1Completer);

        //Step 2
        step1Completer.future().compose(r -> {
          Promise<List<Integer>> f = Promise.promise();
          connection.batch(batchQueries, f);
          return f.future();
        }).compose(r -> {
          connection.setAutoCommit(true, onComplete);
          return onComplete.future();
        });

        //Step 3
        onComplete.future().onComplete(ar -> {
          if (ar.failed()) {
            logger.error("Initializing of the config table failed.", ar.cause());
          } else {
            logger.info("Initializing of the config table was successful.");
          }
          onReady.handle(ar);
          connection.close();
        });

        step1.run();
      });
    });
  }
}
