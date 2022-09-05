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

import com.here.xyz.models.hub.Subscription;
import com.here.xyz.psql.SQLQuery;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.here.xyz.hub.config.JDBCConfig.SUBSCRIPTION_TABLE;

/**
 * A client for reading and editing subscription definitions.
 */
public class JDBCSubscriptionConfigClient extends SubscriptionConfigClient {

  private static final Logger logger = LogManager.getLogger();

  private static JDBCSubscriptionConfigClient instance;
  private final SQLClient client;

  private JDBCSubscriptionConfigClient() {
    this.client = JDBCConfig.getClient();
  }

  public static JDBCSubscriptionConfigClient getInstance() {
    if (instance == null) {
      instance = new JDBCSubscriptionConfigClient();
    }
    return instance;
  }

  @Override
  public void init(Handler<AsyncResult<Void>> onReady) {
    JDBCConfig.init(onReady);
  }

  @Override
  protected void getSubscription(final Marker marker, final String subscriptionId, final Handler<AsyncResult<Subscription>> handler) {
    final SQLQuery query = new SQLQuery("SELECT config FROM " + SUBSCRIPTION_TABLE + " WHERE id = ?", subscriptionId);
    client.queryWithParams(query.text(), new JsonArray(query.parameters()), out -> {
      if (out.succeeded()) {
        final Optional<String> config = out.result().getRows().stream().map(r -> r.getString("config")).findFirst();
        if (config.isPresent()) {
          final Subscription subscription = Json.decodeValue(config.get(), Subscription.class);
          logger.debug(marker, "subscriptionId[{}]: Loaded subscription from the database.", subscriptionId);
          handler.handle(Future.succeededFuture(subscription));
        } else {
          logger.debug(marker, "subscriptionId[{}]: This configuration does not exist", subscriptionId);
          handler.handle(Future.failedFuture("The subscription config not found for subscriptionId: " + subscriptionId));
        }
      } else {
        logger.debug(marker, "subscriptionId[{}]: Failed to load configuration, reason: ", subscriptionId, out.cause());
        handler.handle(Future.failedFuture(out.cause()));
      }
    });
  }

  @Override
  protected void getSubscriptionsBySource(Marker marker, String source, Handler<AsyncResult<List<Subscription>>> handler) {
    final SQLQuery query = new SQLQuery("SELECT config FROM " + SUBSCRIPTION_TABLE + " WHERE source = ?", source);
    client.queryWithParams(query.text(), new JsonArray(query.parameters()), out -> {
      if (out.succeeded()) {
        final Stream<String> config = out.result().getRows().stream().map(r -> r.getString("config"));
        List<Subscription> result = new ArrayList<>();
        config.forEach(s -> {
          if (s != null) {
            final Subscription subscription = Json.decodeValue(s, Subscription.class);
            result.add(subscription);
            logger.debug(marker, "ownerId[{}]: Loaded subscriptions from the database.", source);
          }
        });
        handler.handle(Future.succeededFuture(result));

      } else {
        logger.debug(marker, "source[{}]: Failed to load configurations, reason: ", source, out.cause());
        handler.handle(Future.failedFuture(out.cause()));
      }
    });
  }

  @Override
  protected void getAllSubscriptions(Marker marker, Handler<AsyncResult<List<Subscription>>> handler) {
    client.query("SELECT config FROM " + SUBSCRIPTION_TABLE, out -> {
      if (out.succeeded()) {
        List<Subscription> configs = out.result().getRows().stream()
                .map(r -> r.getString("config"))
                .map(json -> Json.decodeValue(json, Subscription.class))
                .collect(Collectors.toList());
        handler.handle(Future.succeededFuture(configs));

      } else {
        handler.handle(Future.failedFuture(out.cause()));
      }
    });
  }

  @Override
  protected void storeSubscription(Marker marker, Subscription subscription, Handler<AsyncResult<Subscription>> handler) {
    final SQLQuery query = new SQLQuery("INSERT INTO " + SUBSCRIPTION_TABLE + " (id, source, config) VALUES (?, ?, cast(? as JSONB)) " +
        "ON CONFLICT (id) DO " +
        "UPDATE SET id = ?, source = ?, config = cast(? as JSONB)",
        subscription.getId(), subscription.getSource(), Json.encode(subscription),
        subscription.getId(), subscription.getSource(), Json.encode(subscription));
    updateWithParams(subscription, query, handler);
  }

  @Override
  protected void deleteSubscription(Marker marker, String subscriptionId, Handler<AsyncResult<Subscription>> handler) {
    final SQLQuery query = new SQLQuery("DELETE FROM " + SUBSCRIPTION_TABLE + " WHERE id = ?", subscriptionId);
    get(marker, subscriptionId, ar -> {
      if (ar.succeeded()) {
        updateWithParams(ar.result(), query, handler);
      } else {
        logger.error(ar.cause());
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  private void updateWithParams(Subscription modifiedObject, SQLQuery query, Handler<AsyncResult<Subscription>> handler) {
    client.updateWithParams(query.text(), new JsonArray(query.parameters()), out -> {
      if (out.succeeded()) {
        handler.handle(Future.succeededFuture(modifiedObject));
      } else {
        handler.handle(Future.failedFuture(out.cause()));
      }
    });
  }
}
