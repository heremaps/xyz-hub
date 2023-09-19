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

import static com.here.xyz.hub.config.jdbc.JDBCConfig.SUBSCRIPTION_TABLE;

import com.here.xyz.hub.config.SubscriptionConfigClient;
import com.here.xyz.models.hub.Subscription;
import com.here.xyz.psql.SQLQuery;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

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
  public Future<Void> init() {
    return JDBCConfig.init();
  }

  @Override
  protected Future<Subscription> getSubscription(final Marker marker, final String subscriptionId) {
    Promise<Subscription> p = Promise.promise();
    final SQLQuery query = new SQLQuery("SELECT config FROM " + SUBSCRIPTION_TABLE + " WHERE id = #{subscriptionId}")
        .withNamedParameter("subscriptionId", subscriptionId);
    client.queryWithParams(query.substitute().text(), new JsonArray(query.parameters()), out -> {
      if (out.succeeded()) {
        final Optional<String> config = out.result().getRows().stream().map(r -> r.getString("config")).findFirst();
        if (config.isPresent()) {
          final Subscription subscription = Json.decodeValue(config.get(), Subscription.class);
          logger.debug(marker, "subscriptionId[{}]: Loaded subscription from the database.", subscriptionId);
          p.complete(subscription);
        } else {
          logger.debug(marker, "subscriptionId[{}]: This configuration does not exist", subscriptionId);
          p.complete();
        }
      } else {
        logger.debug(marker, "subscriptionId[{}]: Failed to load configuration, reason: ", subscriptionId, out.cause());
        p.fail(out.cause());
      }
    });
    return p.future();
  }

  @Override
  protected Future<List<Subscription>> getSubscriptionsBySource(Marker marker, String source) {
    Promise<List<Subscription>> p = Promise.promise();
    final SQLQuery query = new SQLQuery("SELECT config FROM " + SUBSCRIPTION_TABLE + " WHERE source = #{source}")
        .withNamedParameter("source", source);
    client.queryWithParams(query.substitute().text(), new JsonArray(query.parameters()), out -> {
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
        p.complete(result);

      } else {
        logger.debug(marker, "source[{}]: Failed to load configurations, reason: ", source, out.cause());
        p.fail(out.cause());
      }
    });
    return p.future();
  }

  @Override
  protected Future<List<Subscription>> getAllSubscriptions(Marker marker) {
    Promise<List<Subscription>> p = Promise.promise();
    client.query("SELECT config FROM " + SUBSCRIPTION_TABLE, out -> {
      if (out.succeeded()) {
        List<Subscription> configs = out.result().getRows().stream()
                .map(r -> r.getString("config"))
                .map(json -> Json.decodeValue(json, Subscription.class))
                .collect(Collectors.toList());
        p.complete(configs);

      } else {
        p.fail(out.cause());
      }
    });
    return p.future();
  }

  @Override
  protected Future<Void> storeSubscription(Marker marker, Subscription subscription) {
    final SQLQuery query = new SQLQuery("INSERT INTO " + SUBSCRIPTION_TABLE + " (id, source, config) VALUES (#{subscriptionId}, #{source}, cast(#{subscriptionJson} as JSONB)) " +
        "ON CONFLICT (id) DO " +
        "UPDATE SET id = #{subscriptionId}, source = #{source}, config = cast(#{subscriptionJson} as JSONB)")
        .withNamedParameter("subscriptionId", subscription.getId())
        .withNamedParameter("source", subscription.getSource())
        .withNamedParameter("subscriptionJson", Json.encode(subscription));
    return JDBCConfig.updateWithParams(query);
  }

  @Override
  protected Future<Subscription> deleteSubscription(Marker marker, String subscriptionId) {
    final SQLQuery query = new SQLQuery("DELETE FROM " + SUBSCRIPTION_TABLE + " WHERE id = #{subscriptionId}")
        .withNamedParameter("subscriptionId", subscriptionId);
    return get(marker, subscriptionId).compose(subscription -> JDBCConfig.updateWithParams(query).map(subscription));
  }
}
