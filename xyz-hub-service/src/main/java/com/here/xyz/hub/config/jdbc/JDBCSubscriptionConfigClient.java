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

import static com.here.xyz.hub.config.jdbc.JDBCConfigClient.SCHEMA;
import static com.here.xyz.hub.config.jdbc.JDBCConfigClient.configListParser;
import static com.here.xyz.hub.config.jdbc.JDBCConfigClient.configParser;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.SubscriptionConfigClient;
import com.here.xyz.models.hub.Subscription;
import com.here.xyz.util.db.SQLQuery;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

/**
 * A client for reading and editing subscription definitions.
 */
public class JDBCSubscriptionConfigClient extends SubscriptionConfigClient {
  private static final Logger logger = LogManager.getLogger();
  private static JDBCSubscriptionConfigClient instance;
  private static final String SUBSCRIPTION_TABLE = "xyz_subscription";
  private final JDBCConfigClient client = new JDBCConfigClient(SCHEMA, SUBSCRIPTION_TABLE, Service.configuration);

  public static JDBCSubscriptionConfigClient getInstance() {
    if (instance == null)
      instance = new JDBCSubscriptionConfigClient();
    return instance;
  }

  @Override
  protected Future<Subscription> getSubscription(final Marker marker, final String subscriptionId) {
    final SQLQuery query = client.getQuery("SELECT config FROM ${schema}.${table} WHERE id = #{subscriptionId}")
        .withNamedParameter("subscriptionId", subscriptionId);

    return client.run(query, configParser(Subscription.class))
        .onSuccess(subscription -> {
          if (subscription != null)
            logger.debug(marker, "Loaded subscription {} from the database.", subscriptionId);
          else
            logger.debug(marker, "The configuration {} does not exist", subscriptionId);
        })
        .onFailure(t -> logger.debug(marker, "Failed to load subscription {}", subscriptionId, t));
  }

  @Override
  protected Future<List<Subscription>> getSubscriptionsBySource(Marker marker, String source) {
    final SQLQuery query = client.getQuery("SELECT config FROM ${schema}.${table} WHERE source = #{source}")
        .withNamedParameter("source", source);

    return client.run(query, configListParser(Subscription.class))
        .onSuccess(subscriptions -> logger.debug(marker, "Loaded subscriptions for source {} from the database.", source))
        .onFailure(t -> logger.debug(marker, "Failed to load subscriptions for source {}", source, t));
  }

  @Override
  protected Future<List<Subscription>> getAllSubscriptions(Marker marker) {
    return client.run(client.getQuery("SELECT config FROM ${schema}.${table}"), configListParser(Subscription.class));
  }

  @Override
  protected Future<Void> storeSubscription(Marker marker, Subscription subscription) {
    final SQLQuery query = client.getQuery("INSERT INTO ${schema}.${table} (id, source, config) VALUES (#{subscriptionId}, #{source}, cast(#{subscriptionJson} as JSONB)) " +
        "ON CONFLICT (id) DO " +
        "UPDATE SET id = #{subscriptionId}, source = #{source}, config = cast(#{subscriptionJson} as JSONB)")
        .withNamedParameter("subscriptionId", subscription.getId())
        .withNamedParameter("source", subscription.getSource())
        .withNamedParameter("subscriptionJson", Json.encode(subscription)); //TODO: Use XyzSerializable with static view
    return client.write(query).mapEmpty();
  }

  @Override
  protected Future<Subscription> deleteSubscription(Marker marker, String subscriptionId) {
    final SQLQuery query = client.getQuery("DELETE FROM ${schema}.${table} WHERE id = #{subscriptionId}")
        .withNamedParameter("subscriptionId", subscriptionId);
    return get(marker, subscriptionId).compose(subscription -> client.write(query).map(subscription));
  }

  @Override
  public Future<Void> init() {
    return client.init()
        .compose(v -> initTable());
  }

  private Future<Void> initTable() {
    return client.write(client.getQuery("CREATE TABLE IF NOT EXISTS ${schema}.${table} "
            + "(id TEXT primary key, source TEXT, config JSONB)"))
        .onFailure(e -> logger.error("Can not create table {}!", SUBSCRIPTION_TABLE, e))
        .mapEmpty();
  }
}
