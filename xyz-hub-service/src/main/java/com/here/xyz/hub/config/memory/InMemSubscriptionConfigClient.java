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

package com.here.xyz.hub.config.memory;

import com.here.xyz.hub.config.SubscriptionConfigClient;
import com.here.xyz.models.hub.Subscription;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.Marker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class InMemSubscriptionConfigClient extends SubscriptionConfigClient {

  private Map<String, Subscription> storageMap = new ConcurrentHashMap<>();

  @Override
  public void init(Handler<AsyncResult<Void>> onReady) {
    onReady.handle(Future.succeededFuture());
  }

  @Override
  protected Future<Subscription> getSubscription(Marker marker, String subscriptionId) {
    Subscription subscription = storageMap.get(subscriptionId);
    return Future.succeededFuture(subscription);
  }

  @Override
  protected Future<List<Subscription>> getSubscriptionsBySource(Marker marker, String source) {
    List<Subscription> subscriptions = storageMap.values().stream().filter(s -> s.getSource().equals(source)).collect(Collectors.toList());
    return Future.succeededFuture(subscriptions);
  }

  @Override
  protected Future<List<Subscription>> getAllSubscriptions(Marker marker) {
    List<Subscription> subscriptions = new ArrayList<>(storageMap.values());
    return Future.succeededFuture(subscriptions);
  }

  @Override
  protected Future<Void> storeSubscription(Marker marker, Subscription subscription) {
    if (subscription.getId() == null) {
      subscription.setId(RandomStringUtils.randomAlphanumeric(10));
    }
    storageMap.put(subscription.getId(), subscription);
    return Future.succeededFuture();
  }

  @Override
  protected Future<Subscription> deleteSubscription(Marker marker, String subscriptionId) {
    Subscription subscription = storageMap.remove(subscriptionId);
    return Future.succeededFuture(subscription);
  }
}
