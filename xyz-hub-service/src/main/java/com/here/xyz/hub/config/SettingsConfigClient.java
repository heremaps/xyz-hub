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

import static com.here.xyz.hub.rest.Api.HeaderValues.STREAM_ID;

import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.dynamo.DynamoSettingsConfigClient;
import com.here.xyz.hub.config.settings.Setting;
import com.here.xyz.hub.connectors.models.Connector;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.jackson.JacksonCodec;
import java.awt.Composite;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager.Log4jMarker;

public abstract class SettingsConfigClient implements Initializable {

  protected static final Logger logger = LogManager.getLogger();

  private static final ExpiringMap<String, Setting> cache = ExpiringMap.builder()
      .expirationPolicy(ExpirationPolicy.CREATED)
      .expiration(3, TimeUnit.MINUTES)
      .build();

  public static SettingsConfigClient getInstance() {
    if (Service.configuration.SETTINGS_DYNAMODB_TABLE_ARN != null)
      return new DynamoSettingsConfigClient(Service.configuration.SETTINGS_DYNAMODB_TABLE_ARN);
    else
      //No overrides are needed when running locally
      return new SettingsConfigClient() {
        @Override
        protected Future<Setting> getSetting(Marker marker, String settingId) {
          return Future.succeededFuture();
        }

        @Override
        protected Future<Setting> storeSetting(Marker marker, Setting setting) {
          return Future.succeededFuture(null);
        }
      };
  }

  public Future<Setting> get(Marker marker, String settingId) {
    Setting cached = cache.get(settingId);
    if (cached != null) {
      logger.info(marker, "Loaded setting with id \"{}\" from cache", settingId);
      return Future.succeededFuture(cached);
    }
    return getSetting(marker, settingId);
  }

  public Future<Setting> store(Marker marker, Setting setting) {
    if (setting == null)
      return Future.failedFuture("setting is null");

    if (setting.id == null)
      return Future.failedFuture("setting.id is null");

    return storeSetting(marker, setting);
  }

  protected abstract Future<Setting> getSetting(Marker marker, String settingId);

  protected abstract Future<Setting> storeSetting(Marker marker, Setting setting);

  public Future<Void> insertLocalSettings() {
    if (!Service.configuration.INSERT_LOCAL_SETTINGS)
      return Future.succeededFuture();

    final InputStream input = SettingsConfigClient.class.getResourceAsStream("/settings.json");
    if (input == null)
      return Future.succeededFuture();

    Marker marker = new Log4jMarker("initservice");

    try (BufferedReader buffer = new BufferedReader(new InputStreamReader(input))) {
      final String settingsFile = buffer.lines().collect(Collectors.joining("\n"));
      final List<Setting> settings = JacksonCodec.decodeValue(settingsFile, new TypeReference<>() {});
      final List<Future> futures = settings.stream().<Future>map(s -> storeSettingIfNotExists(marker, s)).toList();
      return CompositeFuture.all(futures).onFailure(e -> logger.warn(marker, "Unable to insert local settings", e)).mapEmpty();
    } catch (IOException e) {
      logger.error("Unable to insert the local settings");
      return Future.failedFuture(e);
    }
  }

  private Future<Setting> storeSettingIfNotExists(Marker marker, Setting setting) {
    return get(marker, setting.id)
        .compose(s -> s != null ? Future.succeededFuture(s) : store(marker, setting))
        .onFailure(e -> logger.info(marker, "Unable to store check if exists or store setting {}", setting, e));
  }
}
