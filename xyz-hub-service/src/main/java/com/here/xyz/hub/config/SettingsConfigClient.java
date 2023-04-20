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

import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.dynamo.DynamoSettingsConfigClient;
import com.here.xyz.hub.config.settings.Setting;
import io.vertx.core.Future;
import java.util.concurrent.TimeUnit;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

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

  protected abstract Future<Setting> getSetting(Marker marker, String settingId);

}
