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

import com.here.xyz.hub.config.SettingsConfigClient;
import com.here.xyz.hub.config.SubscriptionConfigClient;
import com.here.xyz.hub.config.settings.Setting;
import com.here.xyz.models.hub.Subscription;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.Marker;

@SuppressWarnings("unused")
public class InMemSettingsConfigClient extends SettingsConfigClient {

  private Map<String, Setting> storageMap = new ConcurrentHashMap<>();

  @Override
  protected Future<Setting> getSetting(Marker marker, String id) {
    Setting setting = storageMap.get(id);
    return Future.succeededFuture(setting);
  }

  @Override
  protected Future<Setting> storeSetting(Marker marker, Setting setting) {
    if (setting.id == null) {
      return Future.failedFuture("setting.id is null");
    }
    storageMap.put(setting.id, setting);
    return Future.succeededFuture(setting);
  }
}
