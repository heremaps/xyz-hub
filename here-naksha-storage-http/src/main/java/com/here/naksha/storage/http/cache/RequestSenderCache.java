/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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
package com.here.naksha.storage.http.cache;

import static com.here.naksha.storage.http.RequestSender.KeyProperties;

import com.here.naksha.storage.http.RequestSender;
import java.util.concurrent.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class RequestSenderCache {

  public static final int CLEANER_PERIOD_HOURS = 8;
  private final ConcurrentMap<String, RequestSender> requestSenders;

  private RequestSenderCache() {
    this(new ConcurrentHashMap<>(), CLEANER_PERIOD_HOURS, TimeUnit.HOURS);
  }

  RequestSenderCache(ConcurrentMap<String, RequestSender> requestSenders, int cleanPeriod, TimeUnit cleanPeriodUnit) {
    this.requestSenders = requestSenders;
    Executors.newSingleThreadScheduledExecutor()
        .scheduleAtFixedRate(requestSenders::clear, cleanPeriod, cleanPeriod, cleanPeriodUnit);
  }

  @NotNull
  public static RequestSenderCache getInstance() {
    return InstanceHolder.instance;
  }

  @NotNull
  public RequestSender getSenderWith(KeyProperties keyProperties) {
    return requestSenders.compute(
        keyProperties.name(), (__, cachedSender) -> getUpdated(cachedSender, keyProperties));
  }

  private @NotNull RequestSender getUpdated(
      @Nullable RequestSender cachedSender, @NotNull KeyProperties keyProperties) {
    if (cachedSender != null && cachedSender.hasKeyProps(keyProperties)) return cachedSender;
    else return new RequestSender(keyProperties);
  }

  private static final class InstanceHolder {
    private static final RequestSenderCache instance = new RequestSenderCache();
  }
}
