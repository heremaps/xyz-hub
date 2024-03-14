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
package com.here.naksha.storage.http;

import static com.here.naksha.storage.http.RequestSender.KeyProperties;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.lambdas.Fe1;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.storage.http.cache.RequestSenderCache;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpStorage implements IStorage {

  private static final Logger log = LoggerFactory.getLogger(HttpStorage.class);

  private final RequestSender requestSender;

  public HttpStorage(@NotNull Storage storage) {
    HttpStorageProperties properties = HttpStorage.getProperties(storage);
    requestSender = RequestSenderCache.getInstance()
        .getSenderWith(new KeyProperties(
            storage.getId(),
            properties.getUrl(),
            properties.getHeaders(),
            properties.getConnectTimeout(),
            properties.getSocketTimeout()));
  }

  @Override
  public @NotNull IReadSession newReadSession(@Nullable NakshaContext context, boolean useMaster) {
    return new HttpStorageReadSession(context, useMaster, requestSender);
  }

  @Override
  public void initStorage() {
    log.debug("HttpStorage.initStorage called");
  }

  @Override
  public void startMaintainer() {}

  @Override
  public void maintainNow() {}

  @Override
  public void stopMaintainer() {}

  @Override
  public @NotNull <T> Future<T> shutdown(@Nullable Fe1<T, IStorage> onShutdown) {
    return new FutureTask<>(() -> onShutdown != null ? onShutdown.call(this) : null);
  }

  private static @NotNull HttpStorageProperties getProperties(@NotNull Storage storage) {
    return JsonSerializable.convert(storage.getProperties(), HttpStorageProperties.class);
  }
}
