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

package com.here.xyz.hub.cache;

import io.vertx.core.Handler;
import java.util.Arrays;
import java.util.List;

public class MultiLevelCacheClient implements CacheClient {

  final List<CacheClient> clients;

  public MultiLevelCacheClient(CacheClient... clients) {
    this.clients = Arrays.asList(clients);
  }

  @Override
  public void get(String key, Handler<byte[]> handler) {
    _get(0, key, handler);
  }

  public void _get(final int i, final String key, final Handler<byte[]> handler) {
    clients.get(i).get(key, result -> {
      if (result == null) {
        if (clients.size() > i + 1) {
          _get(i + 1, key, handler);
        } else {
          handler.handle(null);
        }
      } else {
        int j = i;
        while (--j >= 0) {
          clients.get(j).set(key, result, Integer.MAX_VALUE);
        }
        handler.handle(result);
      }
    });
  }

  @Override
  public void set(String key, byte[] value, long ttl) {
    clients.forEach(c -> c.set(key, value, ttl));
  }

  @Override
  public void remove(String key) {
    clients.forEach(c -> c.remove(key));
  }

  @Override
  public void shutdown() {
    clients.forEach(c -> c.shutdown());
  }
}
