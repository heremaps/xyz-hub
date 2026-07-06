/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

import com.here.xyz.Typed;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.dynamo.DynamoEntityConfigClient;
import com.here.xyz.util.service.Initializable;
import io.vertx.core.Future;
import java.util.List;
import java.util.UUID;

public abstract class EntityConfigClient implements Initializable {

  public static EntityConfigClient getInstance() {
    return new DynamoEntityConfigClient(Service.configuration.ENTITIES_DYNAMODB_TABLE_ARN);
  }

  public <E extends Typed> Future<UUID> store(E entity) {
    return storeEntity(entity);
  }

  public <E extends Typed> Future<E> load(Class<E> entityType, String uuid) {
    return loadEntity(entityType, uuid);
  }

  public <E extends Typed> Future<List<E>> loadAll(Class<E> entityType) {
    return loadEntities(entityType);
  }

  public Future<Void> delete(Class<? extends Typed> entityType, String uuid) {
    return deleteEntity(entityType, uuid);
  }

  protected abstract <E extends Typed> Future<UUID> storeEntity(E entity);
  protected abstract <E extends Typed> Future<E> loadEntity(Class<E> entityType, String uuid);
  protected abstract <E extends Typed> Future<List<E>> loadEntities(Class<E> entityType);
  protected abstract Future<Void> deleteEntity(Class<? extends Typed> entityType, String uuid);
}
