/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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
import com.here.xyz.hub.config.dynamo.DynamoDataReferenceConfigClient;
import com.here.xyz.models.hub.DataReference;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.Initializable;
import io.vertx.core.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class DataReferenceConfigClient implements Initializable {

  private static final Logger logger = LogManager.getLogger();

  private static final class InstanceHolder {
    private static final DataReferenceConfigClient instance = new DynamoDataReferenceConfigClient(
      Service.configuration.REFERENCES_DYNAMODB_TABLE_ARN
    );
  }

  public static DataReferenceConfigClient getInstance() {
    return InstanceHolder.instance;
  }

  protected abstract Future<UUID> doStore(DataReference dataReference);

  protected abstract Future<Optional<DataReference>> doLoad(UUID id);

  protected abstract Future<List<DataReference>> doLoad(
    String entityId,
    Integer startVersion,
    Integer endVersion,
    String contentType,
    String objectType,
    String sourceSystem,
    String targetSystem
  );

  protected abstract Future<Void> doDelete(UUID id);

  public final Future<UUID> store(@Nonnull DataReference dataReference) {
    checkNotNull(dataReference, "DataReference cannot be null");
    return doStore(dataReference);
  }

  public final Future<Optional<DataReference>> load(@Nonnull UUID id) {
    checkNotNull(id, "DataReference id cannot be null");
    return doLoad(id);
  }

  public final Future<List<DataReference>> load(
    @Nonnull String entityId,
    @Nullable Integer startVersion,
    @Nullable Integer endVersion,
    @Nullable String contentType,
    @Nullable String objectType,
    @Nullable String sourceSystem,
    @Nullable String targetSystem
  ) {
    checkNotNull(entityId, "DataReference entityId cannot be null");
    return doLoad(
      entityId,
      startVersion,
      endVersion,
      contentType,
      objectType,
      sourceSystem,
      targetSystem
    );
  }

  public final Future<Void> delete(@Nonnull UUID id) {
    checkNotNull(id, "DataReference id cannot be null");
    return doDelete(id);
  }

  public final Future<List<DataReference>> loadByEntity(@Nonnull String entityId) {
    return load(entityId, null, null, null, null, null, null);
  }

  /**
   * Sets the keepUntil timestamp to the current time for all DataReferences of the given entity, effectively expiring them.
   * Dynamo client will expire them after 48H grace period
   */
  public final Future<Void> expireForEntity(@Nullable Marker marker, @Nonnull String entityId) {
    checkNotNull(entityId, "DataReference entityId cannot be null");
    return loadByEntity(entityId).compose(references -> {
      if (references == null || references.isEmpty()) {
        logger.info(marker, "expireForEntity: no DataReferences found for entityId={}", entityId);
        return Future.succeededFuture();
      }
      logger.info(marker, "expireForEntity: setting on {} DataReferences for entityId={}",
           references.size(), entityId);
      return Future.all(references.stream()
              .map(reference -> doStore(reference.withKeepUntil(Core.currentTimeMillis()))
                  .onFailure(t -> logger.warn(marker,
                      "expireForEntity: failed to update keepUntil for reference id={} entityId={}",
                      reference.getId(), entityId, t)))
              .toList())
          .mapEmpty();
    });
  }
}
