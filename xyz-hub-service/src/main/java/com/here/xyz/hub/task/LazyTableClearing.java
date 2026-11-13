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

package com.here.xyz.hub.task;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.SpaceConfigClient.ClearResult;
import com.here.xyz.hub.util.SpaceTableResolver;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.service.HttpException;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

/**
 * Clears the lazy-table flag of a space after its first write.
 * <p>
 * A space created on a connector with lazy table creation has no table until its first write. The connector reports
 * {@code materialized} on the response of a successful write to such a space. The Hub removes that field from the response
 * and, after the client has its response, removes {@code storage.params.lazyTable} from the stored space with a conditional
 * operation of the space config client. From then on the space is an ordinary space with its table.
 */
public class LazyTableClearing {

  private static final Logger logger = LogManager.getLogger();

  public static final String SOURCE_CONNECTOR_REPORT = "connector-report";
  public static final String SOURCE_ADMIN_API = "admin-api";
  static final int MAX_CONNECTOR_ID_LENGTH = 256;

  private LazyTableClearing() {}

  /**
   * Reads and removes the {@code materialized} field of a connector response, so that it never reaches a client, a processor
   * or a listener.
   *
   * @param response The response of the connector
   * @return {@code true} if the response is a feature collection with {@code materialized} set to {@code true}
   */
  public static boolean takeMaterialized(XyzResponse response) {
    if (!(response instanceof FeatureCollection featureCollection))
      return false;
    boolean materialized = Boolean.TRUE.equals(featureCollection.getMaterialized());
    featureCollection.setMaterialized(null);
    return materialized;
  }

  /**
   * @param space The space the write was sent for
   * @param context The context of the event that was sent, {@code null} means {@link SpaceContext#DEFAULT}
   * @return The space whose table the write went to: the immediate base for {@link SpaceContext#SUPER}, else the space
   */
  public static Space writeTarget(Space space, SpaceContext context) {
    if (space == null)
      return null;
    if (context == SUPER)
      return space.getExtension() == null ? null : space.getExtension().resolvedSpace;
    return space;
  }

  /**
   * @return Whether the contentUpdatedAt store of a write has to wait for the clear, because the write target is the space
   *     itself and it carries the flag
   */
  public static boolean defersContentUpdate(Space space, SpaceContext context) {
    return context != SUPER && space != null && space.getStorage() != null && Space.isLazyTable(space.getStorage().getParams());
  }

  /**
   * Clears the flag of the write target after a write that reported {@code materialized}.
   *
   * @param marker The log marker
   * @param space The space the write was sent for
   * @param context The context of the event that was sent
   * @param materialized What {@link #takeMaterialized(XyzResponse)} returned for the response
   * @return The result of the clear, or {@code null} if no clear was made: without {@code materialized}, or if the write
   *     target does not carry the flag in the Hub's own config
   */
  public static Future<ClearResult> clearAfterWrite(Marker marker, Space space, SpaceContext context, boolean materialized) {
    if (!materialized)
      return Future.succeededFuture();
    try {
      Space target = writeTarget(space, context);
      if (target == null || target.getStorage() == null || !Space.isLazyTable(target.getStorage().getParams())) {
        logger.debug(marker, "lazyTable report ignored for space {} with context {}: the write target carries no flag",
            space.getId(), context);
        return Future.succeededFuture();
      }
      //A flagged level has the storage ID of the composite, so the composite's connector reported the write
      return Service.spaceConfigClient.clearLazyTable(marker, target.getId(), space.getStorage().getId(),
          SpaceTableResolver.getTableName(target), SOURCE_CONNECTOR_REPORT);
    }
    catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  /**
   * @return Whether the flag is absent from the stored space after a clear with the specified result
   */
  public static boolean isCleared(ClearResult clearResult) {
    return clearResult == ClearResult.REMOVED || clearResult == ClearResult.ABSENT;
  }

  /**
   * Validates the parameters of the admin route that clears the flag.
   *
   * @throws HttpException With status 400 for a missing space ID or connector, a connector ID longer than
   *     {@value #MAX_CONNECTOR_ID_LENGTH} characters, or an invalid table name
   */
  public static void validateClearParams(String spaceId, String connector, String table) throws HttpException {
    if (spaceId == null || spaceId.isEmpty())
      throw new HttpException(BAD_REQUEST, "The space ID is required.");
    if (connector == null || connector.isEmpty())
      throw new HttpException(BAD_REQUEST, "The query parameter \"connector\" is required.");
    if (connector.length() > MAX_CONNECTOR_ID_LENGTH)
      throw new HttpException(BAD_REQUEST,
          "The query parameter \"connector\" must not exceed " + MAX_CONNECTOR_ID_LENGTH + " characters.");
    if (table == null)
      return;
    if (table.isEmpty())
      throw new HttpException(BAD_REQUEST, "The query parameter \"table\" must not be empty.");
    try {
      SpaceTableResolver.validateTableName(table);
    }
    catch (IllegalArgumentException e) {
      throw new HttpException(BAD_REQUEST, e.getMessage(), e);
    }
  }
}
