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
package com.here.naksha.app.service.http.tasks;

import com.here.naksha.app.service.http.HttpResponseType;
import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.naksha.Space;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.storage.IReadTransaction;
import com.here.naksha.lib.core.storage.IResultSet;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class SpaceApiTask<T extends XyzResponse> extends ApiTask<XyzResponse> {

  private final @NotNull SpaceApiReqType reqType;

  public enum SpaceApiReqType {
    GET_ALL_SPACES,
    GET_SPACE_BY_ID,
    CREATE_SPACE,
    UPDATE_SPACE,
    DELETE_SPACE
  }

  public SpaceApiTask(
      final @NotNull SpaceApiReqType reqType,
      final @NotNull NakshaHttpVerticle verticle,
      final @NotNull INaksha nakshaHub,
      final @NotNull RoutingContext routingContext,
      final @NotNull NakshaContext nakshaContext) {
    super(verticle, nakshaHub, routingContext, nakshaContext);
    this.reqType = reqType;
  }

  /**
   * Initializes this task.
   */
  @Override
  protected void init() {}

  /**
   * Execute this task.
   *
   * @return the response.
   */
  @NotNull
  @Override
  protected XyzResponse execute() {
    switch (this.reqType) {
      case GET_ALL_SPACES:
        return executeGetSpaces();
      default:
        return executeUnsupported();
    }
  }

  private XyzResponse executeGetSpaces() {
    try (final IReadTransaction tx = naksha().storage().openReplicationTransaction(naksha().settings())) {
      // TODO HP : Fix reading of all spaces
      /*
      final IResultSet<Space> rs = tx.readFeatures(Space.class, NakshaAdminCollection.SPACES)
      .getAll(0, Integer.MAX_VALUE);
      */
      final IResultSet<Space> rs = null;
      final List<@NotNull Space> featureList = rs.toList(0, Integer.MAX_VALUE);
      rs.close();
      final XyzFeatureCollection response = new XyzFeatureCollection();
      response.setFeatures(featureList);
      verticle.sendXyzResponse(routingContext, HttpResponseType.FEATURE_COLLECTION, response);
      return response;
    }
  }
}
