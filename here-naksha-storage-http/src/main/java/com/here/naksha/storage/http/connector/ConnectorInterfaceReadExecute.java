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
package com.here.naksha.storage.http.connector;

import static com.here.naksha.common.http.apis.ApiParamsConst.*;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.geojson.WebMercatorTile;
import com.here.naksha.lib.core.models.geojson.coordinates.BBox;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.events.feature.*;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.ReadFeaturesProxyWrapper;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.storage.http.PrepareResult;
import com.here.naksha.storage.http.RequestSender;
import com.here.naksha.storage.http.connector.pop.POpToQueries;
import com.here.naksha.storage.http.connector.pop.POpToQueries.POpQueries;
import java.net.http.HttpResponse;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;

public class ConnectorInterfaceReadExecute {

  @NotNull
  public static Result execute(NakshaContext context, ReadFeaturesProxyWrapper request, RequestSender sender) {
    String streamId = context.getStreamId();
    String endpoint = "/" + request.getCollections().get(0);

    Event event =
        switch (request.getReadRequestType()) {
          case GET_BY_ID -> createFeatureByIdEvent(request);
          case GET_BY_IDS -> createFeaturesByIdsEvent(request);
          case GET_BY_BBOX -> createFeatureByBBoxEvent(request);
          case GET_BY_TILE -> createFeaturesByTileEvent(request);
          case ITERATE -> createIterateEvent(request);
        };

    event.setStreamId(streamId);

    String jsonEvent = JsonSerializable.serialize(event);
    HttpResponse<byte[]> httpResponse = sender.post(endpoint, jsonEvent);

    return PrepareResult.prepareReadResult(
        httpResponse, XyzFeatureCollection.class, XyzFeatureCollection::getFeatures);
  }

  private static Event createIterateEvent(ReadFeaturesProxyWrapper request) {
    long limit = request.getQueryParameter(LIMIT);
    IterateFeaturesEvent event = new IterateFeaturesEvent();
    event.setLimit(limit);
    return event;
  }

  private static Event createFeaturesByIdsEvent(ReadFeaturesProxyWrapper request) {
    List<String> id = request.getQueryParameter(FEATURE_IDS);
    return new GetFeaturesByIdEvent().withIds(id);
  }

  private static Event createFeatureByIdEvent(ReadFeaturesProxyWrapper request) {
    String id = request.getQueryParameter(FEATURE_ID);
    return new GetFeaturesByIdEvent().withIds(List.of(id));
  }

  private static Event createFeatureByBBoxEvent(ReadFeaturesProxyWrapper request) {
    BBox bBox = new BBox(
        request.getQueryParameter(WEST),
        request.getQueryParameter(SOUTH),
        request.getQueryParameter(EAST),
        request.getQueryParameter(NORTH));
    long limit = request.getQueryParameter(LIMIT);
    boolean clip = request.getQueryParameter(CLIP_GEO);

    GetFeaturesByBBoxEvent getFeaturesByBBoxEvent = new GetFeaturesByBBoxEvent();
    getFeaturesByBBoxEvent.setLimit(limit);
    getFeaturesByBBoxEvent.setBbox(bBox);
    getFeaturesByBBoxEvent.setClip(clip);
    setPropertyQuery(request, getFeaturesByBBoxEvent);

    return getFeaturesByBBoxEvent;
  }

  static void setPropertyQuery(ReadFeatures request, QueryEvent getFeaturesByBBoxEvent) {
    POp pOp = request.getPropertyOp();
    if (pOp != null) {
      POpQueries pOpQueries = POpToQueries.pOpToQuery(pOp);
      pOpQueries.propertyQuery().ifPresent(getFeaturesByBBoxEvent::setPropertiesQuery);
      pOpQueries.tagsQuery().ifPresent(getFeaturesByBBoxEvent::setTags);
    }
  }

  private static Event createFeaturesByTileEvent(ReadFeaturesProxyWrapper readRequest) {
    String tileType = readRequest.getQueryParameter(TILE_TYPE);
    if (TILE_TYPE_QUADKEY.equals(tileType)) {
      long margin = readRequest.getQueryParameter(MARGIN);
      long limit = readRequest.getQueryParameter(LIMIT);
      String tileId = readRequest.getQueryParameter(TILE_ID);
      boolean clip = readRequest.getQueryParameter(CLIP_GEO);

      GetFeaturesByTileEvent event = new GetFeaturesByTileEvent();
      event.setMargin((int) margin);
      event.setLimit(limit);
      event.setClip(clip);
      setPropertyQuery(readRequest, event);

      WebMercatorTile tileAddress = WebMercatorTile.forQuadkey(tileId);
      event.setBbox(tileAddress.getExtendedBBox(event.getMargin()));
      event.setLevel(tileAddress.level);
      event.setX(tileAddress.x);
      event.setY(tileAddress.y);
      event.setQuadkey(tileAddress.asQuadkey());
      return event;
    } else {
      throw new NotImplementedException("Tile type other than " + TILE_TYPE_QUADKEY);
    }
  }
}
