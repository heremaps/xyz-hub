/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

import com.here.xyz.hub.connectors.models.Space;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Marker;

@SuppressWarnings("unused")
public class InMemSpaceConfigClient extends SpaceConfigClient {

  private Map<String, Space> spaceMap = new ConcurrentHashMap<>();

  @Override
  public void init(Handler<AsyncResult<Void>> onReady) {
    onReady.handle(Future.succeededFuture());
  }

  @Override
  public void getSpace(Marker marker, String spaceId, Handler<AsyncResult<Space>> handler) {
    Space space = spaceMap.get(spaceId);
    handler.handle(Future.succeededFuture(space));
  }

  @Override
  public void storeSpace(Marker marker, Space space, Handler<AsyncResult<Space>> handler) {
    if (space.getId() == null) {
      space.setId(RandomStringUtils.randomAlphanumeric(10));
    }
    spaceMap.put(space.getId(), space);
    handler.handle(Future.succeededFuture(space));
  }

  @Override
  public void deleteSpace(Marker marker, String spaceId, Handler<AsyncResult<Space>> handler) {
    Space space = spaceMap.remove(spaceId);
    handler.handle(Future.succeededFuture(space));
  }

  @Override
  public void getSelectedSpaces(Marker marker, SpaceAuthorizationCondition authorizedCondition, SpaceSelectionCondition selectedCondition,
      Handler<AsyncResult<List<Space>>> handler) {
    //Sets are not even defined that means all access
    Predicate<Space> authorizationFilter = s -> authorizedCondition.spaceIds == null && authorizedCondition.ownerIds == null
        || authorizedCondition.spaceIds != null && authorizedCondition.spaceIds.contains(s.getId())
        || authorizedCondition.ownerIds != null && authorizedCondition.ownerIds.contains(s.getOwner())
        || s.isShared();
    //Sets are not even defined that means don't filter at all by spaceId or ownerId
    Predicate<Space> selectionFilter = s -> authorizedCondition.spaceIds == null && authorizedCondition.ownerIds == null
        || selectedCondition.spaceIds != null && selectedCondition.spaceIds.contains(s.getId())
        || selectedCondition.ownerIds != null && selectedCondition.ownerIds.contains(s.getOwner())
        || selectedCondition.shared && s.isShared();

    List<Space> spaces = spaceMap.values().stream()
        .filter(authorizationFilter)
        .filter(selectionFilter)
        .collect(Collectors.toList());
    handler.handle(Future.succeededFuture(spaces));
  }
}
