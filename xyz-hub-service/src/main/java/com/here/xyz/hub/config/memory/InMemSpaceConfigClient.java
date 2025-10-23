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

import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.hub.config.SpaceConfigClient;
import com.here.xyz.hub.connectors.models.Space;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.Marker;

@SuppressWarnings("unused")
public class InMemSpaceConfigClient extends SpaceConfigClient {

  private final Map<String, Space> spaceMap = new ConcurrentHashMap<>();

  @Override
  public Future<Space> getSpace(Marker marker, String spaceId) {
    return Future.succeededFuture(spaceMap.get(spaceId));
  }

  @Override
  public Future<Void> storeSpace(Marker marker, Space space) {
    if (space.getId() == null) {
      space.setId(RandomStringUtils.randomAlphanumeric(10));
    }
    spaceMap.put(space.getId(), space);
    return Future.succeededFuture();
  }

  @Override
  public Future<Space> deleteSpace(Marker marker, String spaceId) {
    return Future.succeededFuture(spaceMap.remove(spaceId));
  }

  @Override
  protected Future<List<Space>> getSelectedSpaces(Marker marker, SpaceAuthorizationCondition authorizedCondition,
      SpaceSelectionCondition selectedCondition, PropertiesQuery propsQuery) {
    //Sets are not even defined that means all access
    Predicate<Space> authorizationFilter = s -> authorizedCondition.spaceIds == null && authorizedCondition.ownerIds == null
        || authorizedCondition.spaceIds != null && authorizedCondition.spaceIds.contains(s.getId())
        || authorizedCondition.ownerIds != null && authorizedCondition.ownerIds.contains(s.getOwner())
        || s.isShared();
    //Sets are not even defined that means don't filter at all by spaceId or ownerId
    Predicate<Space> selectionFilter = s -> authorizedCondition.spaceIds == null && authorizedCondition.ownerIds == null
        || selectedCondition.spaceIds != null && selectedCondition.spaceIds.contains(s.getId())
        || selectedCondition.ownerIds != null && selectedCondition.ownerIds.contains(s.getOwner())
        || selectedCondition.prefix != null && s.getId().startsWith(selectedCondition.prefix)
        || selectedCondition.shared && s.isShared();

    List<String> contentUpdatedAtList = new ArrayList<>();
    if (propsQuery != null) {
      propsQuery.forEach(conjunctions -> {
        conjunctions.forEach(conj -> {
          conj.getValues().forEach(v -> {
            String operator = QueryOperation.getOutputRepresentation(conj.getOperation());
            contentUpdatedAtList.add(operator);
            contentUpdatedAtList.add(v.toString());
          });
        });
      });
    }

    Predicate<Space> contentUpdatedAtFilter = space -> propsQuery == null || contentUpdatedAtOperation(space.getContentUpdatedAt(),
        contentUpdatedAtList, 1);

    List<Space> spaces = spaceMap.values().stream()
        .filter(authorizationFilter)
        .filter(selectionFilter)
        .filter(contentUpdatedAtFilter)
        .collect(Collectors.toList());
    return Future.succeededFuture(spaces);
  }

  @Override
  public Future<List<Space>> getSpacesFromSuper(Marker marker, String superSpaceId) {
    final List<Space> result = new ArrayList<>();
    spaceMap.forEach((id, space) -> {
      if (space.getExtension() != null && superSpaceId.equals(space.getExtension().getSpaceId())) {
        result.add(space);
      }
    });
    return Future.succeededFuture(result);
  }

  private boolean contentUpdatedAtOperation(long contentUpdatedAt, List<String> contentUpdatedAtList, int idx) {
    if (idx > contentUpdatedAtList.size())
      return false;
    else {
      switch (contentUpdatedAtList.get(0)) {
        case "=":
          return Long.toString(contentUpdatedAt).equals(contentUpdatedAtList.get(idx))
              || contentUpdatedAtOperation(contentUpdatedAt, contentUpdatedAtList, idx + 2);
        case "<>":
          return contentUpdatedAt != Long.parseLong(contentUpdatedAtList.get(idx))
              || contentUpdatedAtOperation(contentUpdatedAt, contentUpdatedAtList, idx + 2);
        case "<":
          return contentUpdatedAt < Long.parseLong(contentUpdatedAtList.get(idx))
              || contentUpdatedAtOperation(contentUpdatedAt, contentUpdatedAtList, idx + 2);
        case ">":
          return contentUpdatedAt > Long.parseLong(contentUpdatedAtList.get(idx))
              || contentUpdatedAtOperation(contentUpdatedAt, contentUpdatedAtList, idx + 2);
        case "<=":
          return contentUpdatedAt <= Long.parseLong(contentUpdatedAtList.get(idx))
              || contentUpdatedAtOperation(contentUpdatedAt, contentUpdatedAtList, idx + 2);
        case ">=":
          return contentUpdatedAt >= Long.parseLong(contentUpdatedAtList.get(idx))
              || contentUpdatedAtOperation(contentUpdatedAt, contentUpdatedAtList, idx + 2);
      }

      return false;
    }
  }

  public static class Provider extends SpaceConfigClient.Provider {

    @Override
    public boolean chooseMe() {
      return "test".equals(System.getProperty("scope"));
    }


    @Override
    protected SpaceConfigClient getInstance() {
      return new InMemSpaceConfigClient();
    }
  }
}
