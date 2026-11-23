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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.events.PropertyQueryList;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.ApiParam;
import com.here.xyz.models.hub.Space.Extension;
import com.here.xyz.models.hub.jwt.JWTPayload;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Shared fixtures of the Iceberg read routing tests.
 */
final class IcebergReadTestSupport {

  static final String SPACE_ID = "iceberg-read-test-space";
  static final String BASE_ID = "iceberg-read-test-base";
  static final long SPACE_CREATED_AT = 1_700_000_000_000L;

  private IcebergReadTestSupport() {}

  /**
   * A routing context for a request on the space {@code pathSpaceId}, which stores its data (e.g., the stream info) in the given map.
   */
  static RoutingContext routingContext(Map<String, Object> data, String pathSpaceId) {
    JWTPayload jwt = new JWTPayload();
    jwt.aid = "test-aid";
    data.put("jwt", jwt);
    RoutingContext context = mock(RoutingContext.class);
    HttpServerRequest request = mock(HttpServerRequest.class);
    when(context.request()).thenReturn(request);
    when(request.headers()).thenReturn(MultiMap.caseInsensitiveMultiMap());
    when(request.uri()).thenReturn("/hub/spaces/" + pathSpaceId + "/test");
    when(context.pathParam(ApiParam.Path.SPACE_ID)).thenReturn(pathSpaceId);
    when(context.get(anyString())).thenAnswer(invocation -> data.get(invocation.<String>getArgument(0)));
    when(context.put(anyString(), any())).thenAnswer(invocation -> {
      data.put(invocation.getArgument(0), invocation.getArgument(1));
      return context;
    });
    return context;
  }

  static Space space(String id) {
    Space space = new Space();
    space.setId(id);
    space.setCreatedAt(SPACE_CREATED_AT);
    return space;
  }

  /**
   * A composite space with the ID {@link #SPACE_ID} on the base {@link #BASE_ID}.
   *
   * @param baseVersion The pinned version of the base, or {@code null}
   */
  static Space composite(Long baseVersion) {
    Space space = space(SPACE_ID);
    space.setExtension(new Extension().withSpaceId(BASE_ID).withVersion(baseVersion));
    return space;
  }

  /**
   * An OR of AND lists of property queries with the operation EQUALS, each given as key and values.
   */
  static PropertiesQuery equalsQuery(List<List<Object[]>> anyOf) {
    PropertiesQuery query = new PropertiesQuery();
    for (List<Object[]> allOf : anyOf) {
      PropertyQueryList andList = new PropertyQueryList();
      for (Object[] keyAndValues : allOf)
        andList.add(new PropertyQuery().withKey((String) keyAndValues[0]).withOperation(QueryOperation.EQUALS)
            .withValues(Arrays.asList(keyAndValues).subList(1, keyAndValues.length)));
      query.add(andList);
    }
    return query;
  }

  static PropertiesQuery momTypeQuery(String... values) {
    Object[] keyAndValues = new Object[values.length + 1];
    keyAndValues[0] = "momType";
    System.arraycopy(values, 0, keyAndValues, 1, values.length);
    return equalsQuery(List.<List<Object[]>>of(List.<Object[]>of(keyAndValues)));
  }
}
