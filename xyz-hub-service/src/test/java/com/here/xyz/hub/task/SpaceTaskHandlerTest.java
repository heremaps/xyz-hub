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

import static com.here.xyz.models.hub.FeatureModificationList.IfExists.ERROR;
import static com.here.xyz.models.hub.FeatureModificationList.IfNotExists.CREATE;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.here.xyz.hub.Config;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.SpaceTask.ConditionalOperation;
import com.here.xyz.hub.task.TaskPipeline.Callback;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.models.hub.jwt.JWTPayload;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SpaceTaskHandlerTest {

  private Config previousConfiguration;

  @BeforeEach
  void setUp() {
    previousConfiguration = Service.configuration;
    Service.configuration = new Config();
  }

  @AfterEach
  void tearDown() {
    Service.configuration = previousConfiguration;
  }

  @Test
  void keepsLegacyResolutionWhenUniqueTableNamesAreDisabled() {
    Service.configuration.ENABLE_UNIQUE_PHYSICAL_TABLE_NAMES = false;
    ModifySpaceOp modifyOp = new ModifySpaceOp(List.of(new HashMap<>(Map.of("id", "my-space"))), CREATE, ERROR, true, false);
    Space result = new Space();
    result.setId("my-space");
    result.setStorage(new ConnectorRef().withId("psql"));
    modifyOp.entries.get(0).result = result;
    ConditionalOperation task = new ConditionalOperation(routingContext(), ApiResponseType.EMPTY, modifyOp, false);

    SpaceTaskHandler.assignTableName(task, successfulCallback());

    assertNull(modifyOp.entries.get(0).result.getStorage().getParams());
  }

  private static Callback<ConditionalOperation> successfulCallback() {
    return new Callback<>() {
      @Override
      public void exception(Throwable e) {
        throw new AssertionError(e);
      }

      @Override
      public void call(ConditionalOperation value) {
      }
    };
  }

  private static RoutingContext routingContext() {
    RoutingContext context = mock(RoutingContext.class);
    HttpServerRequest request = mock(HttpServerRequest.class);
    JWTPayload jwt = new JWTPayload();
    jwt.aid = "test-aid";
    when(context.request()).thenReturn(request);
    when(request.headers()).thenReturn(MultiMap.caseInsensitiveMultiMap());
    when(context.get("jwt")).thenReturn(jwt);
    when(context.put(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(context);
    return context;
  }
}
