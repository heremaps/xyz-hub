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
package com.here.naksha.app.service.http.apis;

import io.vertx.ext.web.RoutingContext;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class ApiParams {
  public static String SPACE_ID = "spaceId";
  public static String PREFIX_ID = "prefixId";
  public static String ADD_TAGS = "addTags";
  public static String REMOVE_TAGS = "removeTags";

  public static @Nullable List<String> queryParamList(
      final @NotNull RoutingContext routingContext, final @NotNull String param) {
    return routingContext.queryParam(param);
  }

  public static @Nullable String queryParam(
      final @NotNull RoutingContext routingContext, final @NotNull String param) {
    final List<String> paramList = routingContext.queryParam(param);
    return (paramList == null || paramList.isEmpty()) ? null : paramList.get(0);
  }

  public static @Nullable String pathParam(
      final @NotNull RoutingContext routingContext, final @NotNull String param) {
    return routingContext.pathParam(param);
  }
}
