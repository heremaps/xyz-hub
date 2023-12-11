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

import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.payload.events.QueryParameter;
import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import com.here.naksha.lib.core.util.ValueList;
import io.vertx.ext.web.RoutingContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class ApiParams {
  public static String STORAGE_ID = "storageId";
  public static String SPACE_ID = "spaceId";
  public static String PREFIX_ID = "prefixId";
  public static String FEATURE_ID = "featureId";
  public static String ADD_TAGS = "addTags";
  public static String REMOVE_TAGS = "removeTags";
  public static String TAGS = "tags";
  public static String FEATURE_IDS = "id";
  public static String WEST = "west";
  public static String NORTH = "north";
  public static String EAST = "east";
  public static String SOUTH = "south";
  public static String LIMIT = "limit";
  public static String TILE_TYPE = "type";
  public static String TILE_ID = "tileId";

  public static long DEF_FEATURE_LIMIT = 30_000;
  public static long DEF_ADMIN_FEATURE_LIMIT = 1_000;

  public static String TILE_TYPE_QUADKEY = "quadkey";

  public static @NotNull String extractMandatoryPathParam(
      final @NotNull RoutingContext routingContext, final @NotNull String param) {
    final String value = routingContext.pathParam(param);
    if (value == null || value.isEmpty()) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Missing " + param + " parameter");
    }
    return value;
  }

  public static double extractQueryParamAsDouble(
      final @NotNull QueryParameterList queryParams, final @NotNull String key, final boolean isMandatory) {
    return extractQueryParamAsDouble(queryParams, key, isMandatory, 0.0);
  }

  public static double extractQueryParamAsDouble(
      final @NotNull QueryParameterList queryParams,
      final @NotNull String key,
      final boolean isMandatory,
      final double defVal) {
    final QueryParameter queryParam = queryParams.get(key);
    if (queryParam == null || queryParam.values().size() < 1) {
      if (isMandatory) throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Parameter " + key + " missing");
      else return defVal;
    }
    final ValueList values = queryParam.values();
    if (values.isDouble(0)) {
      return values.getDouble(0, defVal);
    } else if (values.isLong(0)) {
      final Long longVal = values.getLong(0);
      return (longVal == null) ? defVal : longVal.doubleValue();
    }
    throw new XyzErrorException(
        XyzError.ILLEGAL_ARGUMENT, "Invalid value " + values.getString(0) + " for parameter " + key);
  }

  public static long extractQueryParamAsLong(
      final @NotNull QueryParameterList queryParams, final @NotNull String key, final boolean isMandatory) {
    return extractQueryParamAsLong(queryParams, key, isMandatory, 0);
  }

  public static long extractQueryParamAsLong(
      final @Nullable QueryParameterList queryParams,
      final @NotNull String key,
      final boolean isMandatory,
      final long defVal) {
    if (queryParams == null) {
      if (isMandatory) throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Query parameters missing");
      else return defVal;
    }
    final QueryParameter queryParam = queryParams.get(key);
    if (queryParam == null || queryParam.values().size() < 1) {
      if (isMandatory) throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Parameter " + key + " missing");
      else return defVal;
    }
    final ValueList values = queryParam.values();
    if (!values.isLong(0)) {
      throw new XyzErrorException(
          XyzError.ILLEGAL_ARGUMENT, "Invalid value " + values.getString(0) + " for parameter " + key);
    }
    return values.getLong(0, defVal);
  }

  public static void validateParamRange(
      final @NotNull String param, final double value, final double min, final double max) {
    if (value < min || value > max) {
      throw new XyzErrorException(
          XyzError.ILLEGAL_ARGUMENT, "Invalid value " + value + " for parameter " + param);
    }
  }
}
