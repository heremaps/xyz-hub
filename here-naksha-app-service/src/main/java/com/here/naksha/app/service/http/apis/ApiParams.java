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

import static com.here.naksha.common.http.apis.ApiParamsConst.*;

import com.here.naksha.app.service.models.IterateHandle;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.payload.events.QueryParameter;
import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import com.here.naksha.lib.core.util.ValueList;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class ApiParams {

  public static @NotNull String extractMandatoryPathParam(
      final @NotNull RoutingContext routingContext, final @NotNull String param) {
    final String value = routingContext.pathParam(param);
    if (value == null || value.isEmpty()) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Missing " + param + " parameter");
    }
    return value;
  }

  public static @Nullable QueryParameter extractQueryParamForKey(
      final @Nullable QueryParameterList queryParams, final @NotNull String key, final boolean isMandatory) {
    if (queryParams == null) {
      if (isMandatory) throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Query parameters missing");
      else return null;
    }
    final QueryParameter queryParam = queryParams.get(key);
    if (queryParam == null || queryParam.values().isEmpty()) {
      if (isMandatory) throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Parameter " + key + " missing");
      else return null;
    }
    return queryParam;
  }

  public static boolean extractQueryParamAsBoolean(
      final @Nullable QueryParameterList queryParams, final @NotNull String key, final boolean isMandatory) {
    return extractQueryParamAsBoolean(queryParams, key, isMandatory, false);
  }

  public static boolean extractQueryParamAsBoolean(
      final @Nullable QueryParameterList queryParams,
      final @NotNull String key,
      final boolean isMandatory,
      final boolean defVal) {
    final QueryParameter queryParam = extractQueryParamForKey(queryParams, key, isMandatory);
    if (queryParam == null && !isMandatory) {
      return defVal;
    }
    final ValueList values = queryParam.values();
    if (values.isBoolean(0)) {
      return values.getBoolean(0, defVal);
    } else if (values.isString(0)) {
      return Boolean.parseBoolean(values.getString(0));
    }
    throw new XyzErrorException(
        XyzError.ILLEGAL_ARGUMENT, "Invalid value " + values.get(0) + " for parameter " + key);
  }

  public static double extractQueryParamAsDouble(
      final @Nullable QueryParameterList queryParams, final @NotNull String key, final boolean isMandatory) {
    return extractQueryParamAsDouble(queryParams, key, isMandatory, 0.0);
  }

  public static double extractQueryParamAsDouble(
      final @Nullable QueryParameterList queryParams,
      final @NotNull String key,
      final boolean isMandatory,
      final double defVal) {
    final QueryParameter queryParam = extractQueryParamForKey(queryParams, key, isMandatory);
    if (queryParam == null && !isMandatory) {
      return defVal;
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
      final @Nullable QueryParameterList queryParams, final @NotNull String key, final boolean isMandatory) {
    return extractQueryParamAsLong(queryParams, key, isMandatory, 0);
  }

  public static long extractQueryParamAsLong(
      final @Nullable QueryParameterList queryParams,
      final @NotNull String key,
      final boolean isMandatory,
      final long defVal) {
    final QueryParameter queryParam = extractQueryParamForKey(queryParams, key, isMandatory);
    if (queryParam == null && !isMandatory) {
      return defVal;
    }
    final ValueList values = queryParam.values();
    if (!values.isLong(0)) {
      throw new XyzErrorException(
          XyzError.ILLEGAL_ARGUMENT, "Invalid value " + values.getString(0) + " for parameter " + key);
    }
    return values.getLong(0, defVal);
  }

  public static void validateParamRange(
      final @NotNull String param, final long value, final long min, final long max) {
    if (value < min || value > max) {
      throw new XyzErrorException(
          XyzError.ILLEGAL_ARGUMENT, "Invalid value " + value + " for parameter " + param);
    }
  }

  public static void validateParamRange(
      final @NotNull String param, final double value, final double min, final double max) {
    if (value < min || value > max) {
      throw new XyzErrorException(
          XyzError.ILLEGAL_ARGUMENT, "Invalid value " + value + " for parameter " + param);
    }
  }

  public static void validateFeatureId(
      final @NotNull RoutingContext routingContext, final @NotNull String idFromRequest) {
    final String featureId = ApiParams.extractMandatoryPathParam(routingContext, FEATURE_ID);
    if (!featureId.equals(idFromRequest)) {
      throw new XyzErrorException(
          XyzError.ILLEGAL_ARGUMENT,
          "URI path parameter featureId is not the same as id in feature request body.");
    }
  }

  public static @Nullable QueryParameterList queryParamsFromRequest(final @NotNull RoutingContext routingContext) {
    return (routingContext.request().query() != null)
        ? new QueryParameterList(routingContext.request().query())
        : null;
  }

  public static @Nullable List<String> extractParamAsStringList(
      final @Nullable QueryParameterList queryParams, final @NotNull String apiParamType) {
    return (queryParams != null) ? queryParams.collectAllOfAsString(apiParamType) : null;
  }

  public static @Nullable String extractParamAsString(
      final @Nullable QueryParameterList queryParams, final @NotNull String apiParamType) {
    return (queryParams != null) ? queryParams.getValueAsString(apiParamType) : null;
  }

  public static @Nullable IterateHandle extractQueryParamAsIterateHandle(
      final @Nullable QueryParameterList queryParams, final @NotNull String apiParamName) {
    final String handleStr = extractParamAsString(queryParams, apiParamName);
    IterateHandle handle = null;
    if (!StringUtils.isEmpty(handleStr)) {
      try {
        handle = IterateHandle.base64DecodedDeserializedJson(handleStr);
      } catch (Exception ex) {
        throw new XyzErrorException(
            XyzError.ILLEGAL_ARGUMENT,
            "Unable to use value " + handleStr + " for parameter " + apiParamName + ". " + ex.getMessage());
      }
    }
    return handle;
  }

  public static void validateLatLon(final double lat, final double lon) {
    if (lat != NULL_COORDINATE) validateParamRange(LAT, lat, -90, 90);
    if (lon != NULL_COORDINATE) validateParamRange(LON, lon, -180, 180);
    // Validate that both lat and lon provided or none of them
    if (lat == NULL_COORDINATE && lon != NULL_COORDINATE) {
      // only lon provided, lan is not
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Missing latitude co-ordinate");
    } else if (lat != NULL_COORDINATE && lon == NULL_COORDINATE) {
      // only lan provided, lon is not
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Missing longitude co-ordinate");
    }
  }
}
