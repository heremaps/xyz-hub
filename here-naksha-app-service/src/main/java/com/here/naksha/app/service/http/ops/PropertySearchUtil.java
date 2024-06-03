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
package com.here.naksha.app.service.http.ops;

import static com.here.naksha.lib.core.models.payload.events.QueryDelimiter.*;
import static com.here.naksha.lib.core.models.payload.events.QueryDelimiter.COMMA;
import static com.here.naksha.lib.core.models.payload.events.QueryOperation.*;
import static com.here.naksha.lib.core.util.storage.RequestHelper.pRefFromPropPath;

import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import com.here.naksha.lib.core.models.payload.events.QueryDelimiter;
import com.here.naksha.lib.core.models.payload.events.QueryOperation;
import com.here.naksha.lib.core.models.payload.events.QueryParameter;
import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.util.ValueList;
import java.util.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class PropertySearchUtil {

  private static final String NULL_PROP_VALUE = ".null";

  private static final String SHORT_PROP_PREFIX = "p.";
  private static final String FULL_PROP_PREFIX = XyzFeature.PROPERTIES + ".";
  private static final String SHORT_XYZ_PROP_PREFIX = "f.";
  private static final String FULL_XYZ_PROP_PREFIX = FULL_PROP_PREFIX + XyzProperties.XYZ_NAMESPACE + ".";
  private static final String XYZ_PROP_ID = "f.id";

  private PropertySearchUtil() {}

  /**
   * Function builds Property Operation (POp) based on property key:value pairs supplied as API query parameter.
   * We iterate through all the parameters, exclude the keys that doesn't start with prefix "p." or "f." or "properties.",
   * and interpret the others by identifying the desired operation.
   * <p>
   * Multiple parameter keys result into AND list.
   * <br>
   * So, "p.prop_1=value_1&p.prop_2=value_2" will form AND condition as (p.prop_1=value_1 AND p.prop_2=value_2).
   * </p>
   *
   * <p>
   * Multiple parameter values concatenated with "," (COMMA) delimiter, will result into OR list.
   * <br>
   * So, "p.prop_1=value_1,value_11" will form OR condition as (p.prop_1=value_1 OR p.prop_1=value_11).
   * </p>
   *
   * @param queryParams API query parameter from where property search params need to be extracted
   * @return POp property operation that can be used as part of {@link ReadRequest}
   */
  public static @Nullable POp buildOperationForPropertySearchParams(final @Nullable QueryParameterList queryParams) {
    if (queryParams == null) return null;
    // global initialization
    final List<POp> globalOpList = new ArrayList<>();
    // iterate through each parameter
    for (final QueryParameter param : queryParams) {
      // prepare property search operation
      final POp crtOp = preparePropertySearchOperation(param);
      // add current search operation to global list
      if (crtOp != null) globalOpList.add(crtOp);
    }

    if (globalOpList.isEmpty()) return null;
    // return single operation or AND list (in case of multiple operations)
    if (globalOpList.size() > 1) {
      return POp.and(globalOpList.toArray(POp[]::new));
    }
    return globalOpList.get(0);
  }

  private static @Nullable String[] expandKeyToRealJsonPath(final @NotNull String key) {
    final StringBuilder str = new StringBuilder();
    if (key.startsWith(SHORT_PROP_PREFIX)) {
      str.append(FULL_PROP_PREFIX).append(key.substring(SHORT_PROP_PREFIX.length()));
    } else if (key.equals(XYZ_PROP_ID)) {
      str.append("id");
    } else if (key.startsWith(SHORT_XYZ_PROP_PREFIX)) {
      str.append(FULL_XYZ_PROP_PREFIX).append(key.substring(SHORT_XYZ_PROP_PREFIX.length()));
    } else if (key.startsWith(FULL_PROP_PREFIX)) {
      str.append(key);
    } else {
      return null; // excluding non-supported prop-search key
    }
    return str.toString().split("\\.");
  }

  private static @Nullable POp preparePropertySearchOperation(final @NotNull QueryParameter param) {
    // extract param key, operation, values, delimiters
    final String propKey = param.key();
    final QueryOperation operation = param.op();
    final ValueList propValues = param.values();
    final List<QueryDelimiter> delimiters = param.valuesDelimiter();

    // global operation list if multiple values are supplied for this property key
    final List<POp> gOpList = new ArrayList<>();

    // expand key if needed (e.g. p.prop_1 should be properties.prop_1)
    final String[] propPath = expandKeyToRealJsonPath(propKey);
    if (propPath == null) return null;

    // iterate through all given values for a key
    int delimIdx = 0;
    for (final Object value : propValues) {
      if (value == null) {
        throw new XyzErrorException(
            XyzError.ILLEGAL_ARGUMENT, "Unsupported null value for key %s".formatted(propKey));
      }
      // validate delimiter ("," to be taken as OR operation)
      final QueryDelimiter delimiter = delimiters.get(delimIdx++);
      if (delimiter != AMPERSAND && delimiter != COMMA && delimiter != END) {
        throw new XyzErrorException(
            XyzError.ILLEGAL_ARGUMENT, "Unsupported delimiter %s for key %s".formatted(delimiter, propKey));
      }
      // prepare property operation for crt value
      final POp crtOp;
      if (value instanceof String str) {
        crtOp = mapAPIOperationToPropertyOperation(operation, propPath, str);
      } else if (value instanceof Number num) {
        crtOp = mapAPIOperationToPropertyOperation(operation, propPath, num);
      } else if (value instanceof Boolean bool) {
        crtOp = mapAPIOperationToPropertyOperation(operation, propPath, bool);
      } else {
        throw new XyzErrorException(
            XyzError.ILLEGAL_ARGUMENT,
            "Unsupported value type %s for key %s"
                .formatted(value.getClass().getName(), propKey));
      }
      // add current operation to global list
      gOpList.add(crtOp);
    }

    // return single operation or OR list (in case of multiple operations)
    if (gOpList.size() > 1) {
      return POp.or(gOpList.toArray(POp[]::new));
    }
    return gOpList.get(0);
  }

  private static @NotNull POp mapAPIOperationToPropertyOperation(
      final @NotNull QueryOperation operation, final @NotNull String[] propPath, final @NotNull String value) {
    if (operation == EQUALS) {
      // check if it is NULL operation
      if (NULL_PROP_VALUE.equals(value)) {
        return POp.not(POp.exists(pRefFromPropPath(propPath)));
      } else {
        return POp.eq(pRefFromPropPath(propPath), value);
      }
    } else if (operation == NOT_EQUALS) {
      // check if it is NOT NULL operation
      if (NULL_PROP_VALUE.equals(value)) {
        return POp.exists(pRefFromPropPath(propPath));
      } else {
        return POp.not(POp.eq(pRefFromPropPath(propPath), value));
      }
    } else if (operation == CONTAINS) {
      // if string represents JSON object, then we automatically add JSON array comparison
      if (value.startsWith("{") && value.endsWith("}")) {
        return POp.or(
            POp.contains(pRefFromPropPath(propPath), value),
            POp.contains(pRefFromPropPath(propPath), "[%s]".formatted(value)));
      } else {
        return POp.contains(pRefFromPropPath(propPath), value);
      }
    } else {
      throw new XyzErrorException(
          XyzError.ILLEGAL_ARGUMENT,
          "Unsupported operation %s with string value %s".formatted(operation.name, value));
    }
  }

  private static @NotNull POp mapAPIOperationToPropertyOperation(
      final @NotNull QueryOperation operation, final @NotNull String[] propPath, final @NotNull Number value) {
    if (operation == EQUALS) {
      return POp.eq(pRefFromPropPath(propPath), value);
    } else if (operation == NOT_EQUALS) {
      return POp.not(POp.eq(pRefFromPropPath(propPath), value));
    } else if (operation == GREATER_THAN) {
      return POp.gt(pRefFromPropPath(propPath), value);
    } else if (operation == GREATER_THAN_OR_EQUALS) {
      return POp.gte(pRefFromPropPath(propPath), value);
    } else if (operation == LESS_THAN) {
      return POp.lt(pRefFromPropPath(propPath), value);
    } else if (operation == LESS_THAN_OR_EQUALS) {
      return POp.lte(pRefFromPropPath(propPath), value);
    } else if (operation == CONTAINS) {
      return POp.contains(pRefFromPropPath(propPath), value);
    } else {
      throw new XyzErrorException(
          XyzError.ILLEGAL_ARGUMENT,
          "Unsupported operation %s with numeric value %s".formatted(operation.name, value));
    }
  }

  private static @NotNull POp mapAPIOperationToPropertyOperation(
      final @NotNull QueryOperation operation, final @NotNull String[] propPath, final @NotNull Boolean value) {
    if (operation == EQUALS) {
      return POp.eq(pRefFromPropPath(propPath), value);
    } else if (operation == NOT_EQUALS) {
      return POp.not(POp.eq(pRefFromPropPath(propPath), value));
    } else if (operation == CONTAINS) {
      return POp.contains(pRefFromPropPath(propPath), value);
    } else {
      throw new XyzErrorException(
          XyzError.ILLEGAL_ARGUMENT,
          "Unsupported operation %s with boolean value %s".formatted(operation.name, value));
    }
  }
}
