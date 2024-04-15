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
package com.here.naksha.app.service.http.ops;

import static com.here.naksha.common.http.apis.ApiParamsConst.PROP_SELECTION;
import static com.here.naksha.lib.core.models.payload.events.QueryDelimiter.*;

import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import com.here.naksha.lib.core.models.payload.events.QueryDelimiter;
import com.here.naksha.lib.core.models.payload.events.QueryParameter;
import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import com.here.naksha.lib.core.util.ValueList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class PropertySelectionUtil {

  private static final String SHORT_PROP_PREFIX = "p.";
  private static final String FULL_PROP_PREFIX = XyzFeature.PROPERTIES + ".";
  private static final String SHORT_XYZ_PROP_PREFIX = "f.";
  private static final String FULL_XYZ_PROP_PREFIX = FULL_PROP_PREFIX + XyzProperties.XYZ_NAMESPACE + ".";
  private static final String XYZ_PROP_ID = "f.id";
  private static final String PROP_ID = "id";
  private static final String PROP_FEATURE_TYPE = "type";
  private static final String SHORT_GEOM_PREFIX = "g.";
  private static final String FULL_GEOM_PREFIX = XyzFeature.GEOMETRY + ".";

  private PropertySelectionUtil() {}

  /**
   * Function builds unique Set of property paths based on key:value pairs supplied as API query parameter "selection".
   * We iterate through all the values, split them based on delimiter "," and expand them into its full form,
   * that can be used as Json path while extracting fields from the GeoJson feature.
   *
   * <pre>
   * So query params :
   * "selection=p.name,p.capacity&selection=p.color,rootPropertyName&selection=f.tags"
   *
   * will result into set of:
   *    properties.name
   *    properties.capacity
   *    properties.color
   *    rootPropertyName
   *    properties.@ns:com:here:xyz.tags
   * and will also add standard properties:
   *    id
   *    type
   *    geometry
   *
   * Note - Entire 'geometry' object is added by default, UNLESS it is partially requested.
   *
   * </pre>
   *
   * @param queryParams API query parameter from where property selection params need to be extracted
   * @return Set of expanded property paths
   */
  public static @Nullable Set<String> buildPropPathSetFromQueryParams(
      final @Nullable QueryParameterList queryParams) {
    if (queryParams == null) return null;
    QueryParameter propParams = queryParams.get(PROP_SELECTION);
    if (propParams == null) return null;

    // prop path set to be returned
    Set<String> gPropPathSet = null;
    while (propParams != null && propParams.hasValues()) {
      // get list of all value tokens and respective delimiters
      final ValueList valueTokenList = propParams.values();
      final List<QueryDelimiter> delimList = propParams.valuesDelimiter();
      // iterate through propPath tokens and add them to Global paths depending on delimiter value
      int delimIdx = 0;
      for (final Object value : valueTokenList) {
        if (value == null) {
          delimIdx++;
          continue;
        }
        final String path = String.valueOf(value);
        if (path.isEmpty()) {
          delimIdx++;
          continue;
        }
        final QueryDelimiter delimiter = delimList.get(delimIdx++);
        if (delimiter != AMPERSAND && delimiter != END && delimiter != COMMA) {
          throw new XyzErrorException(
              XyzError.ILLEGAL_ARGUMENT,
              "Invalid delimiter " + delimiter + " for parameter " + PROP_SELECTION);
        }
        if (gPropPathSet == null) {
          gPropPathSet = new HashSet<>();
          // add standard properties
          gPropPathSet.add(PROP_ID);
          gPropPathSet.add(PROP_FEATURE_TYPE);
          gPropPathSet.add(XyzFeature.GEOMETRY);
        }
        final String expandedPath = expandPropSelectionPath(path);
        // remove entire "geometry" object, if it is already (partially) requested
        if (expandedPath.startsWith(FULL_GEOM_PREFIX)) {
          gPropPathSet.remove(XyzFeature.GEOMETRY);
        }
        gPropPathSet.add(expandedPath);
      }
      propParams = propParams.next();
    }

    return gPropPathSet;
  }

  private static @NotNull String expandPropSelectionPath(final @NotNull String propPath) {
    final StringBuilder str = new StringBuilder();
    if (propPath.startsWith(SHORT_PROP_PREFIX)) {
      str.append(FULL_PROP_PREFIX).append(propPath.substring(SHORT_PROP_PREFIX.length()));
    } else if (propPath.equals(XYZ_PROP_ID)) {
      str.append(PROP_ID);
    } else if (propPath.startsWith(SHORT_XYZ_PROP_PREFIX)) {
      str.append(FULL_XYZ_PROP_PREFIX).append(propPath.substring(SHORT_XYZ_PROP_PREFIX.length()));
    } else if (propPath.startsWith(SHORT_GEOM_PREFIX)) {
      str.append(FULL_GEOM_PREFIX).append(propPath.substring(SHORT_GEOM_PREFIX.length()));
    } else {
      str.append(propPath);
    }
    return str.toString();
  }
}
