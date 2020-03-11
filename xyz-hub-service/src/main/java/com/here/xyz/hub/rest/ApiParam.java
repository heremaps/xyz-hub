/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.hub.rest;

import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.events.PropertyQueryList;
import com.here.xyz.events.TagsQuery;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Point;
import io.vertx.ext.web.RoutingContext;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ApiParam {

  private static Object getConvertedValue(String rawValue) {
    // Boolean
    if (rawValue.equals("true")) {
      return true;
    }
    if (rawValue.equals("false")) {
      return false;
    }
    // Long
    try {
      return Long.parseLong(rawValue);
    } catch (NumberFormatException ignored) {
    }
    // Double
    try {
      return Double.parseDouble(rawValue);
    } catch (NumberFormatException ignored) {
    }

    if (rawValue.length() > 2 && rawValue.charAt(0) == '"' && rawValue.charAt(rawValue.length() - 1) == '"') {
      return rawValue.substring(1, rawValue.length() - 1);
    }

    if (rawValue.length() > 2 && rawValue.charAt(0) == '\'' && rawValue.charAt(rawValue.length() - 1) == '\'') {
      return rawValue.substring(1, rawValue.length() - 1);
    }

    // String
    return rawValue;
  }

  private static String getConvertedKey(String rawKey) {
    if (rawKey.startsWith("p.")) {
      return rawKey.replace("p.", "properties.");
    }
    Map<String, String> keyReplacements = new HashMap<String, String>() {{
      put("f.id", "id");
      put("f.createdAt", "properties.@ns:com:here:xyz.createdAt");
      put("f.updatedAt", "properties.@ns:com:here:xyz.updatedAt");
    }};

    return keyReplacements.get(rawKey);
  }

  public static class Header {

  }

  public static class Path {

    public static final String SPACE_ID = "spaceId";
    static final String FEATURE_ID = "featureId";
    static final String TILE_ID = "tileId";
    static final String TILE_TYPE = "type";
  }

  public static class Query {

    public static final String ACCESS_TOKEN = "access_token";
    static final String FEATURE_ID = "id";
    static final String ADD_TAGS = "addTags";
    static final String REMOVE_TAGS = "removeTags";
    static final String TAGS = "tags";
    static final String SELECTION = "selection";
    static final String IF_EXISTS = "e";
    static final String IF_NOT_EXISTS = "ne";
    static final String TRANSACTIONAL = "transactional";
    static final String CONFLICT_RESOLUTION = "cr";
    static final String PREFIX_ID = "prefixId";
    static final String SIMPLIFICATION_LEVEL = "simplificationLevel";
    static final String CLIP = "clip";
    static final String SKIP_CACHE = "skipCache";
    static final String CLUSTERING = "clustering";
    static final String LIMIT = "limit";
    static final String WEST = "west";
    static final String NORTH = "north";
    static final String EAST = "east";
    static final String SOUTH = "south";
    static final String MARGIN = "margin";
    static final String EPSG = "epsg";
    static final String HANDLE = "handle";
    static final String INCLUDE_RIGHTS = "includeRights";
    static final String INCLUDE_CONNECTORS = "includeConnectors";
    static final String OWNER = "owner";
    static final String LAT = "lat";
    static final String LON = "lon";
    static final String RADIUS = "radius";
    static final String REF_SPACE_ID = "refSpaceId";
    static final String REF_FEATURE_ID = "refFeatureId";

    private static List<String> shortOperators = Arrays.asList("!=", ">=", "=gte=", "<=", "=lte=", ">", "=gt=", "<", "=lt=", "=");
    private static Map<String, QueryOperation> operators = new HashMap<String, QueryOperation>() {{
      put("!=", QueryOperation.NOT_EQUALS);
      put(">=", QueryOperation.GREATER_THAN_OR_EQUALS);
      put("=gte=", QueryOperation.GREATER_THAN_OR_EQUALS);
      put("<=", QueryOperation.LESS_THAN_OR_EQUALS);
      put("=lte=", QueryOperation.LESS_THAN_OR_EQUALS);
      put(">", QueryOperation.GREATER_THAN);
      put("=gt=", QueryOperation.GREATER_THAN);
      put("<", QueryOperation.LESS_THAN);
      put("=lt=", QueryOperation.LESS_THAN);
      put("=", QueryOperation.EQUALS);
    }};

    /**
     * Get access to the custom parsed query parameters. Used as a temporary replacement for context.queryParam until
     * https://github.com/vert-x3/issues/issues/380 is resolved.
     */
    public static List<String> queryParam(String param, RoutingContext context) {
      return Api.Context.getQueryParameters(context).getAll(param);
    }

    /**
     * Returns the first value for a query parameter, if such exists, or the provided alternative value otherwise.
     */
    static String getString(RoutingContext context, String param, String alt) {
      queryParam(param, context);
      if (queryParam(param, context).size() == 0) {
        return alt;
      }

      if (queryParam(param, context).get(0) == null) {
        return alt;
      }

      return queryParam(param, context).get(0);
    }

    /**
     * Returns the first value for a query parameter, if such exists and can be parsed as an integer, or the provided alternative value
     * otherwise.
     */
    static int getInteger(RoutingContext context, String param, int alt) {
      try {
        return Integer.parseInt(getString(context, param, null));
      } catch (NumberFormatException | NullPointerException e) {
        return alt;
      }
    }

    /**
     * Returns the first value for a query parameter, if such exists and matches either the string 'true' or 'false', or the provided
     * alternative value otherwise.
     */
    static boolean getBoolean(RoutingContext context, String param, boolean alt) {
      queryParam(param, context);
      if (queryParam(param, context).size() == 0) {
        return alt;
      }

      if ("true".equals(queryParam(param, context).get(0))) {
        return true;
      }

      if ("false".equals(queryParam(param, context).get(0))) {
        return false;
      }

      return alt;
    }

    /**
     * Returns the parsed tags parameter
     */
    static TagsQuery getTags(RoutingContext context) {
      return TagsQuery.fromQueryParameter(queryParam(Query.TAGS, context));
    }

    public static List<String> getSelection(RoutingContext context) {
      if (Query.getString(context, Query.SELECTION, null) == null) {
        return null;
      }

      List<String> input = Query.queryParam(Query.SELECTION, context);
      List<String> selection = new ArrayList<>();

      selection.add("id");
      selection.add("type");
      selection.add("geometry");

      for (String s : input) {
        if (s.startsWith("p.")) {
          selection.add(s.replace("p.", "properties."));
        }
      }

      return selection;
    }

    /**
     * Returns the parsed tags parameter
     */
    static PropertiesQuery getPropertiesQuery(RoutingContext context) {
      PropertiesQuery propertyQuery = context.get("propertyQuery");
      if (propertyQuery == null) {
        propertyQuery = parsePropertiesQuery(context.request().query());
        context.put("propertyQuery", propertyQuery);
      }
      return propertyQuery;
    }

    protected static PropertiesQuery parsePropertiesQuery(String query) {
      if (query == null || query.length() == 0) {
        return null;
      }

      PropertyQueryList pql = new PropertyQueryList();
      Stream.of(query.split("&"))
          .filter(k -> k.startsWith("p.") || k.startsWith("f."))
          .forEach(keyValuePair -> {
            PropertyQuery propertyQuery = new PropertyQuery();

            try {
              keyValuePair = URLDecoder.decode(keyValuePair, "utf-8");
            } catch (UnsupportedEncodingException e) {
              e.printStackTrace();
            }

            for (String shortOperator : shortOperators) {
              if (keyValuePair.contains(shortOperator)) {
                // If the original parameter expression doesn't contain the equal sign API GW appends it at the end
                String[] keyVal = keyValuePair.split(shortOperator);
                if (keyVal.length < 2) {
                  break;
                }
                if ((">".equals(shortOperator) || "<".equals(shortOperator)) && keyVal[1].endsWith("=")) {
                  keyVal[1] = keyVal[1].substring(0, keyVal[1].length() - 1);
                }
                propertyQuery.setKey(getConvertedKey(keyVal[0]));
                propertyQuery.setOperation(operators.get(shortOperator));
                String[] rawValues = keyVal[1].split(",");
                ArrayList<Object> values = new ArrayList<>();
                for (String rawValue : rawValues) {
                  values.add(getConvertedValue(rawValue));
                }
                propertyQuery.setValues(values);
                pql.add(propertyQuery);
                break;
              }
            }
          });
      PropertiesQuery pq = new PropertiesQuery();
      pq.add(pql);

      if (pq.stream().flatMap(List::stream).mapToLong(l -> l.getValues().size()).sum() == 0) {
        return null;
      }
      return pq;
    }

    static Map<String, Object> getClusteringParams(RoutingContext context) {
      Map<String, Object> clusteringParams = context.get("clusteringParams");
      if (clusteringParams == null) {
        clusteringParams = parseClusteringParams(context.request().query());
        context.put("clusteringParams", clusteringParams);
      }
      return clusteringParams;
    }

    static Map<String, Object> parseClusteringParams(String query) {
      if (query == null || query.length() == 0) {
        return null;
      }

      final String clusterPrefix = Query.CLUSTERING + ".";
      Map<String, Object> cp = new HashMap<>();
      Stream.of(query.split("&"))
          .filter(k -> k.startsWith(clusterPrefix))
          .forEach(keyValuePair -> {
            try {
              keyValuePair = URLDecoder.decode(keyValuePair, "utf-8");
            } catch (UnsupportedEncodingException e) {
              e.printStackTrace();
            }

            if (keyValuePair.contains("=")) {
              // If the original parameter expression doesn't contain the equal sign API GW appends it at the end
              String[] keyVal = keyValuePair.split("=");
              if (keyVal.length < 2) {
                return;
              }
              cp.put(keyVal[0].substring(clusterPrefix.length()), getConvertedValue(keyVal[1]));
            }
          });

      return cp;
    }

    public static Point getCenter(RoutingContext context)
        throws Exception {
      double lat,lon;
      List<String> latParam = queryParam(LAT, context);
      List<String> lonParam = queryParam(LON, context);

      if(latParam.size() == 0 && lonParam.size() == 0)
        return null;
      if(latParam.size() == 1 && lonParam.size() == 0)
        throw new Exception("'"+LON+"' Param is missing!");
      if(lonParam.size() == 1 && latParam.size() == 0)
        throw new Exception("'"+LAT+"' Param is missing!");

      try {
        lat = Double.parseDouble(latParam.get(0));
      } catch (Exception e) {
        throw new Exception("Invalid '"+LAT+"' query parameter, must be a WGS'84 longitude in decimal degree (so a value between -90 and +90).");
      }
      try {
        lon = Double.parseDouble(lonParam.get(0));
      } catch (Exception e) {
        throw new Exception("Invalid '"+LON+"' query parameter, must be a WGS'84 longitude in decimal degree (so a value between -180 and +180).");
      }

      return new Point().withCoordinates(new PointCoordinates(lon,lat));
    }

    public static Integer getRadius(RoutingContext context) {
      return getInteger(context, RADIUS, 0);
    }

    public static String getRefFeatureId(RoutingContext context) {
      return getString(context, REF_FEATURE_ID, null);
    }

    public static String getRefSpaceId(RoutingContext context) {
      return getString(context, REF_SPACE_ID , null);
    }
  }
}
