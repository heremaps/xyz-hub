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

package com.here.xyz.hub.rest;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;

import com.amazonaws.util.StringUtils;
import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Point;
import io.vertx.ext.web.RoutingContext;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class ApiParam {
  public static class Path {

    public static final String SPACE_ID = "spaceId";
    public static final String CONNECTOR_ID = "connectorId";
    public static final String FEATURE_ID = "featureId";
    public static final String TILE_ID = "tileId";
    public static final String TILE_TYPE = "type";
    public static final String SUBSCRIPTION_ID = "subscriptionId";
    public static final String VERSION = "version";
    public static final String TAG_ID = "tagId";

    public static final String INCLUDE_SYSTEM_TAGS = "includeSystemTags";
  }

  public static class Query {

    public static final String ACCESS_TOKEN = "access_token";
    static final String FEATURE_ID = "id";
    static final String ADD_TAGS = "addTags";
    static final String REMOVE_TAGS = "removeTags";
    static final String TAGS = "tags";
    static final String SELECTION = "selection";
    static final String SORT = "sort";
    static final String PART = "part";
    static final String IF_EXISTS = "e";
    static final String IF_NOT_EXISTS = "ne";
    static final String TRANSACTIONAL = "transactional";
    static final String CONFLICT_DETECTION = "conflictDetection";
    static final String CONFLICT_RESOLUTION = "cr";
    static final String PREFIX_ID = "prefixId";
    static final String CLIP = "clip";
    static final String FAST_MODE = "fastMode";
    static final String SKIP_CACHE = "skipCache";
    static final String CLUSTERING = "clustering";
    static final String TWEAKS = "tweaks";
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
    static final String TAG = "tag";
    static final String OWNER = "owner";
    static final String LAT = "lat";
    static final String LON = "lon";
    static final String RADIUS = "radius";
    static final String REF_SPACE_ID = "refSpaceId";
    static final String REF_FEATURE_ID = "refFeatureId";
    static final String H3_INDEX = "h3Index";
    static final String CONTENT_UPDATED_AT = "contentUpdatedAt";
    static final String CONTEXT = "context";

    static final String CLUSTERING_PARAM_RESOLUTION = "resolution";
    static final String CLUSTERING_PARAM_RESOLUTION_RELATIVE = "relativeResolution";
    static final String CLUSTERING_PARAM_RESOLUTION_ABSOLUTE = "absoluteResolution";
    static final String CLUSTERING_PARAM_NOBUFFER = "noBuffer";
    static final String CLUSTERING_PARAM_PROPERTY = "property";
    static final String CLUSTERING_PARAM_POINTMODE = "pointmode";
    static final String CLUSTERING_PARAM_SINGLECOORD = "singlecoord";
    static final String CLUSTERING_PARAM_COUNTMODE = "countmode";
    static final String CLUSTERING_PARAM_SAMPLING = "sampling";


    static final String TWEAKS_PARAM_STRENGTH  = "strength";
    static final String TWEAKS_PARAM_ALGORITHM = "algorithm";
    static final String TWEAKS_PARAM_DEFAULT_SELECTION = "defaultselection";
    static final String TWEAKS_PARAM_SAMPLINGTHRESHOLD = "samplingthreshold";

    static final String FORCE_2D = "force2D";
    static final String OPTIM_MODE = "mode";
    static final String OPTIM_VIZSAMPLING = "vizSampling";
    static final String VERSION = "version";
    static final String VERSION_REF = "versionRef";
    static final String START_VERSION = "startVersion";
    static final String END_VERSION = "endVersion";
    static final String PAGE_TOKEN = "pageToken";
    static final String AUTHOR = "author";

    static final String F_PREFIX = "f.";

    static final String REGION = "region";

    //TODO: Remove that query parameter from the public API. It should not be addressable by the user. If needed for testing, we should rather use Unit tests and/or mocks instead.
    @Deprecated
    static final String CONNECTOR_MAPPING = "connectorMapping";

    static final String DRY_RUN = "dryRun",
                        FORCE_STORAGE = "forceStorage";

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
    public static String getString(RoutingContext context, String param, String alt) {
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
    public static Integer getInteger(RoutingContext context, String param, Integer alt) {
      try {
        return Integer.parseInt(getString(context, param, null));
      }
      catch (NumberFormatException | NullPointerException e) {
        return alt;
      }
    }

    /**
     * Returns the first value for a query parameter, if such exists and can be parsed as a long, or the provided alternative value
     * otherwise.
     * @param context The routing context
     * @param param The name of the query param
     * @param alt The alternative value for the optional query parameter
     * @return The parsed long value
     */
    public static Long getLong(RoutingContext context, String param, Long alt) {
      try {
        return Long.parseLong(getString(context, param, null));
      }
      catch (NumberFormatException | NullPointerException e) {
        return alt;
      }
    }

    /**
     * Returns the first value for a mandatory query parameter after parsing it as a long.
     * @param context The routing context
     * @param param The name of the query param
     * @return The parsed long value
     * @throws NumberFormatException If the query param's value could not be parsed as long
     * @throws NullPointerException If the mandatory query param was not provided
     */
    static long getLong(RoutingContext context, String param) throws NumberFormatException, NullPointerException {
      String val = getString(context, param, null);
      Objects.requireNonNull(val);
      return Long.parseLong(val);
    }

    /**
     * Returns the first value for a query parameter, if such exists and matches either the string 'true' or 'false', or the provided
     * alternative value otherwise.
     */
    public static boolean getBoolean(RoutingContext context, String param, boolean alt) {
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

    public static List<String> getSelection(RoutingContext context) {
      if (Query.getString(context, Query.SELECTION, null) == null) {
        return null;
      }

      List<String> input = Query.queryParam(Query.SELECTION, context);

      if (input.size() == 1 && ("*".equals(input.get(0)) || "!geometry".equalsIgnoreCase(input.get(0))))
        return new ArrayList<>(input);

      HashSet<String> selection = new HashSet<>(Arrays.asList("id", "type"));

      for (String s : input)
       switch( s )
       { case "properties": case "id" : case "type" : break;
         default : selection.add( s.replaceFirst("^p\\.", "properties.") ); break;
       }

      return new ArrayList<>(selection);
    }

    public static List<String> getSort(RoutingContext context) {
      if (Query.getString(context, Query.SORT, null) == null) return null;

      List<String> sort = new ArrayList<>();
      for (String s : Query.queryParam(Query.SORT, context))
        if (s.startsWith("p.") || s.startsWith(F_PREFIX))
         sort.add( s.replaceFirst("^p\\.", "properties.") );

      return sort;
    }

    public static Integer[] getPart(RoutingContext context) {
      if (Query.getString(context, Query.PART, null) == null) return null;

      int part, total;
      List<String> l = Query.queryParam(Query.PART, context);
      if( l.size() <= 2 && l.size() >= 1 )
       try
       { part  =  Integer.parseUnsignedInt( l.get(0) );
         total =  ( l.size() > 1 ? Integer.parseUnsignedInt( l.get(1) ) : /* -1 */ 1 ); // -1 to use n-handle modus
         return ( part == 0 || total == 0 ) ? null : new Integer[]{ Math.min(part, total), Math.max(part,total) };
       }
       catch(NumberFormatException e){}

      return null;
    }



    /**
     * Retures the parsed query parameter for space
     */
    static PropertiesQuery getSpacePropertiesQuery(RoutingContext context, String param) {
      PropertiesQuery propertyQuery = context.get("propertyQuery");
      if (propertyQuery == null) {
        propertyQuery = PropertiesQuery.fromString(context.request().query(), param, true);
        context.put("propertyQuery", propertyQuery);
      }
      return propertyQuery;
    }

    /**
     * Returns the parsed tags parameter
     */
    static PropertiesQuery getPropertiesQuery(RoutingContext context) {
      PropertiesQuery propertyQuery = context.get("propertyQuery");
      if (propertyQuery == null) {
        propertyQuery = PropertiesQuery.fromString(context.request().query(), "", false);
        context.put("propertyQuery", propertyQuery);
      }
      return propertyQuery;
    }

    /**
     * Returns the first property found in the query string in the format of: `<key> <operator> <value(s)>`
     * @param query the query part in the url without the '?' or '&' symbol
     * @param key the property to be searched
     * @param multiValue when true, checks for comma separated values, otherwise return the first value found
     * @return null in case none is found
     */
    static PropertyQuery getPropertyQuery(String query, String key, boolean multiValue) {
      if (StringUtils.isNullOrEmpty(query) || StringUtils.isNullOrEmpty(key))
        return null;

      try {
        query = URLDecoder.decode(query, Charset.defaultCharset().name());
      }
      catch (UnsupportedEncodingException e) {
        return null;
      }

      int startIndex = query.indexOf(key);
      if (startIndex != -1) {
        String opValue = query.substring(startIndex + key.length()); // e.g. =eq=head
        String operation = QueryOperation.inputRepresentations()
            .stream()
            .sorted(Comparator.comparingInt(k -> k.length() * -1)) // reverse a sorted list because u want to get the longer ops first.
            .filter(opValue::startsWith) // e.g. in case of key=eq=val, 2 ops will be filtered in: '=eq=' and '='.
            .findFirst() // The reversed sort plus the findFirst makes sure the =eq= is the one you are looking for.
            .orElse(null); // e.g. anything different from the allowed operators

        if (operation == null)
          return null;

        String value = opValue.substring(operation.length()).split("&")[0];
        List<Object> values = multiValue
            ? Arrays.asList(value.split(","))
            : Collections.singletonList(value.split(",")[0]);

        return new PropertyQuery()
            .withKey(key)
            .withOperation(QueryOperation.fromInputRepresentation(operation))
            .withValues(values);
      }

      return null;
    }

    static Map<String, Object> getAdditionalParams(RoutingContext context, String type) throws Exception{
      Map<String, Object> clusteringParams = context.get(type);

      if (clusteringParams == null) {
        clusteringParams = parseAdditionalParams(context.request().query(), type);
        context.put(type, clusteringParams);
      }
      return clusteringParams;
    }

    static Map<String, Object> parseAdditionalParams(String query, String type) throws Exception{
      if (query == null || query.length() == 0) {
        return null;
      }

      final String paramPrefix = type + ".";

      Map<String, Object> cp = new HashMap<>();
      Stream.of(query.split("&"))
              .filter(k -> k.startsWith(paramPrefix))
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
                  String key = keyVal[0].substring(paramPrefix.length());
                  Object value = PropertiesQuery.getConvertedValue(keyVal[1]);
                  try {
                    validateAdditionalParams(type,key,value);
                  }catch (Exception e){
                    throw new RuntimeException(e.getMessage());
                  }
                  cp.put(keyVal[0].substring(paramPrefix.length()), PropertiesQuery.getConvertedValue(keyVal[1]));
                }
              });

      return cp;
    }

    private static void validateAdditionalParams(String type, String key, Object value) throws  Exception{
      if (type.equals(CLUSTERING)) {
        String invalidKeyMessage = "Invalid clustering. " + key + " value. ";
        switch (key) {
          case CLUSTERING_PARAM_RESOLUTION_ABSOLUTE:
          case CLUSTERING_PARAM_RESOLUTION_RELATIVE:
          case CLUSTERING_PARAM_RESOLUTION:
            if (!(value instanceof Long))
              throw new Exception(invalidKeyMessage + "Expect Integer.");

            if (CLUSTERING_PARAM_RESOLUTION_RELATIVE.equals(key) && ((long)value < -2 || (long)value > 4))
             throw new Exception(invalidKeyMessage + "Expect Integer hexbin:[-2,2], quadbin:[0-4].");

            if (!CLUSTERING_PARAM_RESOLUTION_RELATIVE.equals(key) && ((long)value < 0 || (long)value > 18))
              throw new Exception(invalidKeyMessage + "Expect Integer hexbin:[0,13], quadbin:[0,18].");
            break;

          case CLUSTERING_PARAM_PROPERTY:
            if(!(value instanceof String))
              throw new Exception(invalidKeyMessage + "Expect String.");
            break;

          case CLUSTERING_PARAM_POINTMODE:
          case CLUSTERING_PARAM_SINGLECOORD:
          case CLUSTERING_PARAM_NOBUFFER:
          if(!(value instanceof Boolean))
              throw new Exception(invalidKeyMessage + "Expect true or false.");
            break;

          case CLUSTERING_PARAM_COUNTMODE:
            if(!(value instanceof String))
              throw new Exception(invalidKeyMessage + "Expect one of [real,estimated,mixed].");
            break;

          case CLUSTERING_PARAM_SAMPLING:
            if(!(value instanceof String))
              throw new Exception(invalidKeyMessage + "Expect one of [low,lowmed,med,medhigh,high].");
            break;

          default: throw new Exception("Invalid Clustering Parameter! Expect one of ["
                          +CLUSTERING_PARAM_RESOLUTION+","+CLUSTERING_PARAM_RESOLUTION_RELATIVE+","+CLUSTERING_PARAM_RESOLUTION_ABSOLUTE+","
                          +CLUSTERING_PARAM_PROPERTY+","+CLUSTERING_PARAM_POINTMODE+","+CLUSTERING_PARAM_COUNTMODE+"," +CLUSTERING_PARAM_SINGLECOORD+","
                          +CLUSTERING_PARAM_NOBUFFER+","+CLUSTERING_PARAM_SAMPLING +"].");
        }
      }else if(type.equals(TWEAKS)){
        switch( key )
        {
         case TWEAKS_PARAM_STRENGTH :
           if(value instanceof String)
           { String keyS = ((String) value).toLowerCase();
             switch (keyS)
             { case "low": case "lowmed": case "med": case "medhigh": case "high": break;
               default:
                throw new Exception("Invalid tweaks.strength value. Expect [low,lowmed,med,medhigh,high]");
             }
           }
           else if(value instanceof Long)
           {
            if((long)value < 1 || (long)value > 100)
             throw new Exception("Invalid tweaks.strength value. Expect Integer [1,100].");
           }
           else
            throw new Exception("Invalid tweaks.strength value. Expect String or Integer.");

         break;

         case TWEAKS_PARAM_DEFAULT_SELECTION:
          if(!(value instanceof Boolean))
           throw new Exception("Invalid tweaks.defaultselection value. Expect true or false.");
          break;

         case TWEAKS_PARAM_ALGORITHM : break;

         case TWEAKS_PARAM_SAMPLINGTHRESHOLD : // testing, parameter evaluation
          if(!(value instanceof Long) || ((long) value < 10) || ((long) value > 100) )
           throw new Exception("Invalid tweaks. " + key + ". Expect Integer [10,100].");
          break;

         default:
          throw new Exception("Invalid Tweaks Parameter! Expect one of [" + TWEAKS_PARAM_STRENGTH + ","
                                                                          + TWEAKS_PARAM_ALGORITHM + ","
                                                                          + TWEAKS_PARAM_DEFAULT_SELECTION + ","
                                                                          + TWEAKS_PARAM_SAMPLINGTHRESHOLD + "]");
        }

      }
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

    public static ContextAwareEvent.SpaceContext getContext(RoutingContext context){
      return ContextAwareEvent.SpaceContext.of(Query.getString(context, CONTEXT, DEFAULT.toString()).toUpperCase());
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

    public static String getH3Index(RoutingContext context) {
      return getString(context, H3_INDEX , null);
    }
  }
}
