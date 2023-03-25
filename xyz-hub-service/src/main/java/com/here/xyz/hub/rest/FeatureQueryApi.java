/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

import static com.here.xyz.events.feature.GetFeaturesByTileEvent.ResponseType.GEO_JSON;
import static com.here.xyz.events.feature.GetFeaturesByTileEvent.ResponseType.MVT;
import static com.here.xyz.events.feature.GetFeaturesByTileEvent.ResponseType.MVT_FLATTENED;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_VND_MAPBOX_VECTOR_TILE;
import static com.here.xyz.hub.rest.ApiParam.Query.FORCE_2D;
import static com.here.xyz.hub.rest.ApiParam.Query.SKIP_CACHE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.feature.GetFeaturesByBBoxEvent;
import com.here.xyz.events.feature.GetFeaturesByGeometryEvent;
import com.here.xyz.events.feature.GetFeaturesByTileEvent;
import com.here.xyz.events.info.GetStatisticsEvent;
import com.here.xyz.events.feature.IterateFeaturesEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.feature.SearchForFeaturesEvent;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.task.feature.GetFeaturesByBBoxTask;
import com.here.xyz.hub.task.feature.GetFeaturesByGeometryTask;
import com.here.xyz.hub.task.feature.GetStatistics;
import com.here.xyz.hub.task.feature.IterateQuery;
import com.here.xyz.hub.task.feature.SearchQuery;
import com.here.xyz.hub.task.feature.TileQuery;
import com.here.xyz.hub.util.geo.GeoTools;
import com.here.xyz.models.geojson.HQuad;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.geojson.implementation.Geometry;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.ParsedHeaderValue;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import java.io.IOException;
import java.util.List;
import org.apache.logging.log4j.Marker;

public class FeatureQueryApi extends SpaceBasedApi {

  public FeatureQueryApi(RouterBuilder rb) {
    rb.operation("getFeaturesBySpatial").handler(this::getFeaturesBySpatial);
    rb.operation("getFeaturesBySpatialPost").handler(this::getFeaturesBySpatial);
    rb.operation("getFeaturesByBBox").handler(this::getFeaturesByBBox);
    rb.operation("getFeaturesByTile").handler(this::getFeaturesByTile);
    rb.operation("getFeaturesCount").handler(this::getFeaturesCount);
    rb.operation("getStatistics").handler(this::getStatistics);
    rb.operation("iterateFeatures").handler(this::iterateFeatures);
    rb.operation("searchForFeatures").handler(this::searchForFeatures);
  }

  /**
   * Retrieves the count of features in the space.
   */
  @Deprecated
  private void getFeaturesCount(final RoutingContext context) {
    new GetStatistics(new GetStatisticsEvent(), context, ApiResponseType.COUNT_RESPONSE, Query.getBoolean(context, SKIP_CACHE, false))
        .execute(this::sendResponse, this::sendErrorResponse);
  }

  /**
   * Retrieves the statistics about a space.
   */
  private void getStatistics(final RoutingContext context) {
    new GetStatistics(new GetStatisticsEvent(), context, ApiResponseType.STATISTICS_RESPONSE, Query.getBoolean(context, SKIP_CACHE, false))
        .execute(this::sendResponse, this::sendErrorResponse);
  }

  /**
   * Searches for features by tags.
   */
  private void searchForFeatures(final RoutingContext context) {
    try {
      final boolean skipCache = Query.getBoolean(context, SKIP_CACHE, false);
      final boolean force2D = Query.getBoolean(context, FORCE_2D, false);

      final SearchForFeaturesEvent event = new SearchForFeaturesEvent();
      event.withLimit(getLimit(context))
          .withTags(Query.getTags(context))
          .withPropertiesQuery(Query.getPropertiesQuery(context))
          .withForce2D(force2D)
          .withSelection(Query.getSelection(context));

      final SearchQuery task = new SearchQuery(event, context, ApiResponseType.FEATURE_COLLECTION, skipCache);
      task.execute(this::sendResponse, this::sendErrorResponse);
    } catch (HttpException e) {
      sendErrorResponse(context, e);
    }
  }

  /**
   * Iterate the content of the space.
   */
  private void iterateFeatures(final RoutingContext context) {
    try {
      final boolean skipCache = Query.getBoolean(context, SKIP_CACHE, false);
      final boolean force2D = Query.getBoolean(context, FORCE_2D, false);
      Integer version = Query.getInteger(context, Query.VERSION, null);
      final SpaceContext spaceContext = getSpaceContext(context);
      final String author = Query.getString(context, Query.AUTHOR, null);

      List<String> sort = Query.getSort(context);
      PropertiesQuery propertiesQuery = Query.getPropertiesQuery(context);
      String handle = Query.getString(context, Query.HANDLE, null);
      Integer[] part = Query.getPart(context);

      if (sort != null || propertiesQuery != null || part != null || ( handle != null && handle.startsWith("h07~"))) {
        IterateFeaturesEvent event = new IterateFeaturesEvent();
        event.withLimit(getLimit(context))
            .withForce2D(force2D)
            .withTags(Query.getTags(context))
            .withPropertiesQuery(propertiesQuery)
            .withSelection(Query.getSelection(context))
            .withSort(sort)
            .withPart(part)
            .withHandle(handle)
            .withContext(spaceContext)
            .withAuthor(author);

        final SearchQuery task = new SearchQuery(event, context, ApiResponseType.FEATURE_COLLECTION, skipCache);
        task.execute(this::sendResponse, this::sendErrorResponse);
        return;
      }

      IterateFeaturesEvent event = new IterateFeaturesEvent()
          .withLimit(getLimit(context))
          .withForce2D(force2D)
          .withTags(Query.getTags(context))
          .withSelection(Query.getSelection(context))
          .withV(version)
          .withHandle(Query.getString(context, Query.HANDLE, null))
          .withContext(spaceContext);

      final IterateQuery task = new IterateQuery(event, context, ApiResponseType.FEATURE_COLLECTION, skipCache);
      task.execute(this::sendResponse, this::sendErrorResponse);
    } catch (HttpException e) {
      sendErrorResponse(context, e);
    }
  }

  /**
   *  Retrieves the features by intersecting with the provided geometry.
   */
  public void getFeaturesBySpatial(final RoutingContext context) {
    try {
      final String refFeatureId = Query.getRefFeatureId(context);
      final String refSpaceId = Query.getRefSpaceId(context);
      final String h3Index = Query.getH3Index(context);

      Geometry geometry = null;

      if(context.request().method() == HttpMethod.GET) {
        try {
          geometry = Query.getCenter(context);
        } catch (Exception e) {
          throw new HttpException(BAD_REQUEST,e.getMessage());
        }

        if(geometry == null && refFeatureId == null && refSpaceId == null && h3Index ==null)
          throw new HttpException(BAD_REQUEST, "Invalid arguments! Define '"+Query.LAT+"' and '"+Query.LON+"' OR reference a '"+Query.REF_FEATURE_ID+"' in a '"+Query.REF_SPACE_ID+"'!");

        if(geometry != null)
          if(refFeatureId != null || refSpaceId != null || h3Index != null)
            throw new HttpException(BAD_REQUEST, "Invalid arguments! Define '"+Query.LAT+"' and '"+Query.LON+"' or reference a '"+Query.REF_FEATURE_ID+"' in a '"+Query.REF_SPACE_ID+"'!");
        if(refFeatureId != null || refSpaceId != null)
          if(refFeatureId == null || refSpaceId == null || h3Index != null || geometry != null)
            throw new HttpException(BAD_REQUEST, "Invalid arguments! Define '"+Query.LAT+"' and '"+Query.LON+"' or reference a '"+Query.REF_FEATURE_ID+"' in a '"+Query.REF_SPACE_ID+"'!");
        if(h3Index != null)
          if(refFeatureId != null || refSpaceId != null || geometry != null)
            throw new HttpException(BAD_REQUEST, "Invalid arguments! Define '"+Query.LAT+"' and '"+Query.LON+"' or reference a '"+Query.REF_FEATURE_ID+"' in a '"+Query.REF_SPACE_ID+"'!");
      } else if(context.request().method() == HttpMethod.POST) {
          geometry = getBodyAsGeometry(context);
      }

      final boolean skipCache = Query.getBoolean(context, SKIP_CACHE, false);
      final boolean force2D = Query.getBoolean(context, FORCE_2D, false);
      final SpaceContext spaceContext = getSpaceContext(context);
      final String author = Query.getString(context, Query.AUTHOR, null);

      GetFeaturesByGeometryEvent event = new GetFeaturesByGeometryEvent()
          .withGeometry(geometry)
          .withRadius(Query.getRadius(context))
          .withH3Index(Query.getH3Index(context))
          .withLimit(getLimit(context))
          .withTags(Query.getTags(context))
          .withClip(Query.getBoolean(context, Query.CLIP, false))
          .withPropertiesQuery(Query.getPropertiesQuery(context))
          .withSelection(Query.getSelection(context))
          .withForce2D(force2D)
          .withContext(spaceContext)
          .withAuthor(author);

      final GetFeaturesByGeometryTask task = new GetFeaturesByGeometryTask(event, context, ApiResponseType.FEATURE_COLLECTION, skipCache, refSpaceId, refFeatureId);
      task.execute(this::sendResponse, this::sendErrorResponse);
    } catch (HttpException e) {
      sendErrorResponse(context, e);
    }
  }

  /**
   * Retrieves the features in the bounding box.
   */
  private void getFeaturesByBBox(final RoutingContext context) {
    try {
      final boolean skipCache = Query.getBoolean(context, SKIP_CACHE, false);
      final boolean clip = Query.getBoolean(context, Query.CLIP, false);
      final boolean force2D = Query.getBoolean(context, FORCE_2D, false);
      final SpaceContext spaceContext = getSpaceContext(context);
      final String author = Query.getString(context, Query.AUTHOR, null);

      GetFeaturesByBBoxEvent event = (GetFeaturesByBBoxEvent) new GetFeaturesByBBoxEvent<>()
          .withForce2D(force2D)
          .withBbox(getBBox(context))
          .withClip(clip);

      try {
        event.withClusteringType(Query.getString(context, Query.CLUSTERING, null))
            .withClusteringParams(Query.getAdditionalParams(context,Query.CLUSTERING))
            .withTweakType(Query.getString(context, Query.TWEAKS, null))
            .withTweakParams(Query.getAdditionalParams(context, Query.TWEAKS))
            .withLimit(getLimit(context))
            .withTags(Query.getTags(context))
            .withPropertiesQuery(Query.getPropertiesQuery(context))
            .withSelection(Query.getSelection(context))
            .withAuthor(author)
            .withContext(spaceContext);
      } catch (Exception e) {
        throw new HttpException(BAD_REQUEST,e.getMessage());
      }

      final GetFeaturesByBBoxTask task = new GetFeaturesByBBoxTask(event, context, ApiResponseType.FEATURE_COLLECTION, skipCache);
      task.execute(this::sendResponse, this::sendErrorResponse);
    } catch (HttpException e) {
      sendErrorResponse(context, e);
    }
  }

  /**
   * Retrieves the features in a tile.
   */
  private void getFeaturesByTile(final RoutingContext context) {
    try {
      String tileId = context.pathParam(Path.TILE_ID),
             tileType = context.pathParam(Path.TILE_TYPE),
             acceptTypeSuffix = null;

      final boolean skipCache = Query.getBoolean(context, SKIP_CACHE, false);
      final boolean force2D = Query.getBoolean(context, FORCE_2D, false);
      final SpaceContext spaceContext = getSpaceContext(context);
      final String author = Query.getString(context, Query.AUTHOR, null);

      final int indexOfPoint = tileId.indexOf('.');
      if (indexOfPoint >= 0) {
        acceptTypeSuffix = tileId.substring(indexOfPoint + 1);
        tileId = tileId.substring(0, indexOfPoint);
      }

      ApiResponseType responseType = ApiResponseType.FEATURE_COLLECTION;

      if( context.parsedHeaders().accept().stream().map(ParsedHeaderValue::rawValue).anyMatch( APPLICATION_VND_MAPBOX_VECTOR_TILE::equals) )
       responseType = ApiResponseType.MVT;
      else if( acceptTypeSuffix != null )
       switch( acceptTypeSuffix.toLowerCase() )
       { case "mvt2"  :
         case "mvt"   : responseType = ApiResponseType.MVT; break;
         case "mvtf2" :
         case "mvtf"  : responseType = ApiResponseType.MVT_FLATTENED; break;
         default : break;
       }

      GetFeaturesByTileEvent event = new GetFeaturesByTileEvent();

      String optimMode = Query.getString(context, Query.OPTIM_MODE, "raw");

      try {
        event.withClip(Query.getBoolean(context, Query.CLIP, (responseType == ApiResponseType.MVT || responseType == ApiResponseType.MVT_FLATTENED || "viz".equals(optimMode) )))
            .withMargin(Query.getInteger(context, Query.MARGIN, 0))
            .withClusteringType(Query.getString(context, Query.CLUSTERING, null))
            .withClusteringParams(Query.getAdditionalParams(context, Query.CLUSTERING))
            .withTweakType(Query.getString(context, Query.TWEAKS, null))
            .withTweakParams(Query.getAdditionalParams(context, Query.TWEAKS))
            .withLimit(getLimit(context, ("viz".equals(optimMode) ? HARD_LIMIT : DEFAULT_FEATURE_LIMIT)))
            .withTags(Query.getTags(context))
            .withPropertiesQuery(Query.getPropertiesQuery(context))
            .withSelection(Query.getSelection(context))
            .withForce2D(force2D)
            .withOptimizationMode(optimMode)
            .withVizSampling(Query.getString(context, Query.OPTIM_VIZSAMPLING, "med"))
            .withResponseType(responseType == ApiResponseType.MVT ? MVT : responseType == ApiResponseType.MVT_FLATTENED ? MVT_FLATTENED : GEO_JSON)
            .withHereTileFlag("here".equals(tileType))
            .withContext(spaceContext)
            .withAuthor(author);
      } catch (Exception e) {
        throw new HttpException(BAD_REQUEST,e.getMessage());
      }

      try {
        WebMercatorTile tileAddress = null;
        HQuad hereTileAddress = null;
        if ("tms".equals(tileType)) {
          tileAddress = WebMercatorTile.forTMS(tileId);
        } else if ("web".equals(tileType)) {
          tileAddress = WebMercatorTile.forWeb(tileId);
        } else if ("quadkey".equals(tileType)) {
          tileAddress = WebMercatorTile.forQuadkey(tileId);
        } else if ("here".equals(tileType)) {
          if (tileId.contains("_")) {
            String[] levelRowColumnArray = tileId.split("_");
            if (levelRowColumnArray.length == 3) {
              hereTileAddress = new HQuad(
                  Integer.parseInt(levelRowColumnArray[1]),
                  Integer.parseInt(levelRowColumnArray[2]),
                  Integer.parseInt(levelRowColumnArray[0]));
            } else {
              throw new HttpException(BAD_REQUEST, "Invalid argument tileId.");
            }
          } else {
            hereTileAddress = new HQuad(tileId, Service.configuration.USE_BASE_4_H_TILES);
          }
        }

        if (tileAddress != null) {
          event.setBbox(tileAddress.getExtendedBBox((int) event.getMargin()));
          event.setLevel(tileAddress.level);
          event.setX(tileAddress.x);
          event.setY(tileAddress.y);
          event.setQuadkey(tileAddress.asQuadkey());
        } else if (hereTileAddress != null) {
          BBox bBox = hereTileAddress.getBoundingBox();
          event.setBbox(bBox);
          event.setLevel(hereTileAddress.level);
          event.setX(hereTileAddress.x);
          event.setY(hereTileAddress.y);
          event.setQuadkey(hereTileAddress.quadkey);
        }
      } catch (IllegalArgumentException e) {
        throw new HttpException(BAD_REQUEST, "Invalid argument tileId.");
      }

      final TileQuery task = new TileQuery(event, context, responseType, skipCache);
      task.execute(this::sendResponse, this::sendErrorResponse);

    } catch (HttpException e) {
      sendErrorResponse(context, e);
    }
  }

  /**
   * Checks the query string for an EPSG code, when found, it passes and returns it. If not found, it will return the provided default
   * value.
   */
  private int getVerifiedEpsg(RoutingContext context) throws HttpException {

    List<String> epsgParam = Query.queryParam(Query.EPSG, context);

    int epsg = 3785;
    if (epsgParam.size() > 0) {
      try {
        epsg = Integer.parseInt(epsgParam.get(0));
        if (epsg < 0) {
          throw new HttpException(BAD_REQUEST, "Invalid EPSG code provided.");
        }
      } catch (NumberFormatException e) {
        throw new HttpException(BAD_REQUEST, "Invalid EPSG code provided. Expected was an integer value.");
      }
    }

    try {
      if (GeoTools.mathTransform(GeoTools.WGS84_EPSG, "EPSG:" + epsg) != null) {
        return epsg;
      }
    } catch (Exception ignored) {
    }
    throw new HttpException(BAD_REQUEST, "Invalid or unsupported EPSG code provided, in doubt please use 3785");
  }


  /**
   * Parses the provided latitude and longitude values as a bounding box.
   */
  private BBox getBBox(RoutingContext context) throws HttpException {
    final List<String> allWest = Query.queryParam(Query.WEST, context);
    if (allWest.size() != 1) {
      throw new HttpException(BAD_REQUEST,
          "Invalid '" + Query.WEST + "' query parameter, expected exactly one value.");
    }

    final List<String> allNorth = Query.queryParam(Query.NORTH, context);
    if (allNorth.size() != 1) {
      throw new HttpException(BAD_REQUEST,
          "Invalid '" + Query.NORTH + "' query parameter, expected exactly one value.");
    }

    final List<String> allEast = Query.queryParam(Query.EAST, context);
    if (allEast.size() != 1) {
      throw new HttpException(BAD_REQUEST,
          "Invalid '" + Query.EAST + "' query parameter, expected exactly one value.");
    }

    final List<String> allSouth = Query.queryParam(Query.SOUTH, context);
    if (allSouth.size() != 1) {
      throw new HttpException(BAD_REQUEST,
          "Invalid '" + Query.SOUTH + "' query parameter, expected exactly one value.");
    }

    final double west, north, east, south;
    try {
      west = Double.parseDouble(allWest.get(0));
      if (west < -180 || west > 180) {
        throw new NumberFormatException();
      }
    } catch (NumberFormatException e) {
      throw new HttpException(BAD_REQUEST,
          "Invalid '" + Query.WEST
              + "' query parameter, must be a WGS'84 longitude in decimal degree (so a value between -180 and +180).");
    }

    try {
      north = Double.parseDouble(allNorth.get(0));
      if (north < -90 || north > 90) {
        throw new NumberFormatException();
      }
    } catch (NumberFormatException e) {
      throw new HttpException(BAD_REQUEST,
          "Invalid '" + Query.NORTH + "' query parameter, must be a WGS'84 latitude in decimal degree (so a value between -90 and +90).");
    }

    try {
      east = Double.parseDouble(allEast.get(0));
      if (east < -180 || east > 180) {
        throw new NumberFormatException();
      }
    } catch (NumberFormatException e) {
      throw new HttpException(BAD_REQUEST,
          "Invalid '" + Query.EAST
              + "' query parameter, must be a WGS'84 longitude in decimal degree (so a value between -180 and +180).");
    }

    try {
      south = Double.parseDouble(allSouth.get(0));
      if (south < -90 || south > 90) {
        throw new NumberFormatException();
      }
    } catch (NumberFormatException e) {
      throw new HttpException(BAD_REQUEST,
          "Invalid '" + Query.SOUTH + "' query parameter, must be a WGS'84 latitude in decimal degree (so a value between -90 and +90).");
    }

    return new BBox(west, south, east, north);
  }

  /**
   * Parses the body of the request as a FeatureCollection or a Feature object and returns the features as a list.
   */
  private Geometry getBodyAsGeometry(final RoutingContext context) throws HttpException {
    final Marker logMarker = Context.getMarker(context);
    try {
      final String text = context.getBodyAsString();
      if (text == null) {
        throw new HttpException(BAD_REQUEST, "Missing content");
      }

      final Typed input = XyzSerializable.deserialize(text);
      Geometry geometry;
      if (input instanceof Geometry) {
        geometry = (Geometry) input;
        geometry.validate();
      } else {
        throw new HttpException(BAD_REQUEST,
            "The provided content is of type '" + input.getClass().getSimpleName() + "'. Expected is a GeoJson-Geometry [Point,MultiPoint,LineString,MultiLineString,Polygon,MultiPolygon].");
      }

      return geometry;
    } catch (JsonMappingException e) {
      throw new HttpException(BAD_REQUEST, "Invalid JSON type. Expected is a GeoJson-Geometry [Point,MultiPoint,LineString,MultiLineString,Polygon,MultiPolygon].");
    } catch (JsonParseException e) {
      throw new HttpException(BAD_REQUEST,
          "Invalid JSON string. Error at line " + e.getLocation().getLineNr() + ", column " + e.getLocation().getColumnNr() + ".");
    } catch (IOException e) {
      throw new HttpException(BAD_REQUEST, "Cannot read input JSON string.");
    } catch (InvalidGeometryException e) {
      throw new HttpException(BAD_REQUEST, "Geometry isn't valid!");
    }
  }
}
