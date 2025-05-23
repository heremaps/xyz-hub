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

import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.GEO_JSON;
import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.MVT;
import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.MVT_FLATTENED;
import static com.here.xyz.hub.rest.ApiParam.Query.FAST_MODE;
import static com.here.xyz.hub.rest.ApiParam.Query.FORCE_2D;
import static com.here.xyz.hub.rest.ApiParam.Query.SKIP_CACHE;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_VND_MAPBOX_VECTOR_TILE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.task.FeatureTask;
import com.here.xyz.hub.task.FeatureTask.BBoxQuery;
import com.here.xyz.hub.task.FeatureTask.GeometryQuery;
import com.here.xyz.hub.task.FeatureTask.GetStatistics;
import com.here.xyz.hub.task.FeatureTask.IterateQuery;
import com.here.xyz.hub.task.FeatureTask.SearchQuery;
import com.here.xyz.hub.task.FeatureTask.TileQuery;
import com.here.xyz.util.geo.GeoTools;
import com.here.xyz.models.geojson.HQuad;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.util.geo.GeometryValidator;
import com.here.xyz.util.service.HttpException;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.ParsedHeaderValue;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import java.io.IOException;
import java.util.List;

public class FeatureQueryApi extends SpaceBasedApi {

  public FeatureQueryApi(RouterBuilder rb) {
    rb.getRoute("getFeaturesBySpatial").setDoValidation(false).addHandler(this::getFeaturesBySpatial);
    rb.getRoute("getFeaturesBySpatialPost").setDoValidation(false).addHandler(this::getFeaturesBySpatial);
    rb.getRoute("getFeaturesByBBox").setDoValidation(false).addHandler(this::getFeaturesByBBox);
    rb.getRoute("getFeaturesByTile").setDoValidation(false).addHandler(this::getFeaturesByTile);
    rb.getRoute("getStatistics").setDoValidation(false).addHandler(this::getStatistics);
    rb.getRoute("iterateFeatures").setDoValidation(false).addHandler(this::iterateFeatures);
    rb.getRoute("searchForFeatures").setDoValidation(false).addHandler(this::searchForFeatures);
  }

  /**
   * Retrieves the statistics about a space.
   */
  private void getStatistics(final RoutingContext context) {
    try {
      new GetStatistics(new GetStatisticsEvent()
                .withContext(getSpaceContext(context))
                .withFastMode(Query.getBoolean(context, FAST_MODE, false)),
              context,
              ApiResponseType.STATISTICS_RESPONSE, Query.getBoolean(context, SKIP_CACHE, false)
      ).execute(this::sendResponse, this::sendErrorResponse);
    } catch (HttpException e) {
      sendErrorResponse(context, e);
    }
  }

  /**
   * Searches for features by tags.
   */
  private void searchForFeatures(final RoutingContext context) {
    try {
      final boolean skipCache = Query.getBoolean(context, SKIP_CACHE, false);
      final boolean force2D = Query.getBoolean(context, FORCE_2D, false);
      final PropertiesQuery propertiesQuery = getPropertiesQuery(context);
      final SpaceContext spaceContext = getSpaceContext(context);
      final String author = Query.getString(context, Query.AUTHOR, null);

      final SearchForFeaturesEvent event = new SearchForFeaturesEvent();
      event.withPropertiesQuery(propertiesQuery)
          .withLimit(getLimit(context))
          .withRef(getRef(context))
          .withForce2D(force2D)
          .withSelection(Query.getSelection(context))
          .withContext(spaceContext)
          .withAuthor(author);

      final SearchQuery task = new SearchQuery(event, context, ApiResponseType.FEATURE_COLLECTION, skipCache);
      task.execute(this::sendResponse, this::sendErrorResponse);
    } catch (HttpException e) {
      sendErrorResponse(context, e);
    }
  }

  private PropertiesQuery getPropertiesQuery(final RoutingContext context) {
    final PropertiesQuery propertiesQuery = Query.getPropertiesQuery(context);

    if (propertiesQuery == null)
      return null;

    return propertiesQuery.filterOutNamedProperty(Query.F_PREFIX + Query.VERSION, Query.F_PREFIX + Query.AUTHOR);
  }

  /**
   * Iterate the content of the space.
   */
  private void iterateFeatures(final RoutingContext context) {
    try {
      final boolean skipCache = Query.getBoolean(context, SKIP_CACHE, false);
      final boolean force2D = Query.getBoolean(context, FORCE_2D, false);
      final SpaceContext spaceContext = getSpaceContext(context);
      final Integer v = Query.getInteger(context, Query.VERSION, null);
      final Ref ref = getRef(context);

      List<String> sort = Query.getSort(context);
      PropertiesQuery propertiesQuery = Query.getPropertiesQuery(context);
      String handle = Query.getString(context, Query.HANDLE, null);
      Integer[] part = Query.getPart(context);

      //TODO: Streamline the following IterateFeaturesEvent creation
      if (sort != null || propertiesQuery != null || part != null || ( handle != null && handle.startsWith("h07~"))) {
        IterateFeaturesEvent event = new IterateFeaturesEvent();
        event.withLimit(getLimit(context))
            .withForce2D(force2D)
            .withSelection(Query.getSelection(context))
            .withSort(sort)
            .withPart(part)
            .withHandle(handle)
            .withContext(spaceContext)
            .withRef(ref)
            .withV(v);

        final SearchQuery task = new SearchQuery(event, context, ApiResponseType.FEATURE_COLLECTION, skipCache);
        task.execute(this::sendResponse, this::sendErrorResponse);
        return;
      }

      IterateFeaturesEvent event = new IterateFeaturesEvent()
          .withLimit(getLimit(context))
          .withForce2D(force2D)
          .withSelection(Query.getSelection(context))
          .withRef(ref)
          .withV(v)
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

      GetFeaturesByGeometryEvent event = new GetFeaturesByGeometryEvent()
          .withGeometry(geometry)
          .withRadius(Query.getRadius(context))
          .withH3Index(Query.getH3Index(context))
          .withLimit(getLimit(context))
          .withClip(Query.getBoolean(context, Query.CLIP, false))
          .withPropertiesQuery(Query.getPropertiesQuery(context))
          .withSelection(Query.getSelection(context))
          .withForce2D(force2D)
          .withContext(spaceContext)
          .withRef(getRef(context));

      try {
        //If a h3 reference got provided - we do not need to validate the Geometry
        //If there is a referenced feature - the geometry validation happens in FeatureTask - after the geometry is resolved.
        if(h3Index == null && refFeatureId == null && refSpaceId == null)
          GeometryValidator.validateGeometry(event.getGeometry(), event.getRadius());
      }catch (GeometryValidator.GeometryException e){
        throw new HttpException(BAD_REQUEST ,e.getMessage());
      }

      final GeometryQuery task = new GeometryQuery(event, context, ApiResponseType.FEATURE_COLLECTION, skipCache, refSpaceId, refFeatureId);
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

      GetFeaturesByBBoxEvent event = (GetFeaturesByBBoxEvent) new GetFeaturesByBBoxEvent<>()
          .withForce2D(force2D)
          .withBbox(getBBox(context))
          .withClip(clip);

      try {
        event.withClusteringType(Query.getString(context, Query.CLUSTERING, null))
            .withClusteringParams(Query.getAdditionalParams(context,Query.CLUSTERING))
            .withTweakType(Query.getString(context, Query.TWEAKS, null))
            .withTweakParams(Query.getAdditionalParams(context, Query.TWEAKS))
            .withPropertiesQuery(Query.getPropertiesQuery(context))
            .withLimit(getLimit(context))
            .withSelection(Query.getSelection(context))
            .withRef(getRef(context))
            .withContext(spaceContext);
      } catch (Exception e) {
        throw new HttpException(BAD_REQUEST,e.getMessage());
      }

      final BBoxQuery task = new FeatureTask.BBoxQuery(event, context, ApiResponseType.FEATURE_COLLECTION, skipCache);
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

      final int indexOfPoint = tileId.indexOf('.');
      if (indexOfPoint >= 0) {
        acceptTypeSuffix = tileId.substring(indexOfPoint + 1);
        tileId = tileId.substring(0, indexOfPoint);
      }

      ApiResponseType responseType = ApiResponseType.FEATURE_COLLECTION;

      if( context.parsedHeaders().accept().stream().map(ParsedHeaderValue::value).anyMatch( APPLICATION_VND_MAPBOX_VECTOR_TILE::equals) )
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
            .withPropertiesQuery(Query.getPropertiesQuery(context))
            .withSelection(Query.getSelection(context))
            .withForce2D(force2D)
            .withOptimizationMode(optimMode)
            .withVizSampling(Query.getString(context, Query.OPTIM_VIZSAMPLING, "med"))
            .withResponseType(responseType == ApiResponseType.MVT ? MVT : responseType == ApiResponseType.MVT_FLATTENED ? MVT_FLATTENED : GEO_JSON)
            .withHereTileFlag("here".equals(tileType))
            .withContext(spaceContext)
            .withRef(getRef(context));
      } catch (Exception e) {
        throw new HttpException(BAD_REQUEST,e.getMessage());
      }

      try {
        WebMercatorTile tileAddress = null;
        HQuad hereTileAddress = null;

        switch( tileType ) {
         case "tms"     : tileAddress = WebMercatorTile.forTMS(tileId); break;
         case "web"     : tileAddress = WebMercatorTile.forWeb(tileId); break;
         case "quadkey" : tileAddress = WebMercatorTile.forQuadkey(tileId); break;
         case "here" : 
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
          break;
         
         default:
          throw new HttpException(BAD_REQUEST, String.format("Invalid path argument {type} of tile request '%s' != [tms,web,quadkey,here]",tileType));
        }

        if (tileAddress != null) {
          event.setBbox(tileAddress.getExtendedBBox(event.getMargin()));
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
        else
         throw new IllegalArgumentException();

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
    try {
      final String text = context.body().asString();
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
