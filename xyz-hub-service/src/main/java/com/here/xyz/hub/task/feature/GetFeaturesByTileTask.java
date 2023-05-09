package com.here.xyz.hub.task.feature;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_VND_MAPBOX_VECTOR_TILE;

import com.here.xyz.events.feature.GetFeaturesByTileEvent;
import com.here.xyz.events.feature.GetFeaturesByTileResponseType;
import com.here.xyz.exceptions.ParameterError;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.models.geojson.HQuad;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import io.vertx.ext.web.MIMEHeader;
import io.vertx.ext.web.ParsedHeaderValue;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class GetFeaturesByTileTask extends AbstractGetFeaturesByBBoxTask<GetFeaturesByTileEvent> {

  public GetFeaturesByTileTask(@Nullable String streamId) {
    super(streamId);
  }

  @Override
  protected void setResponseType(@NotNull ApiResponseType responseType) {
    super.setResponseType(responseType);
    if (responseType.tileResponseType != null) {
      event.setResponseType(responseType.tileResponseType);
    } else {
      event.setResponseType(GetFeaturesByTileResponseType.GEO_JSON);
    }
  }

  @Override
  public void initEventFromRoutingContext(@NotNull RoutingContext routingContext, @NotNull ApiResponseType responseType) throws ParameterError {
    super.initEventFromRoutingContext(routingContext, responseType);
    assert queryParameters != null;

    String tileId = routingContext.pathParam(Path.TILE_ID);
    String tileType = routingContext.pathParam(Path.TILE_TYPE);
    String acceptTypeSuffix = null;
    final int indexOfPoint = tileId.indexOf('.');
    if (indexOfPoint >= 0) {
      acceptTypeSuffix = tileId.substring(indexOfPoint + 1);
      tileId = tileId.substring(0, indexOfPoint);
    }
    final List<MIMEHeader> acceptHeaders = routingContext.parsedHeaders().accept();
    if (acceptHeaders.stream().map(ParsedHeaderValue::rawValue).anyMatch(APPLICATION_VND_MAPBOX_VECTOR_TILE::equals)) {
      setResponseType(ApiResponseType.MVT);
    } else if (acceptTypeSuffix != null) {
      switch (acceptTypeSuffix.toLowerCase()) {
        case "mvt2":
        case "mvt":
          setResponseType(ApiResponseType.MVT);
          break;
        case "mvtf2":
        case "mvtf":
          setResponseType(ApiResponseType.MVT_FLATTENED);
          break;
      }
    }

    event.setHereTileFlag("here".equals(tileType));
    final HQuad hereTileAddress;
    final WebMercatorTile tileAddress;
    if ("tms".equals(tileType)) {
      tileAddress = WebMercatorTile.forTMS(tileId);
      hereTileAddress = null;
    } else if ("web".equals(tileType)) {
      tileAddress = WebMercatorTile.forWeb(tileId);
      hereTileAddress = null;
    } else if ("quadkey".equals(tileType)) {
      tileAddress = WebMercatorTile.forQuadkey(tileId);
      hereTileAddress = null;
    } else if ("here".equals(tileType)) {
      if (tileId.contains("_")) {
        String[] levelRowColumnArray = tileId.split("_");
        if (levelRowColumnArray.length == 3) {
          tileAddress = null;
          hereTileAddress = new HQuad(
              Integer.parseInt(levelRowColumnArray[1]),
              Integer.parseInt(levelRowColumnArray[2]),
              Integer.parseInt(levelRowColumnArray[0]));
        } else {
          throw new ParameterError("Invalid argument 'tileId': " + tileId);
        }
      } else {
        tileAddress = null;
        hereTileAddress = new HQuad(tileId, Service.configuration.USE_BASE_4_H_TILES);
      }
    } else {
      throw new ParameterError("Unknown tile type: " + tileType);
    }

    if (tileAddress != null) {
      event.setBbox(tileAddress.getExtendedBBox(event.getMargin()));
      event.setLevel(tileAddress.level);
      event.setX(tileAddress.x);
      event.setY(tileAddress.y);
      event.setQuadkey(tileAddress.asQuadkey());
    } else {
      final BBox bBox = hereTileAddress.getBoundingBox();
      event.setBbox(bBox);
      event.setLevel(hereTileAddress.level);
      event.setX(hereTileAddress.x);
      event.setY(hereTileAddress.y);
      event.setQuadkey(hereTileAddress.quadkey);
    }
  }

  @Override
  public @NotNull GetFeaturesByTileEvent createEvent() {
    return new GetFeaturesByTileEvent();
  }
}