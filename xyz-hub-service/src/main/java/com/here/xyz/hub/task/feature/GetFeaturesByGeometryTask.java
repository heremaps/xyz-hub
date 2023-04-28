package com.here.xyz.hub.task.feature;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.feature.GetFeaturesByGeometryEvent;
import com.here.xyz.exceptions.ParameterError;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.XyzResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Search using the geometry of a reference feature from a reference space. So basically, search all features in the current space, in range
 * of the given reference feature in the given reference space.
 */
public final class GetFeaturesByGeometryTask extends AbstractSpatialQueryTask<GetFeaturesByGeometryEvent> {

  public GetFeaturesByGeometryTask(@Nullable String streamId) {
    super(streamId);
  }

  private Space refSpace;
  private String refFeatureId;

  private static final String ARG_ERROR_MSG = "Invalid arguments. "
      + "Define either 'lat' and 'lon', refer a feature in another space via 'refFeatureId' and 'refSpaceId' or address a hex via 'h3Index'.";

  @Override
  public void initFromRoutingContext(@NotNull RoutingContext routingContext, @NotNull ApiResponseType responseType) throws ParameterError {
    super.initFromRoutingContext(routingContext, responseType);
    assert queryParameters != null;

    // Loading of the features delayed to execute to not block the IO thread!
    final String refFeatureId = queryParameters.getRefFeatureId();
    final String refSpaceId = queryParameters.getRefSpaceId();
    final String h3Index = queryParameters.getH3Index();
    final Geometry geometry = getBodyAsGeometry();

    if (geometry == null && refFeatureId == null && refSpaceId == null && h3Index == null) {
      throw new ParameterError(ARG_ERROR_MSG);
    }

    if (geometry != null) {
      if (refFeatureId != null || refSpaceId != null || h3Index != null) {
        throw new ParameterError(ARG_ERROR_MSG);
      }
      event.setGeometry(geometry);
    } else if (h3Index != null) {
      if (refFeatureId != null || refSpaceId != null) {
        throw new ParameterError(ARG_ERROR_MSG);
      }
      event.setH3Index(h3Index);
    } else {
      if (refFeatureId == null || refSpaceId == null) {
        throw new ParameterError(ARG_ERROR_MSG);
      }
      this.refSpace = Space.getById(refSpaceId);
      this.refFeatureId = refFeatureId;
      if (refSpace == null) {
        throw new ParameterError("The referred space '" + refSpaceId + "' does not exist");
      }
    }
  }

  @Override
  public @NotNull GetFeaturesByGeometryEvent createEvent() {
    return new GetFeaturesByGeometryEvent();
  }

  /**
   * Parses the body of the request as FeatureCollection, or Feature object and returns the features as a list.
   *
   * @throws ParameterError If the method is POST and any occurred while decoding the body.
   */
  private Geometry getBodyAsGeometry() throws ParameterError {
    try {
      assert routingContext != null;
      if (routingContext.request().method() != HttpMethod.POST) {
        return null;
      }

      final String text = routingContext.body().asString(StandardCharsets.UTF_8.name());
      if (text == null) {
        throw new ParameterError("Missing content");
      }

      final Typed input = XyzSerializable.deserialize(text);
      Geometry geometry;
      if (input instanceof Geometry) {
        geometry = (Geometry) input;
        geometry.validate();
      } else if (input == null) {
        throw new ParameterError("The content is null");
      } else {
        throw new ParameterError("The provided content is of type '"
            + input.getClass().getSimpleName()
            + "'. Expected is a GeoJson-Geometry [Point,MultiPoint,LineString,MultiLineString,Polygon,MultiPolygon].");
      }
      return geometry;
    } catch (JsonMappingException e) {
      throw new ParameterError(
          "Invalid JSON type. Expected is a GeoJson-Geometry [Point,MultiPoint,LineString,MultiLineString,Polygon,MultiPolygon].");
    } catch (JsonParseException e) {
      throw new ParameterError(
          "Invalid JSON string. Error at line " + e.getLocation().getLineNr() + ", column " + e.getLocation().getColumnNr() + ".");
    } catch (IOException e) {
      throw new ParameterError("Cannot read input JSON string.");
    } catch (InvalidGeometryException e) {
      info("Invalid geometry found", e);
      throw new ParameterError("Invalid geometry.");
    }
  }

  @Override
  protected @NotNull XyzResponse execute() throws Exception {
    if (refSpace != null) {
      if (!refSpace.getId().equals(getSpace().getId())) {
        // TODO: Implement this by creating a new task that loads the referred feature (refFeatureId) and then uses the geometry of
        //       this feature in event.setGeometry().
        throw new UnsupportedOperationException("Referring a feature in another space is not yet supported");
      } else {
        // The referred feature located in the same space, leave this to the connector.
        event.setRef(refFeatureId);
      }
    }
    return super.execute();
  }
}
