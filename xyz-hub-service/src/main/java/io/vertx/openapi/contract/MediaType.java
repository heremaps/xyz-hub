package io.vertx.openapi.contract;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.json.schema.JsonSchema;

import java.util.Arrays;
import java.util.List;

/**
 * This interface represents the most important attributes of an OpenAPI Operation.
 * <br>
 * <a href="https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.1.0.md#media-type-Object">Operation V3.1</a>
 * <br>
 * <a href="https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md#media-type-Object">Operation V3.0</a>
 */
public interface MediaType extends OpenAPIObject {

  String APPLICATION_JSON = HttpHeaderValues.APPLICATION_JSON.toString();
  String APPLICATION_JSON_UTF8 = APPLICATION_JSON + "; charset=utf-8";
  List<String> SUPPORTED_MEDIA_TYPES = Arrays.asList(APPLICATION_JSON,
      APPLICATION_JSON_UTF8,
      "application/vnd.here.changeset-collection",
      "application/vnd.here.changeset",
      "application/vnd.here.compact-changeset",
      "application/geo+json",
      "application/vnd.mapbox-vector-tile",
      "application/vnd.here.feature-modification-list");

  static boolean isMediaTypeSupported(String type) {
    return SUPPORTED_MEDIA_TYPES.contains(type.toLowerCase());
  }

  /**
   * @return the schema defining the content of the request.
   */
  JsonSchema getSchema();

  /**
   * @return the identifier like <i>application/json</i>
   */
  String getIdentifier();
}