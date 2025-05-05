package io.vertx.openapi.contract;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.json.schema.JsonSchema;

import java.util.Arrays;
import java.util.List;

public interface MediaType extends OpenAPIObject {

  String APPLICATION_HAL_JSON = "application/hal+json";
  String APPLICATION_JSON = HttpHeaderValues.APPLICATION_JSON.toString();
  String APPLICATION_JSON_UTF8 = APPLICATION_JSON + "; charset=utf-8";
  String MULTIPART_FORM_DATA = HttpHeaderValues.MULTIPART_FORM_DATA.toString();
  List<String> SUPPORTED_MEDIA_TYPES = Arrays.asList(
      APPLICATION_JSON,
      APPLICATION_JSON_UTF8,
      MULTIPART_FORM_DATA,
      APPLICATION_HAL_JSON,
      "application/vnd.here.changeset-collection",
      "application/vnd.here.changeset",
      "application/vnd.here.compact-changeset",
      "application/geo+json",
      "application/vnd.mapbox-vector-tile",
      "application/vnd.here.feature-modification-list",
      "application/x-empty",
      "text/plain");

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