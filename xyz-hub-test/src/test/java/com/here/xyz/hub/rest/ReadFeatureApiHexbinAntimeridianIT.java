/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

import static com.here.xyz.hub.auth.TestAuthenticator.TEST_SUFFIX;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_GEO_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static io.restassured.path.json.config.JsonPathConfig.NumberReturnType.DOUBLE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.wdtinc.mapbox_vector_tile.adapt.jts.MvtReader;
import com.wdtinc.mapbox_vector_tile.adapt.jts.TagKeyValueMapConverter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.model.JtsLayer;
import io.restassured.path.json.JsonPath;
import io.restassured.path.json.config.JsonPathConfig;
import io.restassured.response.Response;
import io.restassured.response.ValidatableResponse;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Hexbin clustering at the antimeridian: the cell 857eb3b3fffffff (resolution 5) is split at the antimeridian into two parts of about
 * equal size. It contains two points on each side.
 */
@Category(RestTests.class)
public class ReadFeatureApiHexbinAntimeridianIT extends TestSpaceWithFeature {
  private static final String SPACE_ID = "x-hexbin-am" + TEST_SUFFIX;
  private static final String SPLIT_CELL = "857eb3b3fffffff";
  private static final String CELL = "features.find { it.properties.kind_detail == '" + SPLIT_CELL + "' }";
  private static String layerId;

  @BeforeClass
  public static void setup() {
    removeSpace(SPACE_ID);
    layerId = createSpaceWithCustomStorage(SPACE_ID, "psql", null);
    postFeatures(SPACE_ID, new FeatureCollection().withFeatures(List.of(
        point("am-east-1", 179.97, 1.146, 1),
        point("am-east-2", 179.96, 1.15, 2),
        point("am-west-1", -179.97, 1.146, 3),
        point("am-west-2", -179.96, 1.15, 4),
        point("far-away", 8.5131, 50.2141, 5))), AuthProfile.ACCESS_ALL);
  }

  @AfterClass
  public static void tearDown() {
    removeSpace(SPACE_ID);
  }

  @Test
  public void bboxTouching180() {
    get("/bbox?west=179&south=0&east=180&north=2&clustering=hexbin&clustering.absoluteResolution=5")
        .body(CELL + ".geometry.type", equalTo("MultiPolygon"))
        .body(CELL + ".properties.aggregation.qty", equalTo(4));
  }

  @Test
  public void bboxTouchingMinus180() {
    get("/bbox?west=-180&south=0&east=-179&north=2&clustering=hexbin&clustering.absoluteResolution=5")
        .body(CELL + ".geometry.type", equalTo("MultiPolygon"))
        .body(CELL + ".properties.aggregation.qty", equalTo(4));
  }

  @Test
  public void pointModeReturnsTheCellCenter() {
    //h3_cell_to_geometry() of the cell, not the centroid of its split boundary
    JsonPath body = get("/bbox?west=179&south=0&east=180&north=2&clustering=hexbin&clustering.absoluteResolution=5"
        + "&clustering.pointmode=true")
        .body(CELL + ".geometry.type", equalTo("Point"))
        .body(CELL + ".properties.aggregation.qty", equalTo(4))
        //Doubles, the default (floats) is not precise enough
        .extract().jsonPath(JsonPathConfig.jsonPathConfig().numberReturnType(DOUBLE));
    assertThat(body.getDouble(CELL + ".geometry.coordinates[0]"), closeTo(179.9979715, 1e-6));
    assertThat(body.getDouble(CELL + ".geometry.coordinates[1]"), closeTo(1.1466468, 1e-6));
  }

  @Test
  public void geoJsonTileAtTheEdge() {
    //GeoJSON tiles are not clipped by default, so the cell keeps both parts
    for (String tile : new String[]{"7_127_63", "7_0_63"})
      get("/tile/web/" + tile + "?clustering=hexbin")
          .body(CELL + ".geometry.type", equalTo("MultiPolygon"))
          .body(CELL + ".properties.aggregation.qty", equalTo(4));
  }

  @Test
  public void mvtTileAtTheEdge() throws IOException {
    //MVT tiles are clipped, so the cell is the part on the side of the tile
    for (String tile : new String[]{"7_127_63", "7_0_63"}) {
      Geometry cell = mvtGeometry(tile, SPLIT_CELL);
      assertEquals(tile, "Polygon", cell.getGeometryType());
      assertEquals(tile, 4, qtyOf(cell));
    }
  }

  @Test
  public void bboxAwayFromTheAntimeridian() {
    get("/bbox?west=8.5&south=50.2&east=8.53&north=50.23&clustering=hexbin&clustering.absoluteResolution=7")
        .body("features.size()", equalTo(1))
        .body("features[0].properties.kind_detail", equalTo("871faeba8ffffff"))
        .body("features[0].properties.aggregation.qty", equalTo(1))
        .body("features[0].geometry.type", equalTo("Polygon"));
  }

  private static ValidatableResponse get(String path) {
    return given()
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID + path)
        .then()
        .statusCode(OK.code());
  }

  //The geometry of the cell in the hexbin MVT tile
  private static Geometry mvtGeometry(String tile, String h3Index) throws IOException {
    Response response = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID + "/tile/web/" + tile + ".mvt?clustering=hexbin");
    response.then().statusCode(OK.code());
    //The default ring classifier of the reader (V2.1) drops the polygons that ST_AsMVTGeom() returns, the V1 classifier keeps them
    JtsLayer layer = MvtReader.loadMvt(response.getBody().asInputStream(), new GeometryFactory(), new TagKeyValueMapConverter(),
        MvtReader.RING_CLASSIFIER_V1).getLayer(layerId);
    assertNotNull(tile + ": the layer is missing", layer);
    for (Geometry geometry : new ArrayList<>(layer.getGeometries()))
      if (h3Index.equals(((Map<String, Object>) geometry.getUserData()).get("kind_detail")))
        return geometry;
    throw new AssertionError(tile + ": the cell " + h3Index + " is missing");
  }

  //Objects are encoded as JSON text in MVT
  private static int qtyOf(Geometry geometry) {
    return new JsonObject((String) ((Map<String, Object>) geometry.getUserData()).get("aggregation")).getInteger("qty");
  }

  private static Feature point(String id, double lon, double lat, int value) {
    return new Feature()
        .withId(id)
        .withGeometry(new Point().withCoordinates(new PointCoordinates(lon, lat)))
        .withProperties(new Properties().with("value", value));
  }
}
