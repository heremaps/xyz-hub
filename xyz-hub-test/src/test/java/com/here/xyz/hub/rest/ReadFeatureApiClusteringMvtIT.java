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
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
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
import com.wdtinc.mapbox_vector_tile.adapt.jts.model.JtsMvt;
import io.restassured.response.Response;
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
 * MVT tiles of the hexbin and quadbin clustering of a PSQL storage.
 */
@Category(RestTests.class)
public class ReadFeatureApiClusteringMvtIT extends TestSpaceWithFeature {
  private static final String SPACE_ID = "x-clustering-mvt" + TEST_SUFFIX;
  //The web tile 7_67_43 contains the point, which is in the cell 851faebbfffffff of resolution 5 (the one of level 7)
  private static final String TILE = "7_67_43";
  private static String layerId;

  @BeforeClass
  public static void setup() {
    removeSpace(SPACE_ID);
    layerId = createSpaceWithCustomStorage(SPACE_ID, "psql", null);
    postFeatures(SPACE_ID, new FeatureCollection().withFeatures(List.of(new Feature()
        .withId("point")
        .withGeometry(new Point().withCoordinates(new PointCoordinates(8.5131, 50.2141)))
        .withProperties(new Properties()))), AuthProfile.ACCESS_ALL);
  }

  @AfterClass
  public static void tearDown() {
    removeSpace(SPACE_ID);
  }

  @Test
  public void hexbinMvtTile() throws IOException {
    List<Geometry> geometries = mvtGeometries("hexbin");
    assertEquals(1, geometries.size());
    Map<String, Object> properties = (Map<String, Object>) geometries.get(0).getUserData();
    assertEquals("851faebbfffffff", properties.get("kind_detail"));
    //Objects are encoded as JSON text in MVT
    assertEquals(1, (int) new JsonObject((String) properties.get("aggregation")).getInteger("qty"));
    assertEquals("Polygon", geometries.get(0).getGeometryType());
  }

  @Test
  public void quadbinMvtTile() throws IOException {
    List<Geometry> geometries = mvtGeometries("quadbin");
    assertEquals(1, geometries.size());
    assertEquals(1L, ((Map<String, Object>) geometries.get(0).getUserData()).get("count"));
    assertEquals("Polygon", geometries.get(0).getGeometryType());
  }

  private static List<Geometry> mvtGeometries(String clustering) throws IOException {
    Response response = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID + "/tile/web/" + TILE + ".mvt?clustering=" + clustering);
    response.then().statusCode(OK.code());
    //The default ring classifier of the reader (V2.1) drops the polygons that ST_AsMVTGeom() returns, the V1 classifier keeps them
    JtsMvt mvt = MvtReader.loadMvt(response.getBody().asInputStream(), new GeometryFactory(), new TagKeyValueMapConverter(),
        MvtReader.RING_CLASSIFIER_V1);
    //Hexbin names the layer after the space, quadbin after the table of the space
    JtsLayer layer = "hexbin".equals(clustering) ? mvt.getLayer(layerId) : mvt.getLayers().stream().findFirst().orElse(null);
    assertNotNull("The layer is missing", layer);
    assertEquals(1, mvt.getLayers().size());
    return new ArrayList<>(layer.getGeometries());
  }
}
