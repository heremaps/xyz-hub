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

package com.here.xyz.psql;

import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.MVT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.util.Random;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * MVT tiles of the hexbin and quadbin clustering.
 */
public class PSQLClusteringMvtIT extends PSQLAbstractIT {
  private static final String SPACE = PSQLClusteringMvtIT.class.getSimpleName().toLowerCase() + "_" + Random.randomAlpha(5).toLowerCase();
  //The web tile 7/67/43 contains the point
  private static final int LEVEL = 7, X = 67, Y = 43;

  @BeforeAll
  public static void createFixture() throws Exception {
    invokeCreateTestSpace(defaultTestConnectorParams, SPACE);
    assertNotNull(deserializeResponse(invokeLambda(new ModifyFeaturesEvent()
        .withConnectorParams(defaultTestConnectorParams)
        .withSpace(SPACE)
        .withTransaction(true)
        //The handler modifies the list
        .withInsertFeatures(new ArrayList<>(List.of(new Feature()
            .withId("point")
            .withGeometry(new Point().withCoordinates(new PointCoordinates(8.5131, 50.2141)))
            .withProperties(new Properties().withXyzNamespace(new XyzNamespace()))))))));
  }

  @AfterAll
  public static void deleteSpace() throws Exception {
    invokeDeleteTestSpace(defaultTestConnectorParams, SPACE, null);
  }

  @ParameterizedTest
  @CsvSource({"hexbin, kind_detail", "quadbin, qk"})
  public void clusteringReturnsAnMvtTile(String clusteringType, String propertyName) throws Exception {
    Map<String, Object> connectorParams = new HashMap<>(defaultTestConnectorParams);
    connectorParams.put("mvtSupport", true);
    GetFeaturesByTileEvent event = new GetFeaturesByTileEvent()
        .withLevel(LEVEL)
        .withX(X)
        .withY(Y)
        .withResponseType(MVT)
        .withBbox(WebMercatorTile.forWeb(LEVEL, X, Y).getBBox(false))
        .withClip(true)
        .withClusteringType(clusteringType)
        .withClusteringParams(new HashMap<>())
        .withConnectorParams(connectorParams)
        .withSpace(SPACE);

    //The type BinaryResponse is not registered for the JSON deserialization, so the response is read as a tree
    String serialized = invokeLambda(event);
    JsonNode response = new ObjectMapper().readTree(serialized);
    assertEquals("BinaryResponse", response.path("type").asText(), serialized);
    assertEquals("application/vnd.mapbox-vector-tile", response.path("mimeType").asText());
    byte[] bytes = response.path("bytes").binaryValue();
    assertNotNull(bytes, "The tile must not be empty");
    //The connector module has no MVT decoder, but the layer name and the property keys are plain strings in the encoded tile
    String tile = new String(bytes, UTF_8);
    assertTrue(tile.contains(SPACE), "The layer is missing");
    assertTrue(tile.contains(propertyName), "The property " + propertyName + " is missing");
  }
}
