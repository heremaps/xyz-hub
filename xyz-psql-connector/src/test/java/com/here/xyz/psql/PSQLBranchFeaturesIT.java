/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

import static com.here.xyz.events.ModifyBranchEvent.Operation.CREATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.SelectiveEvent;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Ref;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class PSQLBranchFeaturesIT extends PSQLAbstractBranchIT {

  private static final String MAIN_1 = "main_1";
  private static final String B1_1 = "b1_1";
  private static final String B2_1 = "b2_1";

  private List<Ref> branchPath;
  private enum ReadEventType {
    ID, SEARCH, ITERATE, BBOX, TILE, GEOMETRY
  }

  @BeforeEach
  public void setup() throws Exception {
    branchPath = new ArrayList<>();
    invokeCreateTestSpace(defaultTestConnectorParams, TEST_SPACE_ID);
    setupBranch();
  }

  @AfterEach
  public void shutdown() throws Exception {
    invokeDeleteTestSpace();
  }

  public void setupBranch() throws Exception {
    //Add feature to main branch
    long main_v1 = extractVersion(deserializeResponse(writeFeature("main_1")));

    //Create branch b1 on main
    Ref b1_baseRef = getBaseRef(0, main_v1);
    invokeLambda(eventForCreate(CREATE, b1_baseRef)); // Returns node ID - 1
    branchPath.add(b1_baseRef);

    //Add feature to branch b1
    long b1_v2 = extractVersion(deserializeResponse(writeFeature("b1_1", 1, List.of(b1_baseRef))));

    //Create branch b2 on b1
    Ref b2_baseRef = getBaseRef(1, b1_v2);
    invokeLambda(eventForCreate(CREATE, b2_baseRef)); // Returns node ID - 2
    branchPath.add(b2_baseRef);

    //Add feature to branch b2
    long b2_v3 = extractVersion(deserializeResponse(writeFeature("b2_1", 2, List.of(b1_baseRef,b2_baseRef))));
  }


  @ParameterizedTest()
  @MethodSource("withEventTypeAndNodeId")
  public void readFeaturesFromBranch(ReadEventType eventType, int nodeId) throws Exception {
    FeatureCollection fc = deserializeResponse(invokeLambda(getReadFeaturesEventFor(eventType, nodeId)));
    Set<String> expectedFeatureIds = nodeId == 0 ? Set.of(MAIN_1) : nodeId == 1 ? Set.of(MAIN_1, B1_1) : Set.of(MAIN_1, B1_1, B2_1);

    assertEquals(expectedFeatureIds.size(), fc.getFeatures().size());
    assertTrue(expectedFeatureIds.containsAll(extractFeatureIds(fc)));
  }

  //TODO: Add more tests for search, tiles and spatial


  private static Stream<Arguments> withEventTypeAndNodeId() {
    return Stream.of(ReadEventType.values())
            .flatMap(eventType -> Stream.of(0, 1, 2)
                    .map(nodeId -> Arguments.of(eventType, nodeId)));
  }

  private Event getReadFeaturesEventFor(ReadEventType type, int nodeId) {
    SelectiveEvent event = switch (type) {
      case ID -> new GetFeaturesByIdEvent().withIds(List.of(MAIN_1, B1_1, B2_1));
      case SEARCH -> new SearchForFeaturesEvent();
      case ITERATE -> new IterateFeaturesEvent();
      case BBOX -> new GetFeaturesByBBoxEvent().withBbox(new BBox(-10, -10, 10, 10));
      case TILE -> new GetFeaturesByTileEvent().withTid("0").withBbox(new BBox(-10, -10, 10, 10));
      case GEOMETRY -> new GetFeaturesByGeometryEvent();
    };

    return event
            .withNodeId(nodeId)
            .withBranchPath(branchPath.subList(0, nodeId))
            .withSpace(TEST_SPACE_ID);
  }

  private Set<String> extractFeatureIds(FeatureCollection featureCollection) throws JsonProcessingException {
    if (featureCollection == null || featureCollection.getFeatures() == null) return Set.of();
    return featureCollection.getFeatures()
            .stream()
            .map(feature -> feature.getId())
            .collect(Collectors.toSet());
  }

}