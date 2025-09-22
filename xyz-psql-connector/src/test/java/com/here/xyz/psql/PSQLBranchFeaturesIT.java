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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.ModifyBranchEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.SelectiveEvent;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.responses.ModifiedBranchResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Enum;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

public class PSQLBranchFeaturesIT extends PSQLAbstractBranchIT {

  private static final String MAIN_1 = "main_1";
  private static final String MAIN_2 = "main_2";
  private static final String B1_1 = "b1_1";
  private static final String B1_2 = "b1_2";
  private static final String B2_1 = "b2_1";
  private static final String B2_2 = "b2_2";

  private Map<String, Long> idVersionMap;
  private Ref branch1_baseRef;
  private Ref branch2_baseRef;

  private enum ReadEventType {
    ID, SEARCH, ITERATE, BBOX, TILE, GEOMETRY, CHANGESET
  }

  @BeforeEach
  public void setup() throws Exception {
    idVersionMap = new HashMap<>();
    invokeCreateTestSpace(defaultTestConnectorParams, TEST_SPACE_ID);
    setupBranch();
  }

  @AfterEach
  public void shutdown() throws Exception {
    invokeDeleteTestSpace();
  }

  public void setupBranch() throws Exception {
    //Add features to main branch
    long main_v1 = extractVersion(deserializeResponse(writeFeature(MAIN_1)));
    long main_v2 = extractVersion(deserializeResponse(writeFeature(MAIN_2)));
    idVersionMap.put(MAIN_1, main_v1);
    idVersionMap.put(MAIN_2, main_v2);

    //Create branch b1 on main, version 1
    Ref b1_baseRef = getBaseRef(0, main_v1);
    invokeLambda(eventForCreate(b1_baseRef)); // Returns node ID - 1
    branch1_baseRef = b1_baseRef;

    //Add features to branch b1
    long b1_v2 = extractVersion(deserializeResponse(writeFeature(B1_1, 1, List.of(b1_baseRef))));
    long b1_v3 = extractVersion(deserializeResponse(writeFeature(B1_2, 1, List.of(b1_baseRef))));
    idVersionMap.put(B1_1, b1_v2);
    idVersionMap.put(B1_2, b1_v3);

    //Create branch b2 on b1, version 2
    Ref b2_baseRef = getBaseRef(1, b1_v2);
    invokeLambda(eventForCreate(b2_baseRef)); // Returns node ID - 2
    branch2_baseRef = b2_baseRef;

    //Add feature to branch b2
    long b2_v3 = extractVersion(deserializeResponse(writeFeature(B2_1, 2, List.of(b1_baseRef,b2_baseRef))));
    long b2_v4 = extractVersion(deserializeResponse(writeFeature(B2_2, 2, List.of(b1_baseRef,b2_baseRef))));
    idVersionMap.put(B2_1, b2_v3);
    idVersionMap.put(B2_2, b2_v4);
  }

  @Test
  public void checkBranchTableData() throws Exception {

    String mainTable = TEST_SPACE_ID;
    assertTrue(checkIfTableExists(mainTable));
    assertEquals(Set.of(MAIN_1, MAIN_2), extractFeatureIds(getAllRowFromTable(mainTable)));

    String branch1Table = getBranchTableName(TEST_SPACE_ID, 1, branch1_baseRef);
    assertTrue(checkIfTableExists(branch1Table));
    assertEquals(Set.of(B1_1, B1_2), extractFeatureIds(getAllRowFromTable(branch1Table)));

    String branch2Table = getBranchTableName(TEST_SPACE_ID, 2, branch2_baseRef);
    assertTrue(checkIfTableExists(branch2Table));
    assertEquals(Set.of(B2_1, B2_2), extractFeatureIds(getAllRowFromTable(branch2Table)));
  }

  @CartesianTest
  public void readFeaturesFromBranch(@Enum(names = {"ID", "SEARCH", "ITERATE", "BBOX", "TILE", "GEOMETRY"}) ReadEventType eventType,
                                     @Values(ints = {0, 1, 2}) int nodeId) throws Exception {
    FeatureCollection fc = deserializeResponse(invokeLambda(getReadFeaturesEventFor(eventType, nodeId)));
    Set<String> actualFeatureIds = extractFeatureIds(fc);

    Set<String> expectedFeatureIds = nodeId == 0 ? Set.of(MAIN_1, MAIN_2)
            : nodeId == 1 ? Set.of(MAIN_1, B1_1, B1_2) : Set.of(MAIN_1, B1_1, B2_1, B2_2);

    Set<String> notExpectedFeatureIds = nodeId == 0 ? Set.of(B1_1, B1_2, B2_1, B2_2)
            : nodeId == 1 ? Set.of(MAIN_2, B2_1, B2_2) : Set.of(MAIN_2, B2_2);

    assertEquals(expectedFeatureIds.size(), fc.getFeatures().size());
    assertTrue(expectedFeatureIds.containsAll(actualFeatureIds));
    assertTrue(notExpectedFeatureIds.stream().noneMatch(actualFeatureIds::contains));
  }

  @Test
  public void mergeBranchToMain() throws Exception {
    //Merge b1 to main
    ModifyBranchEvent mergeB1ToMain = eventForMerge(1, branch1_baseRef, 0);
    invokeLambda(mergeB1ToMain);
    executeReadFeaturesEvent(getReadFeaturesEventFor(ReadEventType.SEARCH, 0), Set.of(MAIN_1, MAIN_2, B1_1, B1_2));

    //Merge b2 to main after merging b1
    ModifyBranchEvent mergeB2ToMain = eventForMerge(2, branch2_baseRef, 0);
    invokeLambda(mergeB2ToMain);
    executeReadFeaturesEvent(getReadFeaturesEventFor(ReadEventType.SEARCH, 0), Set.of(MAIN_1, MAIN_2, B1_1, B1_2, B2_1, B2_2));
  }

  @Test
  public void mergeBranchToMainInReverse() throws Exception {
    //Merge b2 to main
    ModifyBranchEvent mergeB2ToMain = eventForMerge(2, branch2_baseRef, 0);
    invokeLambda(mergeB2ToMain);
    executeReadFeaturesEvent(getReadFeaturesEventFor(ReadEventType.SEARCH, 0), Set.of(MAIN_1, MAIN_2, B1_1, B2_1, B2_2));

    //Merge b1 to main after merging b2
    ModifyBranchEvent mergeB1ToMain = eventForMerge(1, branch1_baseRef, 0);
    invokeLambda(mergeB1ToMain);
    executeReadFeaturesEvent(getReadFeaturesEventFor(ReadEventType.SEARCH, 0), Set.of(MAIN_1, MAIN_2, B1_1, B1_2, B2_1, B2_2));
  }

  @Test
  public void rebaseBranchToMain() throws Exception {
    // Rebase branch1 to main, version=2
    Ref newBaseRef = getBaseRef(0, idVersionMap.get(MAIN_2));
    ModifiedBranchResponse res = deserializeResponse(invokeLambda(eventForRebase(1, branch2_baseRef, newBaseRef)));
    executeReadFeaturesEvent(
            getReadFeaturesEventFor(ReadEventType.SEARCH, res.getNodeId(), List.of(newBaseRef)),
            Set.of(MAIN_1, MAIN_2, B1_1, B1_2));
  }

  @Test
  public void rebaseBranchToBaseBranch() throws Exception {
    // Rebase branch2 to branch1, version=3
    Ref newBaseRef = getBaseRef(1, idVersionMap.get(B1_2));
    ModifiedBranchResponse res = deserializeResponse(invokeLambda(eventForRebase(2, branch2_baseRef, newBaseRef)));
    executeReadFeaturesEvent(
            getReadFeaturesEventFor(ReadEventType.SEARCH, res.getNodeId(), List.of(branch1_baseRef, newBaseRef)),
            Set.of(MAIN_1, B1_1, B1_2, B2_1, B2_2));
  }

  @Test
  @Disabled
  public void updateFeaturesOnBranch() throws Exception {
    //Update features on branch 1
    writeFeature(MAIN_1, 1, List.of(branch1_baseRef));
    writeFeature(B1_2, 1, List.of(branch1_baseRef));
    System.out.println(invokeLambda(getReadFeaturesEventFor(ReadEventType.SEARCH, 1)));
    String res = invokeLambda(getReadFeaturesEventFor(ReadEventType.CHANGESET, 1));
    System.out.println(res);
  }

  private Event getReadFeaturesEventFor(ReadEventType type, int nodeId) {
    List<Ref> branchPath = nodeId == 1 ? List.of(branch1_baseRef)
            : nodeId == 2 ? List.of(branch1_baseRef, branch2_baseRef) : List.of();

    return getReadFeaturesEventFor(type, nodeId, branchPath);
  }

  private Event getReadFeaturesEventFor(ReadEventType type, int nodeId, List<Ref> branchPath) {
    SelectiveEvent event = switch (type) {
      case ID -> new GetFeaturesByIdEvent().withIds(List.of(MAIN_1, MAIN_2, B1_1, B1_2, B2_1, B2_2));
      case SEARCH -> new SearchForFeaturesEvent();
      case ITERATE -> new IterateFeaturesEvent();
      case BBOX -> new GetFeaturesByBBoxEvent().withBbox(new BBox(-10, -10, 10, 10));
      case TILE -> new GetFeaturesByTileEvent().withTid("0").withBbox(new BBox(-10, -10, 10, 10));
      case GEOMETRY -> new GetFeaturesByGeometryEvent();
      case CHANGESET -> new IterateChangesetsEvent().withRef(new Ref(0, 1000));
    };

    return event
            .withNodeId(nodeId)
            .withBranchPath(branchPath)
            .withVersionsToKeep(1000)
            .withSpace(TEST_SPACE_ID);
  }

  private Set<String> extractFeatureIds(FeatureCollection featureCollection) throws JsonProcessingException {
    if (featureCollection == null || featureCollection.getFeatures() == null) return Set.of();
    return featureCollection.getFeatures()
            .stream()
            .map(feature -> feature.getId())
            .collect(Collectors.toSet());
  }

  private Set<String> extractFeatureIds(List<FeatureRow> featureRows) {
    return featureRows.stream().map(featureRow -> featureRow.id()).collect(Collectors.toSet());
  }

  private void executeReadFeaturesEvent(Event event, Set<String> expectedFeatureIds) throws Exception {
    FeatureCollection fc = deserializeResponse(invokeLambda(event));
    Set<String> actualFeatureIds = extractFeatureIds(fc);
    assertEquals(expectedFeatureIds, actualFeatureIds);
  }

}