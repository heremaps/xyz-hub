/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */

package com.here.xyz.psql;

import static com.here.xyz.events.UpdateStrategy.DEFAULT_DELETE_STRATEGY;
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
import com.here.xyz.responses.changesets.ChangesetCollection;
import com.here.xyz.responses.ModifiedBranchResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
//import org.junitpioneer.jupiter.cartesian.CartesianTest;
//import org.junitpioneer.jupiter.cartesian.CartesianTest.Enum;
//import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

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
    super.setup();
    idVersionMap = new HashMap<>();
    setupBranch();
  }

  public void setupBranch() throws Exception {
    // Add features to main branch
    long main_v1 = extractVersion(deserializeResponse(writeFeature(MAIN_1)));
    long main_v2 = extractVersion(deserializeResponse(writeFeature(MAIN_2)));
    idVersionMap.put(MAIN_1, main_v1);
    idVersionMap.put(MAIN_2, main_v2);

    // Create branch b1 on main, version 1
    branch1_baseRef = getBaseRef(0, main_v1);
    invokeLambda(eventForCreate(branch1_baseRef));

    // Add features to branch b1
    long b1_v2 = extractVersion(deserializeResponse(writeFeature(B1_1, 1, List.of(branch1_baseRef))));
    long b1_v3 = extractVersion(deserializeResponse(writeFeature(B1_2, 1, List.of(branch1_baseRef))));
    idVersionMap.put(B1_1, b1_v2);
    idVersionMap.put(B1_2, b1_v3);

    // Create branch b2 on b1, version 2
    branch2_baseRef = getBaseRef(1, b1_v2);
    invokeLambda(eventForCreate(branch2_baseRef));

    // Add features to branch b2
    long b2_v3 = extractVersion(deserializeResponse(writeFeature(B2_1, 2, List.of(branch1_baseRef, branch2_baseRef))));
    long b2_v4 = extractVersion(deserializeResponse(writeFeature(B2_2, 2, List.of(branch1_baseRef, branch2_baseRef))));
    idVersionMap.put(B2_1, b2_v3);
    idVersionMap.put(B2_2, b2_v4);
  }
//
//  @Test
//  public void checkBranchTableData() throws Exception {
//    String mainTable = TEST_SPACE_ID();
//    assertTrue(checkIfTableExists(mainTable));
//    assertEquals(Set.of(MAIN_1, MAIN_2), extractFeatureIds(getAllRowFromTable(mainTable)));
//
//    String branch1Table = getBranchTableName(TEST_SPACE_ID(), 1, branch1_baseRef);
//    assertTrue(checkIfTableExists(branch1Table));
//    assertEquals(Set.of(B1_1, B1_2), extractFeatureIds(getAllRowFromTable(branch1Table)));
//
//    String branch2Table = getBranchTableName(TEST_SPACE_ID(), 2, branch2_baseRef);
//    assertTrue(checkIfTableExists(branch2Table));
//    assertEquals(Set.of(B2_1, B2_2), extractFeatureIds(getAllRowFromTable(branch2Table)));
//  }
//
//  @CartesianTest
//  public void readFeaturesFromBranch(@Enum(names = {"ID", "SEARCH", "ITERATE", "BBOX", "TILE", "GEOMETRY"}) ReadEventType eventType,
//                                     @Values(ints = {0, 1, 2}) int nodeId) throws Exception {
//    FeatureCollection fc = deserializeResponse(invokeLambda(getReadFeaturesEventFor(eventType, nodeId)));
//    Set<String> actualFeatureIds = extractFeatureIds(fc);
//
//    Set<String> expectedFeatureIds;
//    Set<String> notExpectedFeatureIds;
//
//    if (nodeId == 0) {
//      expectedFeatureIds = Set.of(MAIN_1, MAIN_2);
//      notExpectedFeatureIds = Set.of(B1_1, B1_2, B2_1, B2_2);
//    } else if (nodeId == 1) {
//      expectedFeatureIds = Set.of(MAIN_1, B1_1, B1_2);
//      notExpectedFeatureIds = Set.of(MAIN_2, B2_1, B2_2);
//    } else {
//      expectedFeatureIds = Set.of(MAIN_1, B1_1, B2_1, B2_2);
//      notExpectedFeatureIds = Set.of(MAIN_2, B1_2);
//    }
//
//    assertEquals(expectedFeatureIds, actualFeatureIds);
//    assertTrue(notExpectedFeatureIds.stream().noneMatch(actualFeatureIds::contains));
//  }
//
//  @Test
//  public void mergeBranchToMain() throws Exception {
//    ModifyBranchEvent mergeB1ToMain = eventForMerge(1, branch1_baseRef, 0);
//    invokeLambda(mergeB1ToMain);
//    executeReadFeaturesEvent(getReadFeaturesEventFor(ReadEventType.SEARCH, 0),
//            Set.of(MAIN_1, MAIN_2, B1_1, B1_2));
//
//    ModifyBranchEvent mergeB2ToMain = eventForMerge(2, branch2_baseRef, 0);
//    invokeLambda(mergeB2ToMain);
//    executeReadFeaturesEvent(getReadFeaturesEventFor(ReadEventType.SEARCH, 0),
//            Set.of(MAIN_1, MAIN_2, B1_1, B1_2, B2_1, B2_2));
//  }
//
//  @Test
//  public void mergeBranchToMainInReverse() throws Exception {
//    ModifyBranchEvent mergeB2ToMain = eventForMerge(2, branch2_baseRef, 0);
//    invokeLambda(mergeB2ToMain);
//    executeReadFeaturesEvent(getReadFeaturesEventFor(ReadEventType.SEARCH, 0),
//            Set.of(MAIN_1, MAIN_2, B1_1, B2_1, B2_2));
//
//    ModifyBranchEvent mergeB1ToMain = eventForMerge(1, branch1_baseRef, 0);
//    invokeLambda(mergeB1ToMain);
//    executeReadFeaturesEvent(getReadFeaturesEventFor(ReadEventType.SEARCH, 0),
//            Set.of(MAIN_1, MAIN_2, B1_1, B1_2, B2_1, B2_2));
//  }
//
//  @Test
//  public void rebaseBranchToMain() throws Exception {
//    Ref newBaseRef = getBaseRef(0, idVersionMap.get(MAIN_2));
//    ModifiedBranchResponse res = deserializeResponse(invokeLambda(eventForRebase(1, branch2_baseRef, newBaseRef)));
//    executeReadFeaturesEvent(getReadFeaturesEventFor(ReadEventType.SEARCH, res.getNodeId(),
//            List.of(newBaseRef)), Set.of(MAIN_1, MAIN_2, B1_1, B1_2));
//  }
//
//  @Test
//  public void rebaseBranchToBaseBranch() throws Exception {
//    Ref newBaseRef = getBaseRef(1, idVersionMap.get(B1_2));
//    ModifiedBranchResponse res = deserializeResponse(invokeLambda(eventForRebase(2, branch2_baseRef, newBaseRef)));
//    executeReadFeaturesEvent(getReadFeaturesEventFor(ReadEventType.SEARCH, res.getNodeId(),
//            List.of(branch1_baseRef, newBaseRef)), Set.of(MAIN_1, B1_1, B1_2, B2_1, B2_2));
//  }
//
//  @Test
//  public void updateFeaturesOnBranch() throws Exception {
//    long b1_v4 = extractVersion(deserializeResponse(writeFeature(MAIN_1, 1, List.of(branch1_baseRef), true)));
//    long b1_v5 = extractVersion(deserializeResponse(writeFeature(B1_1, 1, List.of(branch1_baseRef), true)));
//
//    ChangesetCollection cc = deserializeResponse(invokeLambda(getReadFeaturesEventFor(ReadEventType.CHANGESET, 1)));
//    assertTrue(cc.getVersions().containsKey(b1_v4));
//    assertEquals(Set.of(MAIN_1), extractFeatureIds(cc.getVersions().get(b1_v4).getUpdated()));
//
//    assertTrue(cc.getVersions().containsKey(b1_v5));
//    assertEquals(Set.of(B1_1), extractFeatureIds(cc.getVersions().get(b1_v5).getUpdated()));
//  }
//
//  @Test
//  public void deleteFeaturesOnBranch() throws Exception {
//    writeFeature(MAIN_1, 1, List.of(branch1_baseRef), false, DEFAULT_DELETE_STRATEGY);
//    long b1_v4 = 4L;
//
//    writeFeature(B1_1, 1, List.of(branch1_baseRef), false, DEFAULT_DELETE_STRATEGY);
//    long b1_v5 = 5L;
//
//    ChangesetCollection cc = deserializeResponse(invokeLambda(getReadFeaturesEventFor(ReadEventType.CHANGESET, 1)));
//    assertTrue(cc.getVersions().containsKey(b1_v4));
//    assertEquals(Set.of(MAIN_1), extractFeatureIds(cc.getVersions().get(b1_v4).getDeleted()));
//
//    assertTrue(cc.getVersions().containsKey(b1_v5));
//    assertEquals(Set.of(B1_1), extractFeatureIds(cc.getVersions().get(b1_v5).getDeleted()));
//
//    executeReadFeaturesEvent(getReadFeaturesEventFor(ReadEventType.SEARCH, 1), Set.of(B1_2));
//  }
//
//  @Test
//  public void addSameFeatureToBaseAndBranch() throws Exception {
//    String newFeature = "same_feature";
//    long main_v3 = extractVersion(deserializeResponse(writeFeature(newFeature)));
//    long b1_v4 = extractVersion(deserializeResponse(writeFeature(newFeature, 1, List.of(branch1_baseRef))));
//
//    executeReadFeaturesEvent(getReadFeaturesEventFor(ReadEventType.SEARCH, 0), Set.of(MAIN_1, MAIN_2, newFeature));
//    executeReadFeaturesEvent(getReadFeaturesEventFor(ReadEventType.SEARCH, 1), Set.of(MAIN_1, B1_1, B1_2, newFeature));
//  }
//
//  private Event getReadFeaturesEventFor(ReadEventType type, int nodeId) {
//    List<Ref> branchPath = nodeId == 1 ? List.of(branch1_baseRef)
//            : nodeId == 2 ? List.of(branch1_baseRef, branch2_baseRef) : List.of();
//    return getReadFeaturesEventFor(type, nodeId, branchPath);
//  }
//
//  private Event getReadFeaturesEventFor(ReadEventType type, int nodeId, List<Ref> branchPath) {
//    SelectiveEvent event;
//    switch (type) {
//      case ID:
//        event = new GetFeaturesByIdEvent().withIds(List.of(MAIN_1, MAIN_2, B1_1, B1_2, B2_1, B2_2));
//        break;
//      case SEARCH:
//        event = new SearchForFeaturesEvent();
//        break;
//      case ITERATE:
//        event = new IterateFeaturesEvent();
//        break;
//      case BBOX:
//        event = new GetFeaturesByBBoxEvent().withBbox(new BBox(-10, -10, 10, 10));
//        break;
//      case TILE:
//        event = new GetFeaturesByTileEvent().withTid("0").withBbox(new BBox(-10, -10, 10, 10));
//        break;
//      case GEOMETRY:
//        event = new GetFeaturesByGeometryEvent();
//        break;
//      case CHANGESET:
//        event = new IterateChangesetsEvent().withRef(new Ref(0, 1000));
//        break;
//      default:
//        throw new IllegalArgumentException("Unsupported ReadEventType: " + type);
//    }
//
//    return event.withNodeId(nodeId)
//            .withBranchPath(branchPath)
//            .withVersionsToKeep(1000)
//            .withSpace(TEST_SPACE_ID());
//  }
//
//  private Set<String> extractFeatureIds(FeatureCollection featureCollection) throws JsonProcessingException {
//    if (featureCollection == null || featureCollection.getFeatures() == null) return Set.of();
//    return featureCollection.getFeatures()
//            .stream()
//            .map(f -> f.getId())
//            .collect(Collectors.toSet());
//  }
//
////  private Set<String> extractFeatureIds(List<FeatureRow> featureRows) {
////    return featureRows.stream().map(FeatureRow::id).collect(Collectors.toSet());
////  }
//
//  private void executeReadFeaturesEvent(Event event, Set<String> expectedFeatureIds) throws Exception {
//    FeatureCollection fc = deserializeResponse(invokeLambda(event));
//    Set<String> actualFeatureIds = extractFeatureIds(fc);
//    assertEquals(expectedFeatureIds, actualFeatureIds);
//  }
}
