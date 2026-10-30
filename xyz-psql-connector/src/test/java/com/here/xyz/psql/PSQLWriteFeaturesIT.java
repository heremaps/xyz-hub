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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static com.here.xyz.events.UpdateStrategy.DEFAULT_DELETE_STRATEGY;
import static com.here.xyz.events.UpdateStrategy.DEFAULT_UPDATE_STRATEGY;
import static com.here.xyz.events.UpdateStrategy.OnExists.REPLACE;
import static com.here.xyz.events.UpdateStrategy.OnMergeConflict.ERROR;
import static com.here.xyz.events.UpdateStrategy.OnNotExists.CREATE;
import static com.here.xyz.events.UpdateStrategy.OnVersionConflict.MERGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Space.Extension;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Exercises the {@code write_features} engine directly through {@link WriteFeaturesEvent}, without going through the hub.
 *
 * The composite tests are parameterized over {@code compositeWithVersion}, so every assertion runs once for a composite bound to a
 * fixed base version and once for a plain composite tracking its base's HEAD. The unbound run is the regression guard: it has to
 * keep behaving exactly as it did before bound base versions existed.
 */
public class PSQLWriteFeaturesIT extends PSQLAbstractIT {

  private static final int VERSIONS_TO_KEEP = 1000;
  private static final UpdateStrategy MERGE_STRATEGY = new UpdateStrategy(REPLACE, CREATE, MERGE, ERROR);

  private final String spaceId = spaceId();
  private final String extSpaceId = spaceId + "-ext";
  private final String extExtSpaceId = extSpaceId + "-ext";
  private final Map<String, Space> spaces = new HashMap<>();

  @BeforeEach
  public void setup() throws Exception {
    teardown();
  }

  @AfterEach
  public void teardown() throws Exception {
    spaces.clear();
    invokeDeleteTestSpaces(null, List.of(extExtSpaceId, extSpaceId, spaceId));
  }

  //=========================================================================================================================
  // Non-composite spaces - the baseline behaviour which must stay untouched by bound base versions
  //=========================================================================================================================

  @Test
  void writingAnExistingFeatureOfANormalSpaceTransformsToAnUpdate() throws Exception {
    createSpace(spaceId, null);

    writeFeature(spaceId, feature("f-1", "key1", "first"));
    writeFeature(spaceId, feature("f-1", "key1", "second"));

    assertEquals(List.of("I", "U"), operationsOf(spaceId, "f-1"),
        "In a normal space the second write of the same ID has to become an update");
    assertEquals("second", propertyOf(readFeature(spaceId, "f-1"), "key1"));
  }

  @Test
  void deletingAFeatureOfANormalSpaceProducesADeleteOperation() throws Exception {
    createSpace(spaceId, null);

    writeFeature(spaceId, feature("f-1", "key1", "first"));
    deleteFeature(spaceId, "f-1");

    assertEquals(List.of("I", "D"), operationsOf(spaceId, "f-1"),
        "A normal space has no composite below it, so the deletion must not be turned into a hide-composite operation");
    assertNull(readFeature(spaceId, "f-1"));
  }

  @Test
  void mergingAgainstAnOlderVersionOfANormalSpaceUsesThatVersionAsMergeBase() throws Exception {
    createSpace(spaceId, null);

    //Version 1 is the state both writers start from
    writeFeature(spaceId, feature("f-1", "key1", "first"));
    //Another writer adds a second property, producing version 2
    writeFeature(spaceId, feature("f-1", Map.of("key1", "first", "key2", "added")));

    //Now write against version 1, touching only key1. The merge base is version 1, so key2 has to survive.
    writeFeature(spaceId, feature("f-1", Map.of("key1", "changed"), 1L), MERGE_STRATEGY, false);

    Feature merged = readFeature(spaceId, "f-1");
    assertEquals("changed", propertyOf(merged, "key1"));
    assertEquals("added", propertyOf(merged, "key2"),
        "The property added by the other writer must survive, which only happens if version 1 was loaded as the merge base");
  }

  //=========================================================================================================================
  // Composite spaces - bound and unbound
  //=========================================================================================================================

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void writeFeatures(boolean compositeWithVersion) throws Exception {
    initializeSpaces(compositeWithVersion);

    assertWrittenRows(spaceId, Set.of("base-1", "base-2"));
    executeReadFeaturesEvent(iterateEvent(spaceId), Set.of("base-1", "base-2"));

    assertWrittenRows(extSpaceId, Set.of("delta1-1", "delta1-2"));
    Set<String> expectedExtIds = compositeWithVersion
        ? Set.of("base-1", "delta1-1", "delta1-2")
        : Set.of("base-1", "base-2", "delta1-1", "delta1-2");
    executeReadFeaturesEvent(iterateEvent(extSpaceId), expectedExtIds);

    assertWrittenRows(extExtSpaceId, Set.of("delta2-1", "delta2-2"));
    Set<String> expectedExtExtIds = compositeWithVersion
        ? Set.of("base-1", "delta1-1", "delta2-1", "delta2-2")
        : Set.of("base-1", "base-2", "delta1-1", "delta1-2", "delta2-1", "delta2-2");
    executeReadFeaturesEvent(iterateEvent(extExtSpaceId), expectedExtExtIds);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void writingAnInheritedFeatureIntoTheExtensionStaysAnInsert(boolean compositeWithVersion) throws Exception {
    initializeSpaces(compositeWithVersion);

    //base-1 is visible through the extension in both cases, but it lives in the base table, not in the extension
    writeFeature(extSpaceId, feature("base-1", "key1", "overridden"));

    assertEquals(List.of("I"), operationsOf(extSpaceId, "base-1"),
        "Overriding an inherited feature writes the first row of that ID into the extension, so it stays an insert");
    assertEquals("overridden", propertyOf(readFeature(extSpaceId, "base-1"), "key1"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void deletingFeaturesThroughACompositeHidesOnlyWhatIsVisible(boolean compositeWithVersion) throws Exception {
    initializeSpaces(compositeWithVersion);

    //Remove base-1 from the base space itself, which moves the base past the bound version
    deleteFeature(spaceId, "base-1");
    assertEquals(List.of("I", "D"), operationsOf(spaceId, "base-1"));

    /*
    base-2 was written at base version 2, so it is outside a base bound to version 1. Deleting it through the extension
    therefore has nothing to hide, while the unbound composite still sees it at the base's HEAD.
     */
    deleteFeature(extSpaceId, "base-2");
    if (compositeWithVersion)
      assertEquals(List.of(), operationsOf(extSpaceId, "base-2"),
          "Nothing is visible to hide, so no row may be written into the extension");
    else
      assertEquals(List.of("H"), operationsOf(extSpaceId, "base-2"));

    /*
    base-1 is the mirror image: still present in the bound snapshot of version 1, but already deleted at the base's HEAD.
     */
    deleteFeature(extSpaceId, "base-1");
    if (compositeWithVersion)
      assertEquals(List.of("H"), operationsOf(extSpaceId, "base-1"),
          "The bound snapshot still contains base-1, so the deletion has to be recorded as a hide-composite operation");
    else
      assertEquals(List.of(), operationsOf(extSpaceId, "base-1"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void mergingLogicalVersionZeroUsesTheVisibleBaseStateAsMergeBase(boolean compositeWithVersion) throws Exception {
    initializeSpaces(compositeWithVersion);

    //The base moves on after the extension was bound to version 1
    writeFeature(spaceId, feature("base-1", Map.of("key1", "moved", "addedInBase", "yes")));

    /*
    A client which read base-1 through the composite saw it with logical version 0, because it came from a base dataset.
    Writing it back with that version has to resolve the merge base to whatever the composite currently exposes.
     */
    writeFeature(extSpaceId, feature("base-1", Map.of("delta", "written"), 0L), MERGE_STRATEGY, true);

    Feature merged = readFeature(extSpaceId, "base-1");
    assertNotNull(merged, "The merge must not silently drop the feature");
    assertEquals("written", propertyOf(merged, "delta"));

    if (compositeWithVersion) {
      assertEquals("initial", propertyOf(merged, "key1"),
          "A bound composite has to merge onto the snapshot of the bound version, not onto the base's HEAD");
      assertNull(propertyOf(merged, "addedInBase"),
          "Properties added to the base after the bound version must not leak into the merge result");
    }
    else {
      assertEquals("moved", propertyOf(merged, "key1"),
          "An unbound composite still merges onto the base's HEAD");
      assertEquals("yes", propertyOf(merged, "addedInBase"));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void partiallyUpdatingAnInheritedFeatureMergesOntoTheVisibleBaseState(boolean compositeWithVersion) throws Exception {
    initializeSpaces(compositeWithVersion);

    //The base moves on after the extension was bound to version 1
    writeFeature(spaceId, feature("base-1", Map.of("key1", "moved", "addedInBase", "yes")));

    //A partial write only carries the properties it wants to change, everything else comes from the visible state
    writeFeature(extSpaceId, feature("base-1", Map.of("delta", "patched")), DEFAULT_UPDATE_STRATEGY, true);

    Feature patched = readFeature(extSpaceId, "base-1");
    assertEquals("patched", propertyOf(patched, "delta"));

    if (compositeWithVersion) {
      assertEquals("initial", propertyOf(patched, "key1"));
      assertNull(propertyOf(patched, "addedInBase"));
    }
    else {
      assertEquals("moved", propertyOf(patched, "key1"));
      assertEquals("yes", propertyOf(patched, "addedInBase"));
    }
  }

  /**
   * A second write of the same ID through the extension has to be a real three-way merge: the merge base is the state the bound
   * version exposes, while the merge head is the row the extension already owns. Without an extension row of its own the two
   * collapse into the same feature, so this is the only test which distinguishes them.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void mergingLogicalVersionZeroKeepsAnExistingExtensionOverride(boolean compositeWithVersion) throws Exception {
    initializeSpaces(compositeWithVersion);

    //The base moves on after the extension was bound to version 1
    writeFeature(spaceId, feature("base-1", Map.of("key1", "moved")));

    //A first partial write gives the extension a row of its own for the inherited feature
    writeFeature(extSpaceId, feature("base-1", Map.of("headDelta", "head")), DEFAULT_UPDATE_STRATEGY, true);
    assertEquals(List.of("I"), operationsOf(extSpaceId, "base-1"));

    //A second partial write based on logical version zero must merge with that override instead of replacing it
    writeFeature(extSpaceId, feature("base-1", Map.of("staleDelta", "stale"), 0L), MERGE_STRATEGY, true);

    Feature merged = readFeature(extSpaceId, "base-1");
    assertEquals("head", propertyOf(merged, "headDelta"),
        "The property of the existing extension override must survive, so the merge head has to be the extension's own row");
    assertEquals("stale", propertyOf(merged, "staleDelta"));
    assertEquals(compositeWithVersion ? "initial" : "moved", propertyOf(merged, "key1"));
  }

  /**
   * A two-level composite in which the two levels are bound to *different* versions. Both bound predicates have to end up on the
   * table they belong to - binding them the other way round stays invisible as long as every level uses the same version.
   */
  @Test
  void aTwoLevelCompositeAppliesEachBoundVersionToItsOwnTable() throws Exception {
    createSpace(spaceId, null);
    writeFeature(spaceId, feature("base-1", Map.of("key1", "v1"))); //base version 1
    writeFeature(spaceId, feature("base-1", Map.of("key1", "v2"))); //base version 2
    writeFeature(spaceId, feature("base-1", Map.of("key1", "v3"))); //base version 3

    //The intermediate is bound to version 2 of the base, so it exposes base-1 as of "v2"
    createSpace(extSpaceId, new Extension().withSpaceId(spaceId).withVersion(2L));
    writeFeature(extSpaceId, feature("delta1-1", Map.of("key1", "initial"))); //intermediate version 1
    writeFeature(extSpaceId, feature("delta1-2", Map.of("key1", "initial"))); //intermediate version 2

    //The outer composite is bound to version 1 of the intermediate
    createSpace(extExtSpaceId, new Extension().withSpaceId(extSpaceId).withVersion(1L));

    executeReadFeaturesEvent(iterateEvent(extExtSpaceId), Set.of("base-1", "delta1-1"));

    //Patching through the outer composite has to merge onto base-1 as the *intermediate's* bound version exposes it
    writeFeature(extExtSpaceId, feature("base-1", Map.of("outer", "x")), DEFAULT_UPDATE_STRATEGY, true);

    Feature patched = readFeature(extExtSpaceId, "base-1");
    assertEquals("x", propertyOf(patched, "outer"));
    assertEquals("v2", propertyOf(patched, "key1"),
        "v1 means the two bound versions were applied to the wrong tables, v3 means the base was not bound at all");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void writingWithSuperContextIsRejectedOnlyForABoundBase(boolean compositeWithVersion) throws Exception {
    initializeSpaces(compositeWithVersion);

    String response = invokeLambda(writeEvent(extSpaceId, feature("base-1", "key1", "viaSuper"), DEFAULT_UPDATE_STRATEGY, false)
        .withContext(SUPER));
    XyzResponse parsed = XyzSerializable.deserialize(response);

    if (compositeWithVersion) {
      ErrorResponse error = assertInstanceOf(ErrorResponse.class, parsed,
          "Writing through SUPER would target the very table the extension binds to a fixed version");
      assertEquals(XyzError.ILLEGAL_ARGUMENT, error.getError());
    }
    else {
      assertNotNull(parsed);
      assertEquals(List.of("I", "U"), operationsOf(spaceId, "base-1"),
          "Without a bound base version a SUPER write still has to reach the base table");
    }
  }

  //=========================================================================================================================
  // Helpers
  //=========================================================================================================================

  private void initializeSpaces(boolean compositeWithVersion) throws Exception {
    createSpace(spaceId, null);
    writeFeature(spaceId, feature("base-1", "key1", "initial")); //base version 1
    writeFeature(spaceId, feature("base-2", "key1", "initial")); //base version 2

    createSpace(extSpaceId, new Extension().withSpaceId(spaceId).withVersion(compositeWithVersion ? 1L : null));
    writeFeature(extSpaceId, feature("delta1-1", "key1", "initial")); //extension version 1
    writeFeature(extSpaceId, feature("delta1-2", "key1", "initial")); //extension version 2

    createSpace(extExtSpaceId, new Extension().withSpaceId(extSpaceId).withVersion(compositeWithVersion ? 1L : null));
    writeFeature(extExtSpaceId, feature("delta2-1", "key1", "initial"));
    writeFeature(extExtSpaceId, feature("delta2-2", "key1", "initial"));
  }

  private void createSpace(String spaceId, Extension extension) throws Exception {
    Space space = new Space().withId(spaceId).withExtension(extension).withVersionsToKeep(VERSIONS_TO_KEEP);
    invokeCreateTestSpace(null, space);
    spaces.put(spaceId, space);
  }

  private static Feature feature(String id, String key, String value) {
    return feature(id, Map.of(key, value));
  }

  private static Feature feature(String id, Map<String, Object> properties) {
    return feature(id, properties, null);
  }

  /**
   * Builds a feature with deterministic properties. A non-null baseVersion is put into the XYZ namespace, which is where the
   * FeatureWriter picks the merge base up from when the event itself does not carry a version ref.
   */
  private static Feature feature(String id, Map<String, Object> properties, Long baseVersion) {
    Properties props = new Properties();
    properties.forEach(props::with);
    if (baseVersion != null)
      props.withXyzNamespace(new XyzNamespace().withVersion(baseVersion));

    return new Feature()
        .withId(id)
        .withProperties(props)
        .withGeometry(new Point().withCoordinates(new PointCoordinates(0, 0)));
  }

  private String writeFeature(String spaceId, Feature feature) throws Exception {
    return writeFeature(spaceId, feature, DEFAULT_UPDATE_STRATEGY, false);
  }

  private String writeFeature(String spaceId, Feature feature, UpdateStrategy updateStrategy, boolean partialUpdates)
      throws Exception {
    return invokeLambda(writeEvent(spaceId, feature, updateStrategy, partialUpdates));
  }

  private String deleteFeature(String spaceId, String featureId) throws Exception {
    return invokeLambda(writeEvent(spaceId, feature(featureId, Map.of()), DEFAULT_DELETE_STRATEGY, false));
  }

  private WriteFeaturesEvent writeEvent(String spaceId, Feature feature, UpdateStrategy updateStrategy, boolean partialUpdates) {
    return new WriteFeaturesEvent()
        .withSpace(spaceId)
        .withResponseDataExpected(true)
        .withVersionsToKeep(VERSIONS_TO_KEEP)
        .withParams(resolveSpaceParams(spaceId))
        .withModifications(Set.of(new WriteFeaturesEvent.Modification()
            .withUpdateStrategy(updateStrategy)
            .withPartialUpdates(partialUpdates)
            .withFeatureData(new FeatureCollection().withFeatures(List.of(feature)))));
  }

  private Event iterateEvent(String spaceId) {
    return new IterateFeaturesEvent()
        .withVersionsToKeep(VERSIONS_TO_KEEP)
        .withSpace(spaceId)
        .withParams(resolveSpaceParams(spaceId));
  }

  private Feature readFeature(String spaceId, String featureId) throws Exception {
    FeatureCollection fc = deserializeResponse(invokeLambda(new GetFeaturesByIdEvent()
        .withIds(List.of(featureId))
        .withSpace(spaceId)
        .withContext(SpaceContext.DEFAULT)
        .withVersionsToKeep(VERSIONS_TO_KEEP)
        .withParams(resolveSpaceParams(spaceId))));

    return fc.getFeatures().isEmpty() ? null : fc.getFeatures().get(0);
  }

  private static Object propertyOf(Feature feature, String key) {
    return feature == null || feature.getProperties() == null ? null : feature.getProperties().get(key);
  }

  /**
   * The operations recorded for one ID in a single table, oldest first.
   */
  private List<String> operationsOf(String spaceId, String featureId) throws Exception {
    return getAllRowFromTable(spaceId).stream()
        .filter(row -> featureId.equals(row.id()))
        .sorted(Comparator.comparingLong(FeatureRow::version))
        .map(FeatureRow::operation)
        .collect(Collectors.toList());
  }

  /**
   * Mirrors what {@code FeatureHandler.injectSpaceParams} builds on the hub side, so the connector sees the same resolved
   * extension chain it would see in production.
   */
  private Map<String, Object> resolveSpaceParams(String spaceId) {
    Map<String, Object> params = new HashMap<>();
    Space space = spaces.get(spaceId);
    if (space != null && space.getExtension() != null) {
      Map<String, Object> extendsMap = space.getExtension().toMap();
      Space extendedSpace = spaces.get(space.getExtension().getSpaceId());

      if (extendedSpace != null && extendedSpace.getExtension() != null)
        extendsMap.put("extends", extendedSpace.getExtension().toMap());

      params.put("extends", extendsMap);
    }
    return params;
  }

  private void assertWrittenRows(String spaceId, Set<String> expectedFeatureIds) throws Exception {
    List<FeatureRow> rows = getAllRowFromTable(spaceId);
    assertEquals(expectedFeatureIds, extractFeatureIds(rows));
    assertTrue(rows.stream().allMatch(row -> "I".equals(row.operation())));
  }
}
