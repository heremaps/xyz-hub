/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.naksha.lib.core.util.diff;

import com.here.naksha.lib.core.models.geojson.implementation.EXyzAction;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.util.IoHelp;
import com.here.naksha.lib.core.util.json.JsonObject;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.JSONException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.util.Map;

import static com.here.naksha.lib.core.util.diff.PatcherUtils.removeAllRemoveOp;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@SuppressWarnings({"rawtypes", "ConstantConditions"})
class PatcherTest {

  @Test
  void basic() {
    final XyzFeature f1 =
        JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_1.json"), XyzFeature.class);
    assertNotNull(f1);

    final XyzFeature f2 =
        JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_2.json"), XyzFeature.class);
    assertNotNull(f2);

    final Difference diff = Patcher.getDifference(f1, f2);
    assertNotNull(diff);

    final XyzFeature f1_patched_to_f2 = Patcher.patch(f1, diff);
    assertNotNull(f1_patched_to_f2);

    final Difference newDiff = Patcher.getDifference(f1_patched_to_f2, f2);
    assertNull(newDiff);
  }

  @Test
  void testCompareBasicNestedJson() throws JSONException {
    final JsonObject f3 =
            JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_3.json"), JsonObject.class);
    assertNotNull(f3);
    final JsonObject f4 =
            JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_4.json"), JsonObject.class);
    assertNotNull(f4);

    final Difference diff34 = Patcher.getDifference(f3, f4);
    assertNotNull(diff34);
    assert (diff34 instanceof MapDiff);

    // Assert outermost layer
    final MapDiff mapDiff34 = (MapDiff) diff34;
    // TODO if possible to serialize Difference, simply compare the serialized Difference object with test file content
    assertTrue(mapDiff34.get("isAdded") instanceof InsertOp);
    assertTrue(mapDiff34.get("willBeUpdated") instanceof UpdateOp);
    assertTrue(mapDiff34.get("firstToBeDeleted") instanceof RemoveOp);
    assertTrue(mapDiff34.get("map") instanceof MapDiff);
    assertTrue(mapDiff34.get("array") instanceof ListDiff);
    assertTrue(mapDiff34.get("speedLimit") instanceof RemoveOp);

    // Assert nested layer
    final MapDiff nestedMapDiff34 = (MapDiff) mapDiff34.get("map");
    // "mapID" is retained, does not appear in nestedMapDiff34
    assertTrue(nestedMapDiff34.get("isAdded") instanceof InsertOp);
    assertTrue(nestedMapDiff34.get("willBeUpdated") instanceof UpdateOp);
    assertTrue(nestedMapDiff34.get("willBeDeleted") instanceof RemoveOp);

    // Assert nested array
    final ListDiff nestedArrayDiff34 = (ListDiff) mapDiff34.get("array");
    assertTrue(nestedArrayDiff34.get(1) instanceof UpdateOp);
    assertTrue(nestedArrayDiff34.get(2) instanceof MapDiff);
    // "retainedElement" is retained, does not appear in nestedMapDiff34
    // InsertOp case for array (ListDiff) is addressed in the test testCompareSameArrayDifferentOrder()
    assertTrue(nestedArrayDiff34.get(3) instanceof RemoveOp);

    // Some extra nested JSON object in array assertions
    assertTrue(((MapDiff) nestedArrayDiff34.get(2)).get("isAddedProperty") instanceof InsertOp);
    assertTrue(((MapDiff) nestedArrayDiff34.get(2)).get("nestedShouldBeUpdated") instanceof UpdateOp);
    assertTrue(((MapDiff) nestedArrayDiff34.get(2)).get("willBeDeletedProperty") instanceof RemoveOp);

    // Modify the whole difference to get rid of all RemoveOp
    Difference newDiff34 = removeAllRemoveOp(mapDiff34);
    final JsonObject patchedf3 = Patcher.patch(f3,newDiff34);

    assertNotNull(patchedf3);

    final JsonObject expectedPatchedf3 =
            JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_3_patched_to_4_no_remove.json"), JsonObject.class);
    assertNotNull(expectedPatchedf3);

    // Check that the patched feature 3 has the correct content as 4 but no JSON properties deleted
    JSONAssert.assertEquals(expectedPatchedf3.serialize(), patchedf3.serialize(), JSONCompareMode.STRICT);
    final Difference newDiff = Patcher.getDifference(patchedf3, expectedPatchedf3);
    assertNull(newDiff);
  }

  @Test
  void testCompareSameArrayDifferentOrder() throws JSONException {
    final JsonObject f3 =
            JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_3.json"), JsonObject.class);
    assertNotNull(f3);
    final JsonObject f5 =
            JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_5.json"), JsonObject.class);
    assertNotNull(f5);
    final Difference diff35 = Patcher.getDifference(f3, f5);
    assertNotNull(diff35);
    assert (diff35 instanceof MapDiff);
    final MapDiff mapDiff35 = (MapDiff) diff35;
    assertEquals(2,mapDiff35.size());
    assertTrue(mapDiff35.get("array") instanceof ListDiff);
    assertTrue(mapDiff35.get("speedLimit") instanceof RemoveOp);
    final ListDiff nestedArrayDiff35 = (ListDiff) mapDiff35.get("array");
    // The patcher compares array element by element in order,
    // so the nested JSON in feature 3 is compared against the string in feature 5
    // and the string in feature 3 is against the nested JSON in feature 5
    assertTrue(nestedArrayDiff35.get(2) instanceof UpdateOp);
    assertTrue(nestedArrayDiff35.get(3) instanceof UpdateOp);
    assertTrue(nestedArrayDiff35.get(4) instanceof InsertOp);

    // Check that the patched feature 3 has the same content as 5
    final JsonObject patchedf3Tof5 = Patcher.patch(f3, diff35);
    JSONAssert.assertEquals(patchedf3Tof5.serialize(),f3.serialize(), JSONCompareMode.STRICT);
    final Difference newDiff = Patcher.getDifference(patchedf3Tof5, f5);
    assertNull(newDiff);
  }

  @Test
  void testPatchingOnlyShuffledArrayProvided() throws JSONException {
    final JsonObject f3 =
            JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_3.json"), JsonObject.class);
    assertNotNull(f3);
    // feature 6 only contains the same array in feature 3, but with the order of the elements changed
    final JsonObject f6 =
            JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_6.json"), JsonObject.class);
    assertNotNull(f6);

    final Difference diff36 = Patcher.getDifference(f3, f6);
    assertNotNull(diff36);
    // Simulate REST API behaviour, ignore all RemoveOp type of Difference
    final Difference diff36NoRemove = removeAllRemoveOp(diff36);
    final JsonObject patchedf3Tof6 = Patcher.patch(f3, diff36NoRemove);

    final JsonObject expectedPatchedf3 =
            JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_3_patched_with_6_no_remove_op.json"), JsonObject.class);
    assertNotNull(expectedPatchedf3);

    JSONAssert.assertEquals(expectedPatchedf3.serialize(),patchedf3Tof6.serialize(), JSONCompareMode.STRICT);
    final Difference newDiff36 = Patcher.getDifference(patchedf3Tof6, expectedPatchedf3);
    assertNull(newDiff36);
  }

  private static boolean ignoreAll(@NotNull Object key, @Nullable Map source, @Nullable Map target) {
    return true;
  }

  @Test
  void testIgnoreAll() {
    final XyzFeature f1 =
        JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_1.json"), XyzFeature.class);
    assertNotNull(f1);

    final XyzFeature f2 =
        JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_2.json"), XyzFeature.class);
    assertNotNull(f2);

    final Difference diff = Patcher.getDifference(f1, f2, PatcherTest::ignoreAll);
    assertNull(diff);
  }

  private static boolean ignoreXyzProps(@NotNull Object key, @Nullable Map source, @Nullable Map target) {
    if (source instanceof XyzNamespace || target instanceof XyzNamespace) {
      return "txn".equals(key)
          || "txn_next".equals(key)
          || "txn_uuid".equals(key)
          || "uuid".equals(key)
          || "puuid".equals(key)
          || "version".equals(key)
          || "rt_ts".equals(key)
          || "createdAt".equals(key)
          || "updatedAt".equals(key);
    }
    return false;
  }

  @Test
  void testXyzNamespace() {
    final XyzFeature f1 =
        JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_1.json"), XyzFeature.class);
    assertNotNull(f1);

    final XyzFeature f2 =
        JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_2.json"), XyzFeature.class);
    assertNotNull(f2);

    final Difference rawDiff = Patcher.getDifference(f1, f2, PatcherTest::ignoreXyzProps);

    final MapDiff feature = assertInstanceOf(MapDiff.class, rawDiff);
    assertEquals(1, feature.size());
    final MapDiff properties = assertInstanceOf(MapDiff.class, feature.get("properties"));
    assertEquals(1, properties.size());
    final MapDiff xyzNs = assertInstanceOf(MapDiff.class, properties.get("@ns:com:here:xyz"));
    assertEquals(2, xyzNs.size());
    final UpdateOp action = assertInstanceOf(UpdateOp.class, xyzNs.get("action"));
    assertEquals(EXyzAction.CREATE, action.oldValue());
    assertEquals(EXyzAction.UPDATE, action.newValue());
    final ListDiff tags = assertInstanceOf(ListDiff.class, xyzNs.get("tags"));
    assertEquals(23, tags.size());
    for (int i = 0; i < 22; i++) {
      assertNull(tags.get(i));
    }
    final InsertOp inserted = assertInstanceOf(InsertOp.class, tags.get(22));
    assertEquals("utm_dummy_update", inserted.newValue());
    assertNull(inserted.oldValue());
  }

  @ParameterizedTest
  @MethodSource("listDiffSamples")
  void testListDiff(List before, List after, ListDiff expectedResult){
    // When:
    Difference difference = Patcher.getDifference(before, after);

    // Then:
    Assertions.assertEquals(expectedResult, difference);
  }

  private static Stream<Arguments> listDiffSamples(){
    return Stream.of(
        arguments(
            List.of("one", "two"),
            List.of("one", "three"),
            listDiff(
                null,
                new UpdateOp("two","three")
            )
        ),
        arguments(
            List.of("one", "two", "three"),
            List.of("three", "four"),
            listDiff(
                new UpdateOp("one", "three"),
                new UpdateOp("two", "four"),
                new RemoveOp("three")
            )
        ),
        arguments(
            List.of("one", "two"),
            List.of("three", "four", "five"),
            listDiff(
                new UpdateOp("one", "three"),
                new UpdateOp("two", "four"),
                new InsertOp("five")
            )
        ),
        arguments(
            List.of(),
            List.of("one", "two", "three"),
            listDiff(
                new InsertOp("one"),
                new InsertOp("two"),
                new InsertOp("three")
            )
        ),
        arguments(
            List.of("one", "two", "three"),
            List.of(),
            listDiff(
                new RemoveOp("one"),
                new RemoveOp("two"),
                new RemoveOp("three")
            )
        )
    );
  }

  private static ListDiff listDiff(Difference... diffs){
    ListDiff listDiff = new ListDiff(diffs.length);
    listDiff.addAll(Arrays.asList(diffs));
    return listDiff;
  }
}
