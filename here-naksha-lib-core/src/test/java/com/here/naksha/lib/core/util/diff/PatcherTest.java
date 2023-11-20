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

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.here.naksha.lib.core.models.geojson.implementation.EXyzAction;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.EChangeState;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.util.IoHelp;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import java.io.IOException;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

@SuppressWarnings({"rawtypes", "ConstantConditions"})
class PatcherTest {

  @Test
  void basic() throws IOException {
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

  private static boolean ignoreAll(@NotNull Object key, @Nullable Map source, @Nullable Map target) {
    return true;
  }

  @Test
  void testIgnoreAll() throws IOException {
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
      return "puuid".equals(key)
          || "txn".equals(key)
          || "uuid".equals(key)
          || "version".equals(key)
          || "rtuts".equals(key)
          || "createdAt".equals(key)
          || "updatedAt".equals(key);
    }
    return false;
  }

  @Test
  void testXyzNamespace() throws IOException {
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
}
