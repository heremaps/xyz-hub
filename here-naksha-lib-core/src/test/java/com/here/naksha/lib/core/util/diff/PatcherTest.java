package com.here.naksha.lib.core.util.diff;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.here.naksha.lib.core.models.geojson.implementation.Feature;
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
    final Feature f1 = JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_1.json"), Feature.class);
    assertNotNull(f1);

    final Feature f2 = JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_2.json"), Feature.class);
    assertNotNull(f2);

    final Difference diff = Patcher.getDifference(f1, f2);
    assertNotNull(diff);

    final Feature f1_patched_to_f2 = Patcher.patch(f1, diff);
    assertNotNull(f1_patched_to_f2);

    final Difference newDiff = Patcher.getDifference(f1_patched_to_f2, f2);
    assertNull(newDiff);
  }

  private static boolean ignoreAll(@NotNull Object key, @Nullable Map source, @Nullable Map target) {
    return true;
  }

  @Test
  void testIgnoreAll() throws IOException {
    final Feature f1 = JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_1.json"), Feature.class);
    assertNotNull(f1);

    final Feature f2 = JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_2.json"), Feature.class);
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
    final Feature f1 = JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_1.json"), Feature.class);
    assertNotNull(f1);

    final Feature f2 = JsonSerializable.deserialize(IoHelp.readResource("patcher/feature_2.json"), Feature.class);
    assertNotNull(f2);

    final Difference rawDiff = Patcher.getDifference(f1, f2, PatcherTest::ignoreXyzProps);

    final MapDiff feature = assertInstanceOf(MapDiff.class, rawDiff);
    assertEquals(1, feature.size());
    final MapDiff properties = assertInstanceOf(MapDiff.class, feature.get("properties"));
    assertEquals(1, properties.size());
    final MapDiff xyzNs = assertInstanceOf(MapDiff.class, properties.get("@ns:com:here:xyz"));
    assertEquals(2, xyzNs.size());
    final UpdateOp action = assertInstanceOf(UpdateOp.class, xyzNs.get("action"));
    assertEquals("CREATE", action.oldValue());
    assertEquals("UPDATE", action.newValue());
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
