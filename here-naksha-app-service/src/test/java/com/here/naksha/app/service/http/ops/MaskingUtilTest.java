package com.here.naksha.app.service.http.ops;

import static org.junit.jupiter.api.Assertions.*;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class MaskingUtilTest {

  @Test
  void shouldMaskProperties(){
    // Given
    XyzFeature feature = featureWithProps(mutableMapOf(
        "Authorization", mutableMapOf(
            "some_entry_1", 123,
            "some_entry_2", "lorem ipsum"
        ),
        "headers", mutableMapOf(
            "Authorization", "secret stuff, do not look",
            "Content-Type", "application/json"
        ),
        "customValProperties", mutableMapOf(
                    "mod.extension-database.password", "pwd"
            ),
        "very", mutableMapOf(
            "nested", mutableMapOf(
                "map", mutableMapOf(
                    "to", mutableMapOf(
                        "authorization", mutableMapOf(
                            "foo", "bar"
                        )
                    )
                )
            )
        )
    ));

    // When:
    MaskingUtil.maskProperties(feature);

    // Then:
    assertEquals(Map.of(
        "Authorization", MaskingUtil.MASK,
        "headers", Map.of(
            "Authorization", MaskingUtil.MASK,
            "Content-Type", "application/json"
        ),
        "customValProperties", Map.of(
                 "mod.extension-database.password", MaskingUtil.MASK
        ),
        "very", Map.of(
            "nested", Map.of(
                "map", Map.of(
                    "to", Map.of(
                        "authorization", MaskingUtil.MASK
                    )
                )
            )
        )
    ), feature.getProperties().asMap());
  }

  private static XyzFeature featureWithProps(Map<String, Object> props){
    XyzFeature xyzFeature = new XyzFeature();
    xyzFeature.getProperties().putAll(props);
    return xyzFeature;
  }

  // We use this instead of simple `Map::of` because `MaskingUtil` relies on properties' `entrySet`
  // `Map::of` return immutable map, which entries do not support `Entry::setValue` method
  private static Map<String, Object> mutableMapOf(Object... args){
    HashMap<String, Object> map = new HashMap<>();
    for(int i = 0; i < args.length; i += 2){
      map.put(args[i].toString(), args[i+1]);
    }
    return map;
  }
}