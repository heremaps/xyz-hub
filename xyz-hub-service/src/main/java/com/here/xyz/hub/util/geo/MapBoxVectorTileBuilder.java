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

package com.here.xyz.hub.util.geo;


import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerProps;
import io.vertx.core.json.Json;
import java.util.List;
import java.util.Map;

/**
 * A helper class to build a pixel based MapBox Vector Tiles.
 */
public class MapBoxVectorTileBuilder extends MvtTileBuilder {

  /**
   * Create a new empty tile creator.
   */
  public MapBoxVectorTileBuilder() { super(); }

  @SuppressWarnings("unchecked")
  protected void addList(final String prefix, final List<Object> list) {
    if (list == null) {
      return;
    }
    for (int i = 0; i < list.size(); i++) {
      final Object raw = list.get(i);
      if (raw == null) {
        continue;
      }

      if (raw instanceof Map) {
        addProperties(newPrefix(prefix, "" + i), (Map<String, Object>) raw);
        continue;
      }

      if (raw instanceof List) {
        addList(newPrefix(prefix, "" + i), (List<Object>) raw);
        continue;
      }

      addProperty(prefix, "" + i, raw);
    }
  }

  protected void addProperties(final String prefix, final Map<String, Object> map) {
    if (map == null) {
      return;
    }
    for (final String key : map.keySet()) {
      // When this is the root map, so it is the feature object itself, ignore two special keys.
      if (prefix.length() == 0 && ("geometry".equals(key) || "type".equals(key))) {
        continue;
      }

      final Object raw = map.get(key);
      if (raw == null) {
        continue;
      }

      if (raw instanceof Map || raw instanceof List) {
        addProperty(prefix, key, Json.encode(raw));
      } else {
        addProperty(prefix, key, raw);
      }
    }
  }

  @Override
  public void doAddTags(final Object userData, final MvtLayerProps layerProps, final VectorTile.Tile.Feature.Builder featureBuilder) {
    // TODO: We need to agree how to encode the properties that are no number and no string.
    // https://www.mapbox.com/vector-tiles/specification/#how-to-encode-attributes-that-arent-strings-or-numbers

    // Add all properties of the feature recursively.
    this.layerProps = layerProps;
    this.featureBuilder = featureBuilder;

    addProperty("", "id", currentFeature().getId());
    if (currentFeature().getProperties() != null) {
      addProperties("", currentFeature().getProperties().toMap());
    }
  }

}
