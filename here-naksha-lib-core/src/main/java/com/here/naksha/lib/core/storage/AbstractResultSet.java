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
package com.here.naksha.lib.core.storage;

import com.here.naksha.lib.core.models.geojson.coordinates.JTSHelper;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzGeometry;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.view.ViewDeserialize.Storage;
import com.here.naksha.lib.core.view.ViewSerialize;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A special result-set for all read operations, it keeps the features serialized until deserialization needed. This allows as well to the
 * raw strings, when the results anyway only should be transferred somewhere else, what avoids one needless deserialization and
 * re-serialization.
 *
 * @param <FEATURE> The feature-type.
 */
public abstract class AbstractResultSet<FEATURE extends XyzFeature> implements IResultSet<FEATURE> {

  /**
   * Create a new result-set for the given feature-type.
   *
   * @param featureClass the class of the feature-type.
   */
  protected AbstractResultSet(@NotNull Class<FEATURE> featureClass) {
    this.featureClass = featureClass;
    this.json = Json.get();
  }

  protected @NotNull Class<FEATURE> featureClass;
  protected @NotNull Json json;

  /**
   * Convert the JSON and geometry into a real feature.
   *
   * @param jsondata the JSON-string of the feature.
   * @param geo      the hex-string of the WKB of the geometry; if the feature does have a geometry.
   * @return the feature.
   * @throws NoSuchElementException if no feature loaded.
   * @throws Exception              if any error occurred while parsing the feature or geometry.
   */
  protected @NotNull FEATURE featureOf(@NotNull String jsondata, @Nullable String geo) throws Exception {
    final FEATURE f = json.reader(Storage.class).forType(featureClass).readValue(jsondata);
    if (f == null) {
      throw new IllegalArgumentException("Parsing the jsondata returned null");
    }
    if (geo != null) {
      final Geometry geometry = json.wkbReader.read(WKBReader.hexToBytes(geo));
      f.setGeometry(JTSHelper.fromGeometry(geometry));
    }
    return f;
  }

  protected @NotNull String jsonOf(@NotNull FEATURE feature) throws Exception {
    final XyzGeometry xyzGeometry = feature.getGeometry();
    feature.setGeometry(null);
    try {
      return json.writer(ViewSerialize.Storage.class)
          .forType(featureClass)
          .writeValueAsString(feature);
    } finally {
      feature.setGeometry(xyzGeometry);
    }
  }

  protected @Nullable String geometryOf(@NotNull FEATURE feature) throws Exception {
    final XyzGeometry xyzGeometry = feature.getGeometry();
    feature.setGeometry(null);
    try {
      if (xyzGeometry != null) {
        final Geometry jtsGeometry = xyzGeometry.getJTSGeometry();
        assure3d(jtsGeometry.getCoordinates());
        final byte[] geometryBytes = json.wkbWriter.write(jtsGeometry);
        return WKBWriter.toHex(geometryBytes);
      }
      return null;
    } finally {
      feature.setGeometry(xyzGeometry);
    }
  }

  protected static void assure3d(@NotNull Coordinate @NotNull [] coords) {
    for (final @NotNull Coordinate coord : coords) {
      if (coord.z != coord.z) { // if coord.z is NaN
        coord.z = 0;
      }
    }
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public void close() {
    if (json != null) {
      json.close();
      json = null;
      featureClass = null;
    }
  }
}
