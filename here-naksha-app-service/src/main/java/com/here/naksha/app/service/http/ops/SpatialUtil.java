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
package com.here.naksha.app.service.http.ops;

import static com.here.naksha.app.service.http.apis.ApiParams.TILE_TYPE_QUADKEY;

import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.WebMercatorTile;
import org.jetbrains.annotations.NotNull;
import org.locationtech.jts.geom.Geometry;

public class SpatialUtil {

  private SpatialUtil() {}

  public static @NotNull Geometry buildGeometryForTile(
      final @NotNull String tileType, final @NotNull String tileId, final int margin) {
    try {
      if (!TILE_TYPE_QUADKEY.equals(tileType)) {
        throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Tile type " + tileType + " not supported");
      }
      return WebMercatorTile.forQuadkey(tileId)
          .getExtendedBBoxAsPolygon(margin)
          .getGeometry();
    } catch (Exception ex) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Error interpreting tile input: " + ex.getMessage());
    }
  }
}
