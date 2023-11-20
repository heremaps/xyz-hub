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
package com.here.naksha.lib.core.models.storage;

import static com.here.naksha.lib.core.models.storage.OpType.AND;
import static com.here.naksha.lib.core.models.storage.OpType.NOT;
import static com.here.naksha.lib.core.models.storage.OpType.OR;
import static com.here.naksha.lib.core.models.storage.SOpType.INTERSECTS;

import com.here.naksha.lib.core.models.geojson.implementation.XyzGeometry;
import com.vividsolutions.jts.geom.Geometry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Spatial operations always executed against the geometry.
 */
public class SOp extends Op<SOp> {

  SOp(@NotNull OpType op, @NotNull SOp... children) {
    super(op, children);
    this.geometry = null;
  }

  SOp(@NotNull OpType op, @Nullable Geometry geometry) {
    super(op);
    this.geometry = geometry;
  }

  private final @Nullable Geometry geometry;

  public static @NotNull SOp and(@NotNull SOp... children) {
    return new SOp(AND, children);
  }

  public static @NotNull SOp or(@NotNull SOp... children) {
    return new SOp(OR, children);
  }

  public static @NotNull SOp not(@NotNull SOp op) {
    return new SOp(NOT, op);
  }

  /**
   * Returns an operation that tests for an intersection of features with the given geometry.
   *
   * @param geometry The geometry against which existing features should be tested for intersection.
   * @return The operation describing this.
   */
  public static @NotNull SOp intersects(@NotNull XyzGeometry geometry) {
    return intersects(geometry.getJTSGeometry());
  }

  /**
   * Returns an operation that tests for an intersection of features with the given geometry.
   *
   * @param geometry The geometry against which existing features should be tested for intersection.
   * @return The operation describing this.
   */
  public static @NotNull SOp intersects(@NotNull Geometry geometry) {
    return new SOp(INTERSECTS, geometry);
  }
}
