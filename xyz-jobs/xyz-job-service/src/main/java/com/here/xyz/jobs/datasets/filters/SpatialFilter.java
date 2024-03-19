/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.jobs.datasets.filters;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.XyzSerializable.Public;
import com.here.xyz.models.geojson.implementation.Geometry;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class SpatialFilter {

  @JsonView({Public.class})
  private Geometry geometry;

  @JsonView({Public.class})
  private int radius;

  @JsonView({Public.class})
  private boolean clip;

  public Geometry getGeometry() {
    return geometry;
  }

  public void setGeometry(Geometry geometry) {
    this.geometry = geometry;
  }

  public SpatialFilter withGeometry(Geometry geometry) {
    this.setGeometry(geometry);
    return this;
  }

  public int getRadius() {
    return radius;
  }

  public void setRadius(int radius) {
    this.radius = radius;
  }

  public SpatialFilter withRadius(final int radius) {
    setRadius(radius);
    return this;
  }

  /**
   * @deprecated Use {@link #isClip()} instead.
   */
  @Deprecated
  public boolean isClipped() {
    return isClip();
  }

  /**
   * @deprecated Use {@link #setClip(boolean)} instead.
   */
  @Deprecated
  public void setClipped(boolean clipped) {
    setClip(clipped);
  }

  /**
   * @deprecated Use {@link #withClip(boolean)} instead.
   */
  @Deprecated
  public SpatialFilter withClipped(final boolean clipped) {
    return withClip(clipped);
  }

  public boolean isClip() {
    return clip;
  }

  public void setClip(boolean clipped) {
    this.clip = clipped;
  }

  public SpatialFilter withClip(final boolean clipped) {
    setClipped(clipped);
    return this;
  }
}
