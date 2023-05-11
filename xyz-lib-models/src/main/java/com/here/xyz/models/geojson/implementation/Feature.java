/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.models.geojson.implementation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.Extensible;
import com.here.xyz.Typed;
import com.here.xyz.View.All;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Subscription;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A standard GeoJson feature.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings({"unused", "WeakerAccess"})
@JsonTypeName(value = "Feature")
@JsonSubTypes({ // Note: These types need to be added as well into Typed!
    @JsonSubTypes.Type(value = Space.class, name = "Space"),
    @JsonSubTypes.Type(value = Connector.class, name = "Connector"),
    @JsonSubTypes.Type(value = Subscription.class, name = "Subscription")
})
public class Feature extends Extensible implements Typed {
  public static final String ID = "id";
  public static final String BBOX = "bbox";
  public static final String GEOMETRY = "geometry";
  public static final String PROPERTIES = "properties";

  /**
   * Create a new empty feature.
   */
  public Feature() {
    this.properties = new Properties();
  }

  @JsonProperty
  @JsonView(All.class)
  protected String id;

  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_NULL)
  protected BBox bbox;

  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_NULL)
  protected Geometry geometry;

  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_NULL)
  protected @NotNull Properties properties;

  public @Nullable String getId() {
    return id;
  }

  public void setId(@Nullable String id) {
    this.id = id;
  }

  public @Nullable BBox getBbox() {
    return bbox;
  }

  public void setBbox(@Nullable BBox bbox) {
    this.bbox = bbox;
  }

  public @Nullable Geometry getGeometry() {
    return geometry;
  }

  public void setGeometry(@Nullable Geometry geometry) {
    this.geometry = geometry;
  }

  public @NotNull Properties getProperties() {
    return properties;
  }

  public void setProperties(@NotNull Properties properties) {
    this.properties = properties;
  }

  public void calculateAndSetBbox(boolean recalculateBBox) {
    if (!recalculateBBox && getBbox() != null) {
      return;
    }

    final Geometry geometry = getGeometry();
    if (geometry == null) {
      setBbox(null);
    } else {
      setBbox(geometry.calculateBBox());
    }
  }

  /**
   * Validates the geometry of the feature and throws an exception if the geometry is invalid. This method will not throw an exception if
   * the geometry is missing, so null or undefined, but will do so, when the geometry is somehow broken.
   *
   * @throws InvalidGeometryException if the geometry is invalid.
   */
  public void validateGeometry() throws InvalidGeometryException {
    final Geometry geometry = getGeometry();
    if (geometry == null) {
      // This is valid, the feature simply does not have any geometry.
      return;
    }

    if (geometry instanceof GeometryCollection) {
      throw new InvalidGeometryException("GeometryCollection is not supported.");
    }

    geometry.validate();
  }

  @Deprecated
  public static void finalizeFeature(@NotNull Feature feature, String spaceId, boolean addUUID) {

  }
}
