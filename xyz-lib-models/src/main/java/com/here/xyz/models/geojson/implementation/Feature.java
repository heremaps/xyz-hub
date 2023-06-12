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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.Typed;
import com.here.xyz.util.JsonObject;
import com.here.xyz.view.View.Export;
import com.here.xyz.view.View.Import;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Subscription;
import com.here.xyz.util.diff.ConflictResolution;
import com.here.xyz.util.modify.IfExists;
import com.here.xyz.util.modify.IfNotExists;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A standard GeoJson feature.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
@JsonTypeName(value = "Feature")
@JsonSubTypes({ // Note: These types need to be added as well into Typed!
    @JsonSubTypes.Type(value = Space.class, name = "Space"),
    @JsonSubTypes.Type(value = Connector.class, name = "Connector"),
    @JsonSubTypes.Type(value = Subscription.class, name = "Subscription")
})
public class Feature extends JsonObject implements Typed {

  public static final String ID = "id";
  public static final String BBOX = "bbox";
  public static final String GEOMETRY = "geometry";
  public static final String PROPERTIES = "properties";
  public static final String ON_FEATURE_NOT_EXISTS = "onFeatureNotExists";
  public static final String ON_FEATURE_EXISTS = "onFeatureExists";
  public static final String ON_MERGE_CONFLICT = "onMergeConflict";

  /**
   * Create a new empty feature.
   *
   * @param id The ID; if {@code null}, then a random one is generated.
   */
  @JsonCreator
  public Feature(@JsonProperty(ID) @Nullable String id) {
    this.id = id != null && id.length() > 0 ? id : RandomStringUtils.randomAlphabetic(12);
    this.properties = new Properties();
  }

  @JsonProperty(ID)
  protected @NotNull String id;

  @JsonProperty(BBOX)
  protected BBox bbox;

  @JsonProperty(GEOMETRY)
  protected Geometry geometry;

  @JsonProperty(PROPERTIES)
  protected @NotNull Properties properties;

  // Serialize and deserialize internally, do not store in the database, but accept from external.
  @JsonProperty(ON_FEATURE_NOT_EXISTS)
  @JsonView({Export.Private.class, Import.Public.class})
  protected @Nullable IfNotExists onFeatureNotExists;

  // Serialize and deserialize internally, do not store in the database, but accept from external.
  @JsonProperty(ON_FEATURE_EXISTS)
  @JsonView({Export.Private.class, Import.Public.class})
  protected @Nullable IfExists onFeatureExists;

  // Serialize and deserialize internally, do not store in the database, but accept from external.
  @JsonProperty(ON_MERGE_CONFLICT)
  @JsonView({Export.Private.class, Import.Public.class})
  protected @Nullable ConflictResolution onMergeConflict;

  public @NotNull String getId() {
    return id;
  }

  public void setId(@NotNull String id) {
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

  @JsonGetter
  public @NotNull Properties getProperties() {
    return properties;
  }

  @JsonSetter
  public void setProperties(@NotNull Properties properties) {
    this.properties = properties;
  }

  public @Nullable IfNotExists getOnFeatureNotExists() {
    return onFeatureNotExists;
  }

  public void setOnFeatureNotExists(@Nullable IfNotExists onFeatureNotExists) {
    this.onFeatureNotExists = onFeatureNotExists;
  }

  public @Nullable IfExists getOnFeatureExists() {
    return onFeatureExists;
  }

  public void setOnFeatureExists(@Nullable IfExists onFeatureExists) {
    this.onFeatureExists = onFeatureExists;
  }

  public @Nullable ConflictResolution getOnMergeConflict() {
    return onMergeConflict;
  }

  public void setOnMergeConflict(@Nullable ConflictResolution onMergeConflict) {
    this.onMergeConflict = onMergeConflict;
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
