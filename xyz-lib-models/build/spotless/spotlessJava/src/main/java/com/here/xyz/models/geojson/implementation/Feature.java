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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.INaksha;
import com.here.xyz.models.Typed;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.hub.StorageCollection;
import com.here.xyz.models.hub.pipelines.Space;
import com.here.xyz.models.hub.pipelines.Subscription;
import com.here.xyz.models.hub.plugins.EventHandler;
import com.here.xyz.models.hub.plugins.Storage;
import com.here.xyz.models.hub.transactions.TxSignal;
import com.here.xyz.util.diff.ConflictResolution;
import com.here.xyz.util.json.JsonObject;
import com.here.xyz.util.modify.IfExists;
import com.here.xyz.util.modify.IfNotExists;
import com.here.xyz.view.View.Export;
import com.here.xyz.view.View.Import;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** A standard GeoJson feature. */
@SuppressWarnings({"unused", "WeakerAccess"})
@JsonTypeName(value = "Feature")
@JsonSubTypes({
  // Pipelined:
  @JsonSubTypes.Type(value = Space.class),
  @JsonSubTypes.Type(value = Subscription.class),
  // Others:
  @JsonSubTypes.Type(value = EventHandler.class),
  @JsonSubTypes.Type(value = Storage.class),
  @JsonSubTypes.Type(value = StorageCollection.class),
  @JsonSubTypes.Type(value = TxSignal.class)
})
public class Feature extends JsonObject implements Typed {

  public static final String ID = "id";
  public static final String BBOX = "bbox";
  public static final String GEOMETRY = "geometry";
  public static final String PROPERTIES = "properties";
  public static final String ON_FEATURE_NOT_EXISTS = "onFeatureNotExists";
  public static final String ON_FEATURE_EXISTS = "onFeatureExists";
  public static final String ON_MERGE_CONFLICT = "onMergeConflict";

  @AvailableSince(INaksha.v2_0)
  public static final String PACKAGES = "packages";

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

  /** List of packages to which this feature belongs to; if any. */
  @AvailableSince(INaksha.v2_0)
  @JsonProperty(PACKAGES)
  @JsonInclude(Include.NON_EMPTY)
  private @Nullable List<@NotNull String> packages;

  /**
   * Replace the packages this feature is part of.
   *
   * @param packages the new package list.
   */
  @AvailableSince(INaksha.v2_0)
  public void setPackages(@Nullable List<@NotNull String> packages) {
    this.packages = packages;
  }

  /**
   * Returns the packages.
   *
   * @return the packages this features is part of.
   */
  @AvailableSince(INaksha.v2_0)
  public @NotNull List<@NotNull String> usePackages() {
    List<@NotNull String> packages = this.packages;
    if (packages == null) {
      this.packages = packages = new ArrayList<>();
    }
    return packages;
  }

  /**
   * Returns the packages.
   *
   * @return the packages this features is part of.
   */
  @AvailableSince(INaksha.v2_0)
  public @Nullable List<@NotNull String> getPackages() {
    return packages;
  }

  @JsonIgnore
  public @NotNull String getId() {
    return id;
  }

  @JsonSetter
  public void setId(@NotNull String id) {
    this.id = id;
  }

  @JsonIgnore
  public @Nullable BBox getBbox() {
    return bbox;
  }

  public void setBbox(@Nullable BBox bbox) {
    this.bbox = bbox;
  }

  @JsonIgnore
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
   * Validates the geometry of the feature and throws an exception if the geometry is invalid. This
   * method will not throw an exception if the geometry is missing, so null or undefined, but will
   * do so, when the geometry is somehow broken.
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
}
