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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.Extensible;
import com.here.xyz.Typed;
import com.here.xyz.View.All;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "Feature")
@SuppressWarnings({"unused", "WeakerAccess"})
public class Feature extends Extensible<Feature> implements Typed {

  @JsonProperty
  @JsonView(All.class)
  private String id;

  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_NULL)
  private BBox bbox;

  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_NULL)
  private Geometry geometry;

  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_NULL)
  private Properties properties;

  /**
   * Create a new empty feature.
   */
  public Feature() {
    super();
  }

  /**
   * Creates a new Feature object, for which the properties and the xyz namespace are initialized.
   *
   * @return the feature with initialized properties and xyz namespace.
   */
  public static Feature createEmptyFeature() {
    return new Feature().withProperties(new Properties().withXyzNamespace(new XyzNamespace()));
  }

  /**
   * Updates the {@link XyzNamespace} properties in a feature so that all values are valid.
   *
   * @param feature the feature for which to update the {@link XyzNamespace} map.
   * @param space the full qualified space identifier in which this feature is stored (so prefix and base part combined, e.g. "x-foo").
   * @param addUUID If the uuid to be added or not.
   * @throws NullPointerException if feature, space or the 'id' of the feature are null.
   */
  @SuppressWarnings("WeakerAccess")
  public static void finalizeFeature(final Feature feature, final String space, boolean addUUID) throws NullPointerException {
    final XyzNamespace xyzNamespace = feature.getProperties().getXyzNamespace();

    if (addUUID) {
      String puuid = xyzNamespace.getUuid();
      if (puuid != null) {
        xyzNamespace.setPuuid(puuid);
      }
      xyzNamespace.setUuid(UUID.randomUUID().toString());
    }
  }

  public @Nullable String getId() {
    return id;
  }

  public void setId(@Nullable String id) {
    this.id = id;
  }

  public @NotNull Feature withId(@Nullable String id) {
    setId(id);
    return this;
  }

  public @NotNull Feature withProperties(@Nullable Properties properties) {
    setProperties(properties);
    return this;
  }

  public @Nullable BBox getBbox() {
    return bbox;
  }

  public void setBbox(@Nullable BBox bbox) {
    this.bbox = bbox;
  }

  public @NotNull Feature withBbox(@Nullable BBox bbox) {
    setBbox(bbox);
    return this;
  }

  public @Nullable Geometry getGeometry() {
    return geometry;
  }

  public void setGeometry(@Nullable Geometry geometry) {
    this.geometry = geometry;
  }

  public @NotNull Feature withGeometry(@Nullable Geometry geometry) {
    setGeometry(geometry);
    return this;
  }

  public @Nullable Properties getProperties() {
    return properties;
  }

  public void setProperties(@Nullable Properties properties) {
    this.properties = properties;
  }

  public @NotNull Properties useProperties() {
    if (properties == null) {
      properties = new Properties();
    }
    return properties;
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

    if(geometry instanceof GeometryCollection)
      throw new InvalidGeometryException("GeometryCollection is not supported.");

    geometry.validate();
  }
}
