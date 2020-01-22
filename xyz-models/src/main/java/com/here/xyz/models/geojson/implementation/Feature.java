/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.Extensible;
import com.here.xyz.Typed;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "Feature")
@SuppressWarnings({"unused", "WeakerAccess"})
public class Feature extends Extensible<Feature> implements Typed {

  private String id;
  private BBox bbox;
  private Geometry geometry;
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
  public static void finalizeFeature(final Feature feature, final String space, final long timestamp, boolean addUUID) throws NullPointerException {
    if (feature == null) {
      throw new NullPointerException("feature");
    }
    if (space == null) {
      throw new NullPointerException("space");
    }

    if (feature.getProperties() == null) {
      feature.setProperties(new Properties());
    }

    final Properties props = feature.getProperties();
    if (props.getXyzNamespace() == null) {
      props.setXyzNamespace(new XyzNamespace());
    }

    final XyzNamespace nsXyz = props.getXyzNamespace();
    nsXyz.setSpace(space);

    final List<String> tags = nsXyz.getTags();

    if (tags != null) {
      XyzNamespace.normalizeTagsOfFeature(feature);
    } else {
      nsXyz.setTags(new ArrayList<>());
    }
    if (nsXyz.getCreatedAt() == 0) {
      nsXyz.setCreatedAt(timestamp);
    }
    nsXyz.setUpdatedAt(timestamp);
    if (addUUID) {
      String puuid = nsXyz.getUuid();
      if (puuid != null) {
        nsXyz.setPuuid(puuid);
      }
      nsXyz.setUuid(UUID.randomUUID().toString());
    }

    nsXyz.setInputPosition(null);
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Feature withId(String id) {
    setId(id);
    return this;
  }

  public Feature withProperties(final Properties properties) {
    setProperties(properties);
    return this;
  }

  public BBox getBbox() {
    return bbox;
  }

  public BBox setBbox(BBox bbox) {
    BBox old = this.bbox;
    this.bbox = bbox;
    return old;
  }

  public Feature withBbox(final BBox bbox) {
    setBbox(bbox);
    return this;
  }

  public Geometry getGeometry() {
    return geometry;
  }

  public Geometry setGeometry(Geometry geometry) {
    Geometry old = this.geometry;
    this.geometry = geometry;
    return old;
  }

  public Feature withGeometry(final Geometry geometry) {
    setGeometry(geometry);
    return this;
  }

  public Properties getProperties() {
    return properties;
  }

  public Properties setProperties(Properties properties) {
    Properties old = this.properties;
    this.properties = properties;
    return old;
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

    geometry.validate();
  }
}
