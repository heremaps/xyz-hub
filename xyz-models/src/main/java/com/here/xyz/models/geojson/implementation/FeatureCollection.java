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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.here.xyz.LazyParsable;
import com.here.xyz.LazyParsable.RawDeserializer;
import com.here.xyz.LazyParsable.RawSerializer;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.responses.XyzResponse;
import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "FeatureCollection")
@JsonInclude(Include.NON_EMPTY)
public class FeatureCollection extends XyzResponse<FeatureCollection> {

  private LazyParsable<List<Feature>> features;
  private BBox bbox;
  private String handle;
  private Long count;
  private List<String> inserted;
  private List<String> updated;
  private List<String> deleted;
  private List<Feature> oldFeatures;
  private List<ModificationFailure> failed;

  public FeatureCollection() {
    setFeatures(new ArrayList<>());
  }

  @SuppressWarnings("WeakerAccess")
  public void calculateAndSetBBox(boolean recalculateChildrenBoxes) throws JsonProcessingException {
    if (this.getFeatures() == null || this.getFeatures().size() == 0) {
      return;
    }

    double minLon = Double.POSITIVE_INFINITY;
    double minLat = Double.POSITIVE_INFINITY;
    double maxLon = Double.NEGATIVE_INFINITY;
    double maxLat = Double.NEGATIVE_INFINITY;

    for (Feature feature : getFeatures()) {
      if (recalculateChildrenBoxes || feature.getBbox() == null) {
        feature.calculateAndSetBbox(recalculateChildrenBoxes);
      }

      BBox bbox = feature.getBbox();

      if (bbox != null) {
        if (bbox.minLon() < minLon) {
          minLon = bbox.minLon();
        }
        if (bbox.minLat() < minLat) {
          minLat = bbox.minLat();
        }
        if (bbox.maxLon() > maxLon) {
          maxLon = bbox.maxLon();
        }
        if (bbox.maxLat() > maxLat) {
          maxLat = bbox.maxLat();
        }
      }
    }

    if (minLon != Double.POSITIVE_INFINITY && minLat != Double.POSITIVE_INFINITY && maxLon != Double.NEGATIVE_INFINITY
        && maxLat != Double.NEGATIVE_INFINITY) {
      setBbox(new BBox(minLon, minLat, maxLon, maxLat));
    } else {
      setBbox(null);
    }
  }

  public BBox getBbox() {
    return bbox;
  }

  public void setBbox(BBox bbox) {
    this.bbox = bbox;
  }

  @SuppressWarnings("unused")
  public FeatureCollection withBbox(final BBox bbox) {
    setBbox(bbox);
    return this;
  }

  public List<Feature> getFeatures() throws JsonProcessingException {
    return features != null ? this.features.get() : null;
  }

  public void setFeatures(List<Feature> features) {
    if (this.features == null) {
      this.features = new LazyParsable<>();
    }
    this.features.set(features);
  }

  @SuppressWarnings("unused")
  @JsonDeserialize(using = RawDeserializer.class)
  @JsonProperty("features")
  public void _setFeatures(Object features) {
    if (features instanceof String) {
      this.features = new LazyParsable<>((String) features);
    } else if (features instanceof List) {
      this.features = new LazyParsable<>();
      //noinspection unchecked
      this.features.set((List<Feature>) features);
    }
  }

  @JsonSerialize(using = RawSerializer.class)
  @JsonProperty("features")
  private LazyParsable<List<Feature>> _getFeatures() {
    if (features == null) {
      features = new LazyParsable<>("[]");
    }

    return features;
  }

  @SuppressWarnings("unused")
  public FeatureCollection withFeatures(final List<Feature> features) {
    setFeatures(features);
    return this;
  }

  /**
   * Returns the Space handle which is used to iterate above data.
   *
   * @return the handle.
   */
  public String getHandle() {
    return this.handle;
  }

  /**
   * Sets the Space handle that can be used to continue an iterate.
   *
   * @param handle the handle, if null the handle property is removed.
   */
  @SuppressWarnings("WeakerAccess")
  public void setHandle(String handle) {
    this.handle = handle;
  }

  @SuppressWarnings("unused")
  public FeatureCollection withHandle(final String handle) {
    setHandle(handle);
    return this;
  }

  /**
   * Returns the proprietary count property that is used by Space count requests to return the number of features found.
   *
   * @return the amount of features that are matching the query.
   */
  public Long getCount() {
    return this.count;
  }

  /**
   * Sets the amount of features that where matching a query, without returning the features (so features will be null or an empty array).
   *
   * @param count the amount of features that where matching a query, if null, then the property is removed.
   */
  @SuppressWarnings("WeakerAccess")
  public void setCount(Long count) {
    this.count = count;
  }

  @SuppressWarnings("unused")
  public FeatureCollection withCount(final Long count) {
    setCount(count);
    return this;
  }

  /**
   * @return list of features IDs of those features that where successfully inserted.
   */
  public List<String> getInserted() {
    return this.inserted;
  }

  /**
   * Sets the list of successfully inserted feature IDs.
   *
   * @param inserted the IDs of the features that where inserted.
   */
  @SuppressWarnings("WeakerAccess")
  public void setInserted(List<String> inserted) {
    this.inserted = inserted;
  }

  @SuppressWarnings("unused")
  public FeatureCollection withInserted(List<String> inserted) {
    setInserted(inserted);
    return this;
  }

  /**
   * @return list of features IDs of those features that where successfully updated.
   */
  public List<String> getUpdated() {
    return this.updated;
  }

  /**
   * Sets the list of successfully updated feature IDs.
   *
   * @param updated the IDs of the features that where updated.
   */
  @SuppressWarnings("WeakerAccess")
  public void setUpdated(List<String> updated) {
    this.updated = updated;
  }

  @SuppressWarnings("unused")
  public FeatureCollection withUpdated(List<String> updated) {
    setUpdated(updated);
    return this;
  }

  /**
   * @return list of features IDs of those features that where successfully deleted.
   */
  public List<String> getDeleted() {
    return this.deleted;
  }

  /**
   * Sets the list of successfully deleted feature IDs.
   *
   * @param deleted the IDs of the features that where deleted.
   */
  @SuppressWarnings("WeakerAccess")
  public void setDeleted(List<String> deleted) {
    this.deleted = deleted;
  }

  @SuppressWarnings("unused")
  public FeatureCollection withDeleted(List<String> deleted) {
    setDeleted(deleted);
    return this;
  }

  /**
   * @return A list of modification failures
   */
  public List<ModificationFailure> getFailed() {
    return this.failed;
  }

  @SuppressWarnings("WeakerAccess")
  public void setFailed(List<ModificationFailure> failed) {
    this.failed = failed;
  }

  @SuppressWarnings("unused")
  public FeatureCollection withFailed(List<ModificationFailure> failed) {
    setFailed(failed);
    return this;
  }

  @SuppressWarnings("unused")
  public List<Feature> getOldFeatures() {
    return oldFeatures;
  }

  @SuppressWarnings("WeakerAccess")
  public void setOldFeatures(List<Feature> oldFeatures) {
    this.oldFeatures = oldFeatures;
  }

  @SuppressWarnings("unused")
  public FeatureCollection withOldFeatures(List<Feature> oldFeatures) {
    setOldFeatures(oldFeatures);
    return this;
  }

  public static class ModificationFailure {

    private String id;
    private Long position;
    private String message;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    @SuppressWarnings("unused")
    public ModificationFailure withId(String id) {
      setId(id);
      return this;
    }

    @SuppressWarnings("unused")
    public Long getPosition() {
      return position;
    }

    @SuppressWarnings("WeakerAccess")
    public void setPosition(Long position) {
      this.position = position;
    }

    @SuppressWarnings("unused")
    public ModificationFailure withPosition(Long position) {
      setPosition(position);
      return this;
    }

    @SuppressWarnings("unused")
    public String getMessage() {
      return message;
    }

    @SuppressWarnings("WeakerAccess")
    public void setMessage(String message) {
      this.message = message;
    }

    @SuppressWarnings("unused")
    public ModificationFailure withMessage(String message) {
      setMessage(message);
      return this;
    }
  }
}
