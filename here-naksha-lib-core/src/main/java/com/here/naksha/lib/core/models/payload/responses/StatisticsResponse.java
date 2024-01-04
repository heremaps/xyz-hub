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
package com.here.naksha.lib.core.models.payload.responses;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.models.geojson.coordinates.BBox;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.info.GetStatisticsEvent;
import java.util.List;

/** The response that is sent for a {@link GetStatisticsEvent}. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "StatisticsResponse")
public class StatisticsResponse extends XyzResponse {

  private Value<Long> count;

  @Deprecated
  private Value<Long> byteSize;

  private Value<Long> dataSize;
  private Value<BBox> bbox;
  private PropertiesStatistics properties;
  private Value<List<PropertyStatistics>> tags;
  private Value<List<String>> geometryTypes;

  /**
   * Returns the amount of features stored in the space.
   *
   * @return the amount of features stored in the space.
   */
  @SuppressWarnings("unused")
  public Value<Long> getCount() {
    return this.count;
  }

  /**
   * Sets the amount of features stored in the space.
   *
   * @param count the amount of features stored in the space.
   */
  @SuppressWarnings({"unused", "WeakerAccess"})
  public void setCount(Value<Long> count) {
    this.count = count;
  }

  /**
   * Sets the amount of features stored in the space.
   *
   * @return this.
   */
  @SuppressWarnings("unused")
  public StatisticsResponse withCount(Value<Long> count) {
    setCount(count);
    return this;
  }

  /**
   * Returns the amount of bytes that are stored in the space.
   *
   * @return the amount of bytes that are stored in the space.
   * @deprecated use {@link #getDataSize()} instead.
   */
  @SuppressWarnings({"unused"})
  public Value<Long> getByteSize() {
    return this.byteSize;
  }

  /**
   * Sets the amount of bytes that are stored in the space.
   *
   * @param byteSize the amount of bytes that are stored in the space.
   * @deprecated use {@link #setDataSize(Value)} instead.
   */
  @SuppressWarnings({"WeakerAccess"})
  public void setByteSize(Value<Long> byteSize) {
    this.byteSize = byteSize;
  }

  /**
   * Sets the amount of bytes that are stored in the space.
   *
   * @return this.
   * @deprecated use {@link #withDataSize(Value)} instead.
   */
  @SuppressWarnings({"unused"})
  public StatisticsResponse withByteSize(Value<Long> byteSize) {
    setByteSize(byteSize);
    return this;
  }

  /**
   * Returns the amount of bytes that are stored in the space.
   *
   * @return the amount of bytes that are stored in the space.
   */
  @SuppressWarnings({"unused"})
  public Value<Long> getDataSize() {
    return this.dataSize;
  }

  /**
   * Sets the amount of bytes that are stored in the space.
   *
   * @param dataSize the amount of bytes that are stored in the space.
   */
  @SuppressWarnings({"WeakerAccess"})
  public void setDataSize(Value<Long> dataSize) {
    this.dataSize = dataSize;
  }

  /**
   * Sets the amount of bytes that are stored in the space.
   *
   * @return this.
   */
  @SuppressWarnings({"unused"})
  public StatisticsResponse withDataSize(Value<Long> dataSize) {
    setDataSize(dataSize);
    return this;
  }

  /**
   * Returns the most outer bounding box around all features being within the space.
   *
   * @return the most outer bounding box around all features being within the space; null if no
   *     feature is in the space.
   */
  public Value<BBox> getBbox() {
    return this.bbox;
  }

  /**
   * Sets the most outer bounding box around all features being within the space.
   *
   * @param bbox the bbox value to be set
   */
  public void setBbox(Value<BBox> bbox) {
    this.bbox = bbox;
  }

  /**
   * Sets the bbox for this space.
   *
   * @return this.
   */
  @SuppressWarnings({"unused"})
  public StatisticsResponse withBBox(Value<BBox> bbox) {
    setBbox(bbox);
    return this;
  }

  public PropertiesStatistics getProperties() {
    return properties;
  }

  public void setProperties(PropertiesStatistics properties) {
    this.properties = properties;
  }

  @SuppressWarnings("unused")
  public StatisticsResponse withProperties(PropertiesStatistics properties) {
    setProperties(properties);
    return this;
  }

  public Value<List<PropertyStatistics>> getTags() {
    return tags;
  }

  public void setTags(Value<List<PropertyStatistics>> tags) {
    this.tags = tags;
  }

  @SuppressWarnings("unused")
  public StatisticsResponse withTags(Value<List<PropertyStatistics>> tags) {
    setTags(tags);
    return this;
  }

  @SuppressWarnings("unused")
  public Value<List<String>> getGeometryTypes() {
    return geometryTypes;
  }

  @SuppressWarnings("WeakerAccess")
  public void setGeometryTypes(Value<List<String>> geometryTypes) {
    this.geometryTypes = geometryTypes;
  }

  @SuppressWarnings("unused")
  public StatisticsResponse withGeometryTypes(Value<List<String>> geometryTypes) {
    setGeometryTypes(geometryTypes);
    return this;
  }

  public static class Value<T> {

    T value;
    Boolean estimated;

    public Value() {}
    ;

    public Value(T value) {
      this.value = value;
    }

    public T getValue() {
      return this.value;
    }

    public void setValue(T value) {
      this.value = value;
    }

    @SuppressWarnings({"unused", "unchecked"})
    public <E extends Value<T>> E withValue(T value) {
      setValue(value);
      return (E) this;
    }

    @SuppressWarnings("unused")
    public Boolean getEstimated() {
      return this.estimated;
    }

    @SuppressWarnings("WeakerAccess")
    public void setEstimated(Boolean estimated) {
      this.estimated = estimated;
    }

    @SuppressWarnings({"unused", "unchecked"})
    public <E extends Value<T>> E withEstimated(Boolean estimated) {
      setEstimated(estimated);
      return (E) this;
    }
  }

  public static class PropertiesStatistics extends Value<List<PropertyStatistics>> {

    private Searchable searchable = Searchable.NONE;

    @SuppressWarnings("unused")
    public Searchable getSearchable() {
      return searchable;
    }

    @SuppressWarnings("WeakerAccess")
    public void setSearchable(Searchable searchable) {
      this.searchable = searchable;
    }

    @SuppressWarnings("unused")
    public PropertiesStatistics withSearchable(Searchable searchable) {
      setSearchable(searchable);
      return this;
    }

    @SuppressWarnings("unused")
    public enum Searchable {
      ALL,
      PARTIAL,
      NONE
    }
  }

  public static class TagStatistics {

    protected String key;
    protected long count;
  }

  public static class PropertyStatistics extends TagStatistics {

    @JsonInclude(Include.NON_NULL)
    private String datatype;

    private boolean searchable = false;

    @SuppressWarnings("unused")
    public String getKey() {
      return key;
    }

    @SuppressWarnings("WeakerAccess")
    public void setKey(String key) {
      this.key = key;
    }

    @SuppressWarnings("unused")
    public PropertyStatistics withKey(String key) {
      setKey(key);
      return this;
    }

    @SuppressWarnings("unused")
    public String getDatatype() {
      return datatype;
    }

    @SuppressWarnings("WeakerAccess")
    public void setDatatype(String datatype) {
      this.datatype = datatype;
    }

    @SuppressWarnings("unused")
    public PropertyStatistics withDatatype(String datatype) {
      setDatatype(datatype);
      return this;
    }

    @SuppressWarnings("unused")
    public long getCount() {
      return count;
    }

    @SuppressWarnings("WeakerAccess")
    public void setCount(long count) {
      this.count = count;
    }

    @SuppressWarnings("unused")
    public PropertyStatistics withCount(long count) {
      setCount(count);
      return this;
    }

    @SuppressWarnings("unused")
    public boolean isSearchable() {
      return searchable;
    }

    @SuppressWarnings("WeakerAccess")
    public void setSearchable(boolean searchable) {
      this.searchable = searchable;
    }

    @SuppressWarnings("unused")
    public PropertyStatistics withSearchable(boolean searchable) {
      setSearchable(searchable);
      return this;
    }
  }
}
