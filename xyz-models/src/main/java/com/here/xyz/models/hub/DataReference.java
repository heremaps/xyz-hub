/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.models.hub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.xyz.XyzSerializable;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.UUID;

// TODO: This really begs for Lombok. And to be an immutable class, with a builder.
@JsonIgnoreProperties(ignoreUnknown = true)
public final class DataReference implements XyzSerializable {

  private UUID id;

  private String entityId;

  private boolean isPatch;

  private Integer startVersion;

  private Integer endVersion;

  private String objectType;

  private String contentType;

  private String location;

  private String sourceSystem;

  private String targetSystem;

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public DataReference withId(UUID id) {
    setId(id);
    return this;
  }

  public String getEntityId() {
    return entityId;
  }

  public void setEntityId(String entityId) {
    this.entityId = entityId;
  }

  public DataReference withEntityId(String entityId) {
    setEntityId(entityId);
    return this;
  }

  @JsonProperty("isPatch")
  public boolean isPatch() {
    return isPatch;
  }

  public void setPatch(boolean patch) {
    isPatch = patch;
  }

  public DataReference withPatch(boolean patch) {
    setPatch(patch);
    return this;
  }

  public Integer getStartVersion() {
    return startVersion;
  }

  public void setStartVersion(Integer startVersion) {
    this.startVersion = startVersion;
  }

  public DataReference withStartVersion(Integer startVersion) {
    setStartVersion(startVersion);
    return this;
  }

  public Integer getEndVersion() {
    return endVersion;
  }

  public void setEndVersion(Integer endVersion) {
    this.endVersion = endVersion;
  }

  public DataReference withEndVersion(Integer endVersion) {
    setEndVersion(endVersion);
    return this;
  }

  public String getObjectType() {
    return objectType;
  }

  public void setObjectType(String objectType) {
    this.objectType = objectType;
  }

  public DataReference withObjectType(String objectType) {
    setObjectType(objectType);
    return this;
  }

  public String getContentType() {
    return contentType;
  }

  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  public DataReference withContentType(String contentType) {
    setContentType(contentType);
    return this;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public DataReference withLocation(String location) {
    setLocation(location);
    return this;
  }

  public String getSourceSystem() {
    return sourceSystem;
  }

  public void setSourceSystem(String sourceSystem) {
    this.sourceSystem = sourceSystem;
  }

  public DataReference withSourceSystem(String sourceSystem) {
    setSourceSystem(sourceSystem);
    return this;
  }

  public String getTargetSystem() {
    return targetSystem;
  }

  public void setTargetSystem(String targetSystem) {
    this.targetSystem = targetSystem;
  }

  public DataReference withTargetSystem(String targetSystem) {
    setTargetSystem(targetSystem);
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DataReference reference = (DataReference) o;

    return new EqualsBuilder()
      .append(isPatch(), reference.isPatch())
      .append(getId(), reference.getId())
      .append(getEntityId(), reference.getEntityId())
      .append(getStartVersion(), reference.getStartVersion())
      .append(getEndVersion(), reference.getEndVersion())
      .append(getObjectType(), reference.getObjectType())
      .append(getContentType(), reference.getContentType())
      .append(getLocation(), reference.getLocation())
      .append(getSourceSystem(), reference.getSourceSystem())
      .append(getTargetSystem(), reference.getTargetSystem())
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
      .append(getId())
      .append(getEntityId())
      .append(isPatch())
      .append(getStartVersion())
      .append(getEndVersion())
      .append(getObjectType())
      .append(getContentType())
      .append(getLocation())
      .append(getSourceSystem())
      .append(getTargetSystem())
      .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
      .append("id", id)
      .append("entityId", entityId)
      .append("isPatch", isPatch)
      .append("startVersion", startVersion)
      .append("endVersion", endVersion)
      .append("objectType", objectType)
      .append("contentType", contentType)
      .append("location", location)
      .append("sourceSystem", sourceSystem)
      .append("targetSystem", targetSystem)
      .toString();
  }

}
