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

package com.here.xyz.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "IterateChangesetsEvent")
public final class IterateChangesetsEvent extends SearchForFeaturesEvent<IterateChangesetsEvent> {

  private String reader;
  private Operation operation;
  private String pageToken;
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private Long startVersion;
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private Long endVersion;
  private Long numberOfVersions;
  private boolean compact;

  public Long getStartVersion() {
    return startVersion;
  }

  public void setStartVersion(Long startVersion) {
    this.startVersion = startVersion;
  }

  public IterateChangesetsEvent withStartVersion(Long startVersion) {
    setStartVersion(startVersion);
    return this;
  }

  public Long getEndVersion() {
    return endVersion;
  }

  public void setEndVersion(Long endVersion) {
    this.endVersion = endVersion;
  }

  public IterateChangesetsEvent withEndVersion(Long endVersion) {
    setEndVersion(endVersion);
    return this;
  }

  @SuppressWarnings("unused")
  public String getPageToken() {
    return pageToken;
  }

  @SuppressWarnings("WeakerAccess")
  public void setPageToken(String pageToken) {
    this.pageToken = pageToken;
  }

  @SuppressWarnings("unused")
  public IterateChangesetsEvent withPageToken(String pageToken) {
    setPageToken(pageToken);
    return this;
  }

  public boolean isCompact() {
    return compact;
  }

  public void setCompact(boolean compact) {
    this.compact = compact;
  }

  public IterateChangesetsEvent withCompact(boolean compact) {
    setCompact(compact);
    return this;
  }

  public String getReader() {
    return reader;
  }

  public void setReader(String reader) {
    this.reader = reader;
  }

  public IterateChangesetsEvent withReader(String reader) {
    setReader(reader);
    return this;
  }

  public Operation getOperation() {
    return operation;
  }

  public void setOperation(Operation operation) {
    this.operation = operation;
  }

  public IterateChangesetsEvent withOperation(Operation operation) {
    setOperation(operation);
    return this;
  }

  public Long getNumberOfVersions() {
    return numberOfVersions;
  }

  public void setNumberOfVersions(Long numberOfVersions) {
    this.numberOfVersions = numberOfVersions;
  }

  public IterateChangesetsEvent withNumberOfVersions(Long numberOfVersions) {
    setNumberOfVersions(numberOfVersions);
    return this;
  }

  public enum Operation {
    READ, PEEK, ADVANCE;

    public static Operation safeValueOf(String operation) {
      return safeValueOf(operation, null);
    }

    public static Operation safeValueOf(String operation, Operation defaultValue) {
      try {
        return Operation.valueOf(operation.toUpperCase());
      } catch (Exception e) {
        return defaultValue;
      }
    }
  }
}
