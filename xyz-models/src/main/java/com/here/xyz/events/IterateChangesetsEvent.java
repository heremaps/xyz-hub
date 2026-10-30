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

package com.here.xyz.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.FeatureChange.Operation;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "IterateChangesetsEvent")
public final class IterateChangesetsEvent extends IterateFeaturesEvent<IterateChangesetsEvent> {
  private List<String> authors;
  private long startTime;
  private long endTime;
  private Operation operation;
  private boolean squashed;

  public List<String> getAuthors() {
    return authors;
  }

  public void setAuthors(List<String> authors) {
    this.authors = authors;
  }

  public IterateChangesetsEvent withAuthors(List<String> authors) {
    setAuthors(authors);
    return this;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public IterateChangesetsEvent withStartTime(long startTime) {
    setStartTime(startTime);
    return this;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public IterateChangesetsEvent withEndTime(long endTime) {
    setEndTime(endTime);
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

  public boolean isSquashed() {
    return squashed;
  }

  public void setSquashed(boolean squashed) {
    this.squashed = squashed;
  }

  public IterateChangesetsEvent withSquashed(boolean squashed) {
    setSquashed(squashed);
    return this;
  }
}
