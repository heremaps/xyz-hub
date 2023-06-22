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

package com.here.naksha.lib.core.models.payload.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Query to a single property, the values are OR combined:
 *
 * <pre>{@code key {op} values[0] OR key {op} values[1] OR ...}</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "PropertyQuery")
public class PropertyQuery {

  public PropertyQuery() {}

  public PropertyQuery(@NotNull String key, @NotNull QueryOperation op) {}

  /** The property key as JSON path. */
  private String key;
  /** The operation. */
  private QueryOperation operation;
  /** The values to apply the operation for, OR condition (so any of the values should match). */
  private List<Object> values;

  @SuppressWarnings("unused")
  public String getKey() {
    return this.key;
  }

  @SuppressWarnings("WeakerAccess")
  public void setKey(String key) {
    this.key = key;
  }

  @SuppressWarnings("unused")
  public PropertyQuery withKey(String key) {
    setKey(key);
    return this;
  }

  @SuppressWarnings("unused")
  public @NotNull QueryOperation getOperation() {
    return this.operation;
  }

  @SuppressWarnings("WeakerAccess")
  public void setOperation(QueryOperation operation) {
    this.operation = operation;
  }

  @SuppressWarnings("unused")
  public @NotNull PropertyQuery withOperation(QueryOperation operation) {
    setOperation(operation);
    return this;
  }

  @SuppressWarnings("unused")
  public @NotNull List<@Nullable Object> getValues() {
    if (values == null) {
      values = new ArrayList<>();
    }
    return values;
  }

  public void setValues(@NotNull List<@Nullable Object> values) {
    this.values = values;
  }

  public @NotNull PropertyQuery withValues(@NotNull List<@Nullable Object> values) {
    this.values = values;
    return this;
  }
}
