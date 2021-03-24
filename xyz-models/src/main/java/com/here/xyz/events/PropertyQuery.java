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

package com.here.xyz.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "PropertyQuery")
public class PropertyQuery {

  private String key;
  private QueryOperation operation;
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
  public QueryOperation getOperation() {
    return this.operation;
  }

  @SuppressWarnings("WeakerAccess")
  public void setOperation(QueryOperation operation) {
    this.operation = operation;
  }

  @SuppressWarnings("unused")
  public PropertyQuery withOperation(QueryOperation operation) {
    setOperation(operation);
    return this;
  }

  @SuppressWarnings("unused")
  public List<Object> getValues() {
    return this.values;
  }

  @SuppressWarnings("WeakerAccess")
  public void setValues(List<Object> values) {
    this.values = values;
  }

  @SuppressWarnings("unused")
  public PropertyQuery withValues(List<Object> values) {
    setValues(values);
    return this;
  }

  public enum QueryOperation {EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, LESS_THAN_OR_EQUALS, GREATER_THAN_OR_EQUALS, CONTAINS }
}
