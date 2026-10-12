/*
 * Copyright (C) 2017-2016 HERE Europe B.V.
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
import java.util.regex.Pattern;
import java.util.List;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "PropertyQuery")
public class PropertyQuery {

  private static final Pattern DISALLOWED_KEY_CHARACTERS = Pattern.compile("[^a-zA-Z0-9@:.,_!=\\-<>]");
  private static final Pattern DISALLOWED_VALUE_CHARACTERS = Pattern.compile("[^a-zA-Z0-9@:.,_!=\\-<> {}\"]");

  private String key;
  private QueryOperation operation;
  private List<Object> values;

  @SuppressWarnings("unused")
  public String getKey() {
    return this.key;
  }

  @SuppressWarnings("WeakerAccess")
  public void setKey(String key) {
    this.key = sanitizeKey(key);
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
    this.values = values == null ? null : values.stream()
            .map(PropertyQuery::sanitizeValue)
            .collect(Collectors.toList());
  }

  @SuppressWarnings("unused")
  public PropertyQuery withValues(List<Object> values) {
    setValues(values);
    return this;
  }

  private static Object sanitizeValue(Object value) {
    if (value instanceof String)
      return sanitizeValue((String) value);
    if (value instanceof List<?>)
      return ((List<?>) value).stream()
              .map(PropertyQuery::sanitizeValue)
              .collect(Collectors.toList());
    return value;
  }

  private static String sanitizeKey(String key) {
    return key == null ? null : DISALLOWED_KEY_CHARACTERS.matcher(key).replaceAll("");
  }

  private static String sanitizeValue(String value) {
    return value == null ? null : DISALLOWED_VALUE_CHARACTERS.matcher(value).replaceAll("");
  }

  public enum QueryOperation {EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, LESS_THAN_OR_EQUALS, GREATER_THAN_OR_EQUALS, CONTAINS }
}
