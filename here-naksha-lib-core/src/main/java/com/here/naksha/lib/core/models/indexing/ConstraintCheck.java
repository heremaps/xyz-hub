/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.naksha.lib.core.models.indexing;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/** A condition to check. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "Check")
public class ConstraintCheck extends Constraint {

  /** The condition to apply. */
  public enum Test {
    /** If the property is not null; ignores {@link #value}. */
    NOT_NULL,

    /** If the value of the property is unique; ignores {@link #value}. */
    UNIQUE,

    /** If the value of the property is greater than the {@link #value}. */
    GT,

    /** If the value of the property is greater than or equal to the {@link #value}. */
    GTE,

    /** If the value of the property is equal to the {@link #value}. */
    EQ,

    /** If the value of the property is less than the {@link #value}. */
    LT,

    /** If the value of the property is less than or equal to the {@link #value}. */
    LTE,

    /** If the length of the property is more than or equal to the defined {@link #value}. */
    MIN_LEN,

    /** If the length of the property is less than or equal to the defined {@link #value}. */
    MAX_LEN,

    /** If the value matches the given regular expression, given in the {@link #value}. */
    MATCHES
  }

  /** The check to perform. */
  @JsonProperty
  public Test test;

  /** The JSON path to the property to check. */
  @JsonProperty
  public String path;

  /** The optional value for the check. */
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public Object value;
}
