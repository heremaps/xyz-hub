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
package com.here.naksha.lib.core.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

/** The copyright information object. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Copyright {

  /** The copyright label to be displayed by the client. */
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  private String label;

  /** The description text for the label to be displayed by the client. */
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  private String alt;

  public String getLabel() {
    return label;
  }

  public void setLabel(final String label) {
    this.label = label;
  }

  public Copyright withLabel(final String label) {
    setLabel(label);
    return this;
  }

  public String getAlt() {
    return alt;
  }

  public void setAlt(final String alt) {
    this.alt = alt;
  }

  public Copyright withAlt(final String alt) {
    setAlt(alt);
    return this;
  }
}
