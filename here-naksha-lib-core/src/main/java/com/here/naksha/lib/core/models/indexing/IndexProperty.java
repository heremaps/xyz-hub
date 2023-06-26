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

@JsonIgnoreProperties(ignoreUnknown = true)
public class IndexProperty {

  /** The JSON path to the property to index. */
  @JsonProperty
  public String path;

  /** If the property should be naturally ordered ascending. */
  @JsonProperty
  public boolean asc = true;

  /**
   * Optionally decide if {@link null} values should be ordered first or last. If not explicitly
   * defined, automatically decided.
   */
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public Nulls nulls;

  public enum Nulls {
    FIRST,
    LAST
  }
}
