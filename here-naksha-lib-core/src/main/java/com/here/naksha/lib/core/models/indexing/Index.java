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
package com.here.naksha.lib.core.models.indexing;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/** The specification of an index. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Index {

  /**
   * The algorithm to use. The implementing processor will decide if it supports the algorithm.
   *
   * <p>The PostgresQL processor supports the following algorithms (with its recommended targets):
   * <ul>
   * <li>{@code btree} for {@code String}, {@code Number} and {@code Boolean}.
   * <li>{@code hash} for {@code String}, {@code Number} and {@code Boolean}.
   * <li>{@code brin} for {@code String}, {@code Number} and {@code Boolean}.
   * <li>{@code gin} for {@code List} and {@code Map}.
   * <li>{@code gin_trigram} for {@code String}.
   * </ul>
   * <p>Note that if no algorithm given, the PostgresQL processor will auto-select on and return the selected algorithm in the response.
   */
  @JsonProperty
  public String alg;

  /** If the index should be applied to the history too. */
  @JsonProperty
  public boolean indexHistory = false;

  /** All properties that should be included in this index. */
  @JsonProperty
  public List<@NotNull IndexProperty> properties;
}
