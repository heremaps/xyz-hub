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

package com.here.xyz;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@JsonInclude(Include.NON_NULL)
public abstract class Extensible<SELF extends Extensible<SELF>> implements XyzSerializable {

  @JsonIgnore
  private @Nullable Map<@NotNull String, @Nullable Object> additionalProperties;

  @JsonAnyGetter
  protected final @Nullable Map<@NotNull String, @Nullable Object> jsonAnyGetter() {
    return additionalProperties;
  }

  @SuppressWarnings("unchecked")
  public <V> @Nullable V get(@NotNull Object key) {
    if (key instanceof String) {
      return (V) additionalProperties().get(key);
    }
    return null;
  }

  @JsonAnySetter
  public void put(@NotNull String key, @Nullable Object value) {
    additionalProperties().put(key, value);
  }

  protected final @NotNull Map<@NotNull String, @Nullable Object> additionalProperties() {
    if (this.additionalProperties == null) {
      this.additionalProperties = new HashMap<>();
    }
    return additionalProperties;
  }

  @SuppressWarnings("unchecked")
  public @NotNull SELF with(@NotNull String key, @Nullable Object value) {
    put(key, value);
    return (SELF) this;
  }

  @SuppressWarnings("unchecked")
  public @NotNull SELF remove(@NotNull String key) {
    additionalProperties().remove(key);
    return (SELF) this;
  }
}
