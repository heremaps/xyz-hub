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
package com.here.naksha.app.service.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.jetbrains.annotations.NotNull;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class IterateHandle implements JsonSerializable {
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private long offset;

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private long limit;

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public IterateHandle withOffset(long offset) {
    setOffset(offset);
    return this;
  }

  public long getLimit() {
    return limit;
  }

  public void setLimit(long limit) {
    this.limit = limit;
  }

  public IterateHandle withLimit(long limit) {
    setLimit(limit);
    return this;
  }

  public String base64EncodedSerializedJson() {
    return Base64.getEncoder().encodeToString(this.serialize().getBytes(StandardCharsets.UTF_8));
  }

  public static IterateHandle base64DecodedDeserializedJson(final @NotNull String handle) {
    return JsonSerializable.deserialize(Base64.getDecoder().decode(handle), IterateHandle.class);
  }
}
