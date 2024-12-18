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

package com.here.xyz.jobs.steps.payloads;


import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.xyz.Typed;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.outputs.Output;
import java.util.HashMap;
import java.util.Map;

@JsonSubTypes({
    @JsonSubTypes.Type(value = Input.class, name = "Input"),
    @JsonSubTypes.Type(value = Output.class, name = "Output")
})
public abstract class StepPayload<T extends StepPayload> implements Typed {
  @JsonAnySetter
  private Map<String, String> metadata;

  @JsonAnyGetter
  public Map<String, String> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public T withMetadata(Map<String, String> metadata) {
    setMetadata(metadata);
    return (T) this;
  }

  public T withMetadata(String key, String value) {
    if (getMetadata() == null)
      setMetadata(new HashMap<>());
    getMetadata().put(key, value);
    return (T) this;
  }

  @JsonIgnore
  protected boolean hasMetadata() {
    return false;
  }
}
