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

package com.here.xyz.jobs;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.XyzSerializable.Public;
import com.here.xyz.XyzSerializable.Static;
import java.util.Map;

public class JobClientInfo {

  @JsonView({Public.class, Static.class})
  private Map<String, Object> clientProvidedData;

  @JsonAnyGetter
  public Map<String, Object> getClientProvidedData() {
    return clientProvidedData;
  }

  public void setClientProvidedData(Map<String, Object> clientProvidedData) {
    this.clientProvidedData = clientProvidedData;
  }

  public JobClientInfo withClientProvidedData(Map<String, Object> clientProvidedData) {
    setClientProvidedData(clientProvidedData);
    return this;
  }

  @JsonAnySetter
  public JobClientInfo putClientProvidedData(String key, Object value) {
    getClientProvidedData().put(key, value);
    return this;
  }
}
