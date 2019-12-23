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
package com.here.xyz.hub.util.health.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.here.xyz.hub.util.health.Config;
import java.util.Map;

public interface ResponseSerializer {

  static ObjectMapper internalMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
  static ObjectWriter publicWriter = internalMapper.copy().disable(MapperFeature.DEFAULT_VIEW_INCLUSION).writerWithView(Public.class);

  public default boolean isPublicRequest(String secretHeaderValue) {
    return Config.getHealthCheckHeaderValue().equals(secretHeaderValue);
  }

  public default String toPublicResponseString() {
    try {
      return publicWriter.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      return "ERROR: Failed serializing response!";
    }
  }

  public default String toInternalResponseString() {
    try {
      return internalMapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      return "ERROR: Failed serializing internal response!";
    }
  }

  public default String toResponseString(String secretHeaderValue) {
    return isPublicRequest(secretHeaderValue) ?
        toInternalResponseString() : toPublicResponseString();
  }

  public default String toResponseString(Map<String, String> requestHeaders) {
    return toResponseString(requestHeaders != null ?
        requestHeaders.get(Config.getHealthCheckHeaderName()) : null);
  }

}
