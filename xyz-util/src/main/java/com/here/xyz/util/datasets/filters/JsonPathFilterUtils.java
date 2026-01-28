/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */

package com.here.xyz.util.datasets.filters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.here.xyz.models.geojson.implementation.Feature;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.util.Collection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JsonPathFilterUtils {

  private static final Logger logger = LogManager.getLogger();
  private static final Configuration JACKSON_CONFIG = Configuration.builder()
      .jsonProvider(new JacksonJsonNodeJsonProvider())
      .mappingProvider(new JacksonMappingProvider())
      .build();
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JsonPathFilterUtils() {
  }

  public static boolean filterByJsonPaths(Feature feature, Collection<String> jsonPaths) {
    if (feature == null || jsonPaths == null || jsonPaths.isEmpty()) {
      return false;
    }
    JsonNode featureNode = MAPPER.valueToTree(feature);
    return jsonPaths.stream().allMatch(jsonPath -> {
      try {
        Object object = JsonPath.compile(jsonPath).read(featureNode, JACKSON_CONFIG);
        if (object == null ||
            (object instanceof ArrayNode && ((ArrayNode) object).isEmpty())) {
          return false;
        }
      } catch (PathNotFoundException pathNotFoundException) {
        logger.debug("Json path not found: {}", jsonPath);
        return false;
      }
      return true;
    });
  }
}
