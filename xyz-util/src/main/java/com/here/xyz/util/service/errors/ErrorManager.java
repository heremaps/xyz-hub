/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.util.service.errors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ErrorManager {

  private static final Logger logger = LogManager.getLogger();
  private static final Map<String, ErrorDefinition> errorMap = new HashMap<>();
  private static Map<String, String> globalPlaceholders = new HashMap<>();

  public static void loadErrors(String fileName) {
    logger.info("Loading error definitions from resource file: {}", fileName);
    try (InputStream inputStream = ErrorManager.class.getClassLoader().getResourceAsStream(fileName)) {
      if (inputStream == null) {
        String errMsg = fileName + " resource not found";
        logger.error(errMsg);
        throw new RuntimeException(errMsg);
      }
      ObjectMapper mapper = new ObjectMapper();
      List<ErrorDefinition> errors = mapper.readValue(inputStream, new TypeReference<List<ErrorDefinition>>() {
      });
      errors.forEach(error -> {
        if(errorMap.containsKey(error.getCode()))
          throw new IllegalStateException("Error definition for code " + error.getCode() + " is already registered");
        errorMap.put(error.getCode(), error);
        logger.debug("Loaded error definition: code={}, title={}", error.getCode(), error.getTitle());
      });
      logger.info("Successfully loaded {} error definitions from resource: {}", errors.size(), fileName);
    } catch (Exception e) {
      logger.error("Failed to load error definitions from file: {}", fileName, e);
      throw new RuntimeException("Failed to load error definitions", e);
    }
  }

  public static void registerGlobalPlaceholders(Map<String, String> placeholders) {
    globalPlaceholders.putAll(placeholders);
    logger.info("Added default placeholders: {}", placeholders);
  }

  static ErrorDefinition getErrorDefinition(String errorCode) {
    ErrorDefinition errorDefinition = errorMap.get(errorCode);
    if (errorDefinition == null)
      throw new IllegalArgumentException("Requested error code not found: " + errorCode);
    return errorDefinition;
  }

  static String format(String template, Map<String, String> placeholders) {
    String result = template;
    Map<String, String> effectivePlaceholders = new HashMap<>(globalPlaceholders);
    if (placeholders != null)
      effectivePlaceholders.putAll(placeholders);

    for (Map.Entry<String, String> entry : effectivePlaceholders.entrySet())
      result = result.replace("${" + entry.getKey() +"}", entry.getValue());

    return result;
  }
}