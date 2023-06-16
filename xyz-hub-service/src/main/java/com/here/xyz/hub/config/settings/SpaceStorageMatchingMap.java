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

package com.here.xyz.hub.config.settings;

import java.util.Collections;
import java.util.Hashtable;
import java.util.Optional;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class SpaceStorageMatchingMap extends SingletonSetting<Map<String, String>> {

  private static final Logger logger = LogManager.getLogger();

  private static final Hashtable<String, PatternConnectorId> compiledPatternsMap = new Hashtable<>();


  public void updatePatterns() {

    Optional.of(data).orElse(Collections.emptyMap())
        .forEach((key, value) -> {
          try {
            Pattern pattern = Pattern.compile(key);
            compiledPatternsMap.put(key, new PatternConnectorId(pattern, value));
          } catch (PatternSyntaxException ex) {
            logger.warn("Unable to compile pattern: " + key, ex);
          }
        });

    Set<String> currentKeys = Optional.of(data).orElse(Collections.emptyMap()).keySet();
    compiledPatternsMap.keySet().removeIf(s -> !currentKeys.contains(s));
  }

  /**
   * Otherwise null
   * @param spaceId the space id to match with one of the existing patterns
   * @return connector id
   */
  public static String getIfMatches(String spaceId) {
    return compiledPatternsMap
        .values()
        .stream()
        .filter(p -> p.pattern.matcher(spaceId).matches())
        .findFirst()
        .map(p -> p.connectorId)
        .orElse(null);
  }

  static class PatternConnectorId {

    Pattern pattern;
    String connectorId;

    public PatternConnectorId(Pattern pattern, String connectorId) {
      this.pattern = pattern;
      this.connectorId = connectorId;
    }
  }
}
