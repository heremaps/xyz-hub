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
import software.amazon.awssdk.utils.StringUtils;

/**
 * Holds the mapping between space ids regular expressions and region/connector ids.
 * The value can be represented as follows:
 * {
 *   "^.*topology$": {
 *     "eu-west-1": "topology-psql-db1-eu-west-1-sit",
 *     "ap-northeast-2": "topology-psql-db1-ap-northeast-2-sit"
 *   },
 *   "^.*address$": {
 *     "eu-west-1": "address-psql-db1-eu-west-1-sit",
 *     "ap-northeast-2": "dh-psql-db1-ap-northeast-2-sit"
 *   }
 * }
 */
public class SpaceStorageMatchingMap extends SingletonSetting<Map<String, Map<String, String>>> {

  private static final Logger logger = LogManager.getLogger();

  private static final Hashtable<String, PatternConnectorId> compiledPatternsMap = new Hashtable<>();


  public void updatePatterns() {

    Optional.of(data).orElse(Collections.emptyMap())
        .forEach((regex, regionConnectorMap) -> {
          regionConnectorMap.forEach((region, connectorId) -> {
            try {
              Pattern pattern = Pattern.compile(regex);
              compiledPatternsMap.put(regex, new PatternConnectorId(pattern, region, connectorId));
            } catch (PatternSyntaxException ex) {
              logger.warn("Unable to compile pattern: " + regex, ex);
            }
          });
        });

    Set<String> currentKeys = Optional.of(data).orElse(Collections.emptyMap()).keySet();
    compiledPatternsMap.keySet().removeIf(s -> !currentKeys.contains(s));
  }

  /**
   * Otherwise null
   * @param spaceId the space id to match with one of the existing patterns
   * @param region the connector region to be matched
   * @return connector id
   */
  public static String getIfMatches(String spaceId, String region) {
    return compiledPatternsMap
        .values()
        .stream()
        .filter(p -> p.pattern.matcher(spaceId).matches() && StringUtils.equals(region, p.region))
        .findFirst()
        .map(p -> p.connectorId)
        .orElse(null);
  }

  static class PatternConnectorId {

    Pattern pattern;
    String region;
    String connectorId;

    public PatternConnectorId(Pattern pattern, String region, String connectorId) {
      this.pattern = pattern;
      this.region = region;
      this.connectorId = connectorId;
    }
  }
}
