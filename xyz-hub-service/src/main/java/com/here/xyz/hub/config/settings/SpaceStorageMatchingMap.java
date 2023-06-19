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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

  private static final List<PatternConnectorId> compiledPatterns = Collections.synchronizedList(new ArrayList<>());


  public void updatePatterns() {
    int initialSize = compiledPatterns.size();

    Optional.of(data).orElse(Collections.emptyMap())
        .forEach((regex, regionConnectorMap) -> {
          regionConnectorMap.forEach((region, connectorId) -> {
            try {
              Pattern pattern = Pattern.compile(regex);
              compiledPatterns.add(new PatternConnectorId(regex, pattern, region, connectorId));
            } catch (PatternSyntaxException ex) {
              logger.warn("Unable to compile pattern: " + regex, ex);
            }
          });
        });

    while (initialSize-- > 0) compiledPatterns.remove(0);
  }

  /**
   * Otherwise null
   * @param spaceId the space id to match with one of the existing patterns
   * @param region the connector region to be matched
   * @return connector id
   */
  public static String getIfMatches(String spaceId, String region) {
    return compiledPatterns
        .stream()
        .filter(p -> p.compiledPattern.matcher(spaceId).matches() && StringUtils.equals(region, p.region))
        .findFirst()
        .map(p -> p.connectorId)
        .orElse(null);
  }

  static class PatternConnectorId {

    String pattern;
    Pattern compiledPattern;
    String region;
    String connectorId;

    public PatternConnectorId(String pattern, Pattern compiledPattern, String region, String connectorId) {
      this.pattern = pattern;
      this.compiledPattern = compiledPattern;
      this.region = region;
      this.connectorId = connectorId;
    }
  }
}
