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
package com.here.naksha.app.service.util;

import static java.util.Arrays.stream;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class UrlUtil {

  private UrlUtil() {}

  /**
   * Checks whether given url contains exactly one value for given query key
   * @param url Url containing query to verify
   * @param key Key of verified query param
   * @return true if url contains query with specified key
   */
  public static boolean containsSingleQueryValue(String url, String key) {
    List<String> values = queryParams(url).get(key);
    return values != null && values.size() == 1;
  }

  /**
   * Extracts query params from supplied URL and returns them as map
   *
   * @param url URL to extract query params from
   * @return Map of key-values query params, empty map if no query params defined in original url
   */
  public static Map<String, List<String>> queryParams(String url) {
    String[] splitted = url.split("\\?");
    if (splitted.length < 2) {
      return emptyMap();
    }
    String[] queryParts = splitted[1].split("&");
    return stream(queryParts)
        .map(queryPart -> queryPart.split("="))
        .collect(groupingBy(keyAndValue -> keyAndValue[0], mapping(keyAndValue -> keyAndValue[1], toList())));
  }

  /**
   * Returns new url based on supplied one but with new set of query params (old ones are ignored)
   *
   * @param originalUrl Original url to be used (before query part)
   * @param newParams   Map of query parameters to become part of new url (query part)
   * @return Url with overridden query params
   */
  public static String urlWithOverriddenParams(String originalUrl, Map<String, List<String>> newParams) {
    String newQueryPart = newParams.entrySet().stream()
        .map(keyAndValues -> queryParamsString(keyAndValues.getKey(), keyAndValues.getValue()))
        .collect(Collectors.joining("&"));
    return originalUrl.split("\\?")[0] + "?" + newQueryPart;
  }

  private static String queryParamsString(String key, List<String> values) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < values.size() - 1; i++) {
      sb.append(key).append("=").append(values.get(i)).append("&");
    }
    sb.append(key).append("=").append(values.get(values.size() - 1));
    return sb.toString();
  }
}
