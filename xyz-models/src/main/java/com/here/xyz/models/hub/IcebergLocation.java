/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.models.hub;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;

/**
 * The location of an exported version in an Iceberg table, as stored in the {@code location} of an ICEBERG {@link DataReference}.
 * Format: {@code iceberg://<namespace>/<table>?ref=<tag>}
 *
 * @param namespace The Iceberg namespace (levels separated by dots)
 * @param table The table name
 * @param ref The tag (or branch) name in the table that holds the exported version
 */
public record IcebergLocation(String namespace, String table, String ref) {

  public static final String SCHEME = "iceberg";
  /**
   * The key in {@code event.params} under which the Hub passes the read location to the Iceberg connector.
   */
  public static final String EVENT_PARAM = "iceberg";
  /**
   * The key of the location inside the {@link #EVENT_PARAM} map.
   */
  public static final String EVENT_PARAM_LOCATION = "location";
  /**
   * The prefix of iterate page tokens created by the Iceberg connector.
   */
  public static final String ITERATE_TOKEN_PREFIX = "ice:";
  private static final String REF_PARAM = "ref";

  public IcebergLocation {
    requireNonEmpty(namespace, "namespace");
    requireNonEmpty(table, "table");
    requireNonEmpty(ref, "ref");
  }

  /**
   * Parses a location of the form {@code iceberg://<namespace>/<table>?ref=<tag>}.
   *
   * @throws IllegalArgumentException if the location is malformed, has another scheme or an empty part
   */
  public static IcebergLocation parse(String location) {
    if (location == null)
      throw new IllegalArgumentException("Iceberg location is missing");
    URI uri;
    try {
      uri = new URI(location);
    }
    catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid Iceberg location: " + location, e);
    }
    if (!SCHEME.equalsIgnoreCase(uri.getScheme()))
      throw new IllegalArgumentException("Iceberg location must start with " + SCHEME + "://, got: " + location);

    String namespace = decode(uri.getRawAuthority());
    String rawPath = uri.getRawPath();
    String table = rawPath == null || rawPath.isEmpty() ? null : decode(rawPath.substring(1));
    if (table != null && table.contains("/"))
      throw new IllegalArgumentException("Iceberg location must have the form " + SCHEME + "://<namespace>/<table>?ref=<tag>, got: "
          + location);

    String ref = null;
    String rawQuery = uri.getRawQuery();
    if (rawQuery != null)
      for (String param : rawQuery.split("&")) {
        int eq = param.indexOf('=');
        if (eq > 0 && REF_PARAM.equals(decode(param.substring(0, eq))))
          ref = decode(param.substring(eq + 1));
      }

    try {
      return new IcebergLocation(namespace, table, ref);
    }
    catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage() + " in Iceberg location: " + location);
    }
  }

  @Override
  public String toString() {
    return SCHEME + "://" + encode(namespace) + "/" + encode(table) + "?" + REF_PARAM + "=" + encode(ref);
  }

  private static void requireNonEmpty(String value, String name) {
    if (value == null || value.isEmpty())
      throw new IllegalArgumentException("The " + name + " is missing");
  }

  private static String decode(String value) {
    return value == null ? null : URLDecoder.decode(value, UTF_8);
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, UTF_8);
  }
}
