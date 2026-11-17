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

package com.here.xyz.hub.task;

import com.here.xyz.hub.Service;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The mode of the Iceberg read routing, see {@link com.here.xyz.hub.Config#ICEBERG_READS}.
 */
public enum IcebergReadMode {
  OFF,
  SHADOW,
  ON;

  private static final Logger logger = LogManager.getLogger();
  private static final Set<String> warnedValues = ConcurrentHashMap.newKeySet();
  private static volatile Allowlist allowlist = new Allowlist(null, Set.of());

  /**
   * The configured mode. A missing or unknown value is {@link #OFF}, an unknown value is logged once.
   */
  public static IcebergReadMode current() {
    String value = Service.configuration == null ? null : Service.configuration.ICEBERG_READS;
    if (value == null || value.isBlank())
      return OFF;
    try {
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    }
    catch (IllegalArgumentException e) {
      if (warnedValues.add(value))
        logger.warn("Unknown value \"{}\" of ICEBERG_READS, Iceberg reads are switched off", value);
      return OFF;
    }
  }

  /**
   * The configured space IDs for which the routing applies, or an empty set if it applies to all spaces.
   */
  public static Set<String> allowlist() {
    String value = Service.configuration == null ? null : Service.configuration.ICEBERG_READS_SPACES;
    Allowlist current = allowlist;
    if (!Objects.equals(current.raw, value)) {
      current = new Allowlist(value, value == null ? Set.of() : Arrays.stream(value.split(","))
          .map(String::trim)
          .filter(id -> !id.isEmpty())
          .collect(Collectors.toUnmodifiableSet()));
      allowlist = current;
    }
    return current.spaceIds;
  }

  private record Allowlist(String raw, Set<String> spaceIds) {}
}
