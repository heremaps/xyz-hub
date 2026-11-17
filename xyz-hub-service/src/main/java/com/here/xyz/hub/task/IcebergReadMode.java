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
import com.here.xyz.models.hub.Space;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The mode of the Iceberg read routing of a space. The space's property {@code icebergReads} (OFF, ON or SHADOW) takes
 * precedence; if it is not set, the Hub setting {@link com.here.xyz.hub.Config#ICEBERG_READS} (OFF or ON) applies. A composite
 * space has its own mode, the mode of its base does not matter.
 */
public enum IcebergReadMode {
  OFF,
  SHADOW,
  ON;

  /**
   * The name of the space property
   */
  public static final String SPACE_PROPERTY = "icebergReads";

  private static final Logger logger = LogManager.getLogger();
  private static final Set<String> warnedValues = ConcurrentHashMap.newKeySet();

  /**
   * The effective mode of a space: the space's own value if it is set, else the Hub setting.
   *
   * @param space The space, or null for the Hub setting
   */
  public static IcebergReadMode of(Space space) {
    IcebergReadMode spaceMode = space == null ? null : ofSpaceValue(space.getIcebergReads());
    return spaceMode != null ? spaceMode : global();
  }

  /**
   * The Hub setting {@link com.here.xyz.hub.Config#ICEBERG_READS}: {@link #ON} or {@link #OFF}. A missing value is {@link #OFF}.
   * Any other value, including {@link #SHADOW}, which is only a space value, is {@link #OFF} and logged once.
   */
  public static IcebergReadMode global() {
    String value = Service.configuration == null ? null : Service.configuration.ICEBERG_READS;
    if (value == null || value.isBlank())
      return OFF;
    IcebergReadMode mode = parse(value);
    if (mode == ON || mode == OFF)
      return mode;
    if (warnedValues.add("global:" + value))
      logger.warn("Unsupported value \"{}\" of ICEBERG_READS (only OFF or ON, SHADOW can only be set per space in the space property "
          + "{}), Iceberg reads are switched off for the spaces without that property", value, SPACE_PROPERTY);
    return OFF;
  }

  /**
   * @return The mode of a value of the space property, null if it is not set. An unknown value (the space API rejects it, see
   *     {@link #isValidSpaceValue(String)}) is {@link #OFF} and logged once.
   */
  static IcebergReadMode ofSpaceValue(String value) {
    if (value == null)
      return null;
    IcebergReadMode mode = parse(value);
    if (mode != null)
      return mode;
    if (warnedValues.add("space:" + value))
      logger.warn("Unknown value \"{}\" of the space property {}, Iceberg reads are switched off for the spaces with it", value,
          SPACE_PROPERTY);
    return OFF;
  }

  /**
   * @return Whether a value may be stored in the space property: null (not set) or exactly one of the names of the modes
   */
  public static boolean isValidSpaceValue(String value) {
    if (value == null)
      return true;
    for (IcebergReadMode mode : values())
      if (mode.name().equals(value))
        return true;
    return false;
  }

  private static IcebergReadMode parse(String value) {
    try {
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    }
    catch (IllegalArgumentException e) {
      return null;
    }
  }
}
