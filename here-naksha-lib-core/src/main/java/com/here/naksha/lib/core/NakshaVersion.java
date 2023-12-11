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
package com.here.naksha.lib.core;

import static com.here.naksha.lib.core.NakshaVersion.v2_0_3;

import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Just an abstraction for Naksha versioning.
 *
 */
@SuppressWarnings("unused")
@AvailableSince(v2_0_3)
public class NakshaVersion implements Comparable<NakshaVersion> {
  /**
   * Naksha version constant. The last version compatible with XYZ-Hub.
   */
  public static final String v0_6 = "0.6.0";

  public static final String v2_0_0 = "2.0.0";
  public static final String v2_0_3 = "2.0.3";
  public static final String v2_0_4 = "2.0.4";
  public static final String v2_0_5 = "2.0.5";
  public static final String v2_0_6 = "2.0.6";
  public static final String v2_0_7 = "2.0.7";
  public static final String v2_0_8 = "2.0.8";
  public static final String v2_0_9 = "2.0.9";

  /**
   * The latest version of the naksha-extension stored in the resources.
   */
  @AvailableSince(v2_0_5)
  public static final NakshaVersion latest = of(v2_0_9);

  private final int major;
  private final int minor;
  private final int revision;

  /**
   * @param major    the major version (0-65535).
   * @param minor    the minor version (0-65535).
   * @param revision the revision (0-65535).
   */
  public NakshaVersion(int major, int minor, int revision) {
    this.major = major;
    this.minor = minor;
    this.revision = revision;
  }

  /**
   * Parses the given version string and returns the Naksha version.
   *
   * @param version the version like "2.0.3".
   * @return the Naksha version.
   * @throws NumberFormatException if the given string is no valid version.
   */
  @AvailableSince(v2_0_3)
  public static @NotNull NakshaVersion of(@NotNull String version) throws NumberFormatException {
    final int majorEnd = version.indexOf('.');
    final int minorEnd = version.indexOf('.', majorEnd + 1);
    return new NakshaVersion(
        Integer.parseInt(version.substring(0, majorEnd)),
        Integer.parseInt(version.substring(majorEnd + 1, minorEnd)),
        Integer.parseInt(version.substring(minorEnd + 1)));
  }

  @AvailableSince(v2_0_3)
  public NakshaVersion(long value) {
    this((int) ((value >>> 32) & 0xffff), (int) ((value >>> 16) & 0xffff), (int) (value & 0xffff));
  }

  @AvailableSince(v2_0_3)
  public int getMajor() {
    return major;
  }

  @AvailableSince(v2_0_3)
  public int getMinor() {
    return minor;
  }

  @AvailableSince(v2_0_3)
  public int getRevision() {
    return revision;
  }

  @AvailableSince(v2_0_3)
  public long toLong() {
    return ((major & 0xffffL) << 32) | ((minor & 0xffffL) << 16) | (revision & 0xffffL);
  }

  @Override
  public int compareTo(@NotNull NakshaVersion o) {
    final long result = toLong() - o.toLong();
    return result < 0 ? -1 : result == 0 ? 0 : 1;
  }

  @Override
  public int hashCode() {
    return (int) (toLong() >>> 16);
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (other instanceof NakshaVersion) {
      NakshaVersion o = (NakshaVersion) other;
      return this.toLong() == o.toLong();
    }
    return false;
  }

  @Override
  public @NotNull String toString() {
    return "" + major + '.' + minor + '.' + revision;
  }
}
