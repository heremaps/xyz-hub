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

package com.here.xyz.models.hub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.here.xyz.XyzSerializable;

public class Ref implements XyzSerializable {
  public static final String HEAD = "HEAD";
  public static final String ALL_VERSIONS = "*";
  private String tag;
  private long version = -1;
  private long startVersion = -1;
  private boolean head;
  private boolean allVersions;

  @JsonCreator
  public Ref(String ref) {
    if (ref == null || ref.isEmpty() || HEAD.equals(ref))
      head = true;
    else if (ALL_VERSIONS.equals(ref))
      allVersions = true;
    else if (ref.contains(".."))
      try {
        String[] rangeParts = ref.split("\\.\\.");
        startVersion = validateVersion(Long.parseLong(rangeParts[0]));
        version = validateVersion(Long.parseLong(rangeParts[1]));
        if (getStartVersion() >= getEndVersion())
          throw new InvalidRef("Invalid ref: The provided version-range is invalid. The start-version must be less than the end-version: "
              + "\"" + ref + "\"");
      }
      catch (NumberFormatException e) {
        throw new InvalidRef("Invalid ref: The provided version-range is invalid: \"" + ref + "\"");
      }
    else
      try {
        version = validateVersion(Long.parseLong(ref));
      }
      catch (NumberFormatException e) {
        if (!Tag.isValidId(ref))
          throw new InvalidRef("Invalid ref: the provided ref is not a valid ref or version: \"" + ref + "\"");

        tag = ref;
      }
  }

  public Ref(long version) {
    this.version = validateVersion(version);
  }

  /**
   * Validates a version number.
   * @param version The version to validate
   * @returns The validated version for further usage inside an expression
   */
  private long validateVersion(long version) {
    if (version < 0)
      throw new InvalidRef("Invalid ref: The provided version number may not be lower than 0");
    return version;
  }

  @JsonValue
  @Override
  public String toString() {
    if (!isTag() && version < 0 && !head && !allVersions && !isRange())
      throw new InvalidRef("Not a valid ref");
    if (isTag())
      return tag;
    if (head)
      return HEAD;
    if (allVersions)
      return ALL_VERSIONS;
    if (isRange())
      return startVersion + ".." + version;
    return String.valueOf(version);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof Ref otherRef && otherRef.toString().equals(toString());
  }

  /**
   * @return <code>true</code> if this ref depicts a tag, <code>false</code> otherwise
   */
  public boolean isTag() {
    return tag != null;
  }

  /**
   * If this ref is depicting a tag, this method returns the tag's ID.
   * @return The tag name if this ref depicts a tag, <code>null</code> otherwise
   */
  public String getTag() {
    return tag;
  }

  /**
   * The version being referenced by this ref object.
   * A valid version is an integer >= 0 where 0 is the very first version of an empty space just after having been created.
   */
  public long getVersion() {
    if (!isSingleVersion())
      throw new NumberFormatException("Ref is not depicting a single version.");
    if (isHead())
      throw new NumberFormatException("Version number of alias HEAD is not known for this ref.");
    return version;
  }

  public long getStartVersion() {
    if (!isRange())
      throw new NumberFormatException("Ref is not depicting a version range.");
    return startVersion;
  }

  public long getEndVersion() {
    if (!isRange())
      throw new NumberFormatException("Ref is not depicting a version range.");
    return version;
  }

  public boolean isHead() {
    return head;
  }

  public boolean isAllVersions() {
    return allVersions;
  }

  public boolean isSingleVersion() {
    return !isAllVersions() && !isRange();
  }

  public boolean isRange() {
    return startVersion >= 0;
  }

  public static class InvalidRef extends IllegalArgumentException {

    private InvalidRef(String message) {
      super(message);
    }
  }
}
