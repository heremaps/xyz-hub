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
import java.util.regex.Pattern;

public class Ref implements XyzSerializable {
  public static final String HEAD = "HEAD";
  public static final String ALL_VERSIONS = "*";
  public static final String OP_RANGE = "..";

  private String tag;
  private long version = -1;
  private boolean head;
  private boolean allVersions;
  private Ref start; //Only set if this ref is a range
  private Ref end; //Only set if this ref is a range

  @JsonCreator
  public Ref(String ref) {
    if (ref == null || ref.isEmpty() || HEAD.equals(ref))
      head = true;
    else if (ALL_VERSIONS.equals(ref))
      allVersions = true;
    else if (ref.contains(OP_RANGE))
      parseRange(ref);
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

  public Ref(long startVersion, long endVersion) {
    start = new Ref(startVersion);
    end = new Ref(endVersion);
    validateRange();
  }

  /**
   * Validates a version number.
   * @param version The version to validate
   * @return The validated version for further usage inside an expression
   */
  private long validateVersion(long version) {
    if (version < 0)
      throw new InvalidRef("Invalid ref: The provided version number may not be lower than 0");
    return version;
  }

  private void parseRange(String ref) {
    try {
      String[] rangeParts = ref.split(Pattern.quote(OP_RANGE));
      if (rangeParts.length > 2)
        throw new InvalidRef("Invalid ref: A range can only have one start and one end.");

      parseAndSetRangePart(rangeParts[0], true);
      parseAndSetRangePart(rangeParts[1], false);
    }
    catch (NumberFormatException | InvalidRef e) {
      throw new InvalidRef("Invalid ref: The provided version-range is invalid: \"" + ref + "\" Reason: " + e.getMessage());
    }
    validateRange();
  }

  private void validateRange() {
    if (isOnlyNumeric() && getStart().getVersion() >= getEnd().getVersion())
      throw new InvalidRef("Invalid ref: The provided version-range is invalid. The start-version must be less than the end-version: "
          + "\"" + this + "\"");
  }

  private void parseAndSetRangePart(String rangePart, boolean isStart) {
    try {
      Ref subRef = new Ref(rangePart);
      if (subRef.isAllVersions())
        throw new InvalidRef("Invalid ref: A range may not contain \"*\" (all versions).");
      if (isStart) {
        if (subRef.isHead())
          throw new InvalidRef("The start of a range may not be HEAD!");
        start = subRef;
      }
      else
        end = subRef;
    }
    catch (Exception e) {
      throw new InvalidRef("Invalid " + (isStart ? "start" : "end") + " of range: " + e.getMessage());
    }
  }

  @JsonValue
  @Override
  public String toString() {
    if (!isTag() && version < 0 && !head && !allVersions && !isRange())
      throw new InvalidRef("Not a valid ref"); //Should never happen, only if this class has a bug

    if (isTag())
      return tag;
    if (isHead())
      return HEAD;
    if (isAllVersions())
      return ALL_VERSIONS;
    if (isRange())
      return getStart() + OP_RANGE + getEnd();
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
    if (!isTag())
      throw new IllegalStateException("Ref is not depicting a tag but " + (isRange() ? "a range" : isHead() ? "is HEAD" : "a version") + ".");
    return tag;
  }

  /**
   * The version being referenced by this ref object.
   * A valid version is an integer >= 0 where 0 is the very first version of an empty space just after having been created.
   */
  public long getVersion() {
    if (isTag())
      throw new NumberFormatException("Ref is not depicting a version but a tag.");
    if (!isSingleVersion())
      throw new NumberFormatException("Ref is not depicting a single version.");
    if (isHead())
      throw new NumberFormatException("Version number of alias HEAD is not known for this ref.");
    return version;
  }

  public Ref getStart() {
    assertIsRange();
    return start;
  }

  public Ref getEnd() {
    assertIsRange();
    return end;
  }

  private void assertIsRange() {
    if (!isRange())
      throw new NumberFormatException("Ref is not depicting a version range.");
  }

  public boolean isOnlyNumeric() {
    return isRange() ? getStart().isOnlyNumeric() && getEnd().isOnlyNumeric() : !isTag() && !isHead() && !isAllVersions();
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
    return start != null;
  }

  public static class InvalidRef extends IllegalArgumentException {

    private InvalidRef(String message) {
      super(message);
    }
  }
}
