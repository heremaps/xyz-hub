/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

import static com.google.common.base.Strings.isNullOrEmpty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.here.xyz.XyzSerializable;
import java.util.regex.Pattern;

public class Ref implements XyzSerializable {

  //TODO: Rename "tag" property to "alias" to explicitly indicate that it could be a branch ID *or* a tag ID?

  public static final String MAIN = "main";
  public static final String HEAD = "HEAD";
  public static final String ALL_VERSIONS = "*";
  public static final String OP_RANGE = "..";

  private String branch = MAIN;
  private String tag;
  private long version = -1;
  private boolean head;
  private boolean allVersions;
  private Ref start; //Only set if this ref is a range
  private Ref end; //Only set if this ref is a range

  @JsonCreator
  public Ref(String ref) {
    String versionPart = HEAD;
    if (!isNullOrEmpty(ref)) {
      String[] refParts = ref.split(":");
      if (refParts.length < 1)
        throw new InvalidRef("Invalid ref: The provided ref is not valid: \"" + ref + "\"");
      if (refParts.length > 2)
        throw new InvalidRef("Invalid ref: The provided ref (" + ref + ") has a wrong format. Only one colon \":\" is allowed.");

      if (refParts.length == 1) {
        //Check if the provided ref depicts a branch or a version
        if (isValidVersionOrRange(refParts[0]))
          versionPart = refParts[0];
        else if (Tag.isValidId(refParts[0])) {
          tag = refParts[0];
          return;
        }
      }
      else {
        //Branch & version was provided, validate the parts
        String branchPart;
        branchPart = refParts[0];
        versionPart = refParts[1];
        if (!isValidBranchId(branchPart))
          throw new InvalidRef("Invalid ref: The provided branch ID is not valid: " + branchPart);
        if (!isValidVersionOrRange(versionPart))
          throw new InvalidRef("Invalid ref: The provided version is not valid: " + versionPart);
        branch = branchPart;
      }
    }

    //Parse the version part
    if (versionPart.isEmpty() || HEAD.equals(versionPart))
      head = true;
    else if (ALL_VERSIONS.equals(versionPart))
      allVersions = true;
    else if (versionPart.contains(OP_RANGE))
      parseRange(versionPart);
    else
      try {
        version = validateVersion(Long.parseLong(versionPart));
      }
      catch (NumberFormatException e) {
        if (!Tag.isValidId(ref))
          throw new InvalidRef("Invalid ref: the provided ref is not a valid ref or version: \"" + versionPart + "\"");

        tag = ref;
      }
  }

  public Ref(long version) {
    this.version = validateVersion(version);
  }

  public Ref(long startVersion, long endVersion) {
    this(new Ref(startVersion), new Ref(endVersion));
  }

  public Ref(Ref startRef, Ref endRef) {
    start = startRef;
    end = endRef;
    validateRange();
  }

  public static Ref fromBranchId(String branchId) {
    return fromBranchId(branchId, new Ref(HEAD));
  }

  public static Ref fromBranchId(String branchId, long version) {
    return fromBranchId(branchId, new Ref(version));
  }

  public static Ref fromBranchId(String branchId, Ref versionRef) {
    if (isNullOrEmpty(branchId))
      branchId = MAIN;
    if (branchId.contains(":"))
      throw new InvalidRef("The branchId must be a valid branch ID. No colons allowed.");
    return new Ref(branchId + ":" + (versionRef.isHead() ? HEAD : versionRef.getVersion()));
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

  private void parseAndSetRangePart(String rangePart, boolean isStart) {
    try {
      Ref subRef = new Ref(rangePart);
      if (subRef.isAllVersions())
        throw new InvalidRef("Invalid ref: A range may not contain \"*\" (all versions).");
      if (isStart)
        start = subRef;
      else
        end = subRef;
    }
    catch (Exception e) {
      throw new InvalidRef("Invalid " + (isStart ? "start" : "end") + " of range: " + e.getMessage());
    }
  }

  private void validateRange() {
    if (getStart().isHead())
      throw new InvalidRef("Invalid ref: The provided version-range is invalid. The start of a range may not be HEAD!");

    if (isOnlyNumeric() && getStart().getVersion() > getEnd().getVersion())
      throw new InvalidRef("Invalid ref: The provided version-range is invalid. The start-version must not be greater than the end-version: "
          + "\"" + this + "\"");
  }

  @JsonValue
  @Override
  public String toString() {
    return toString(true);
  }

  private String toString(boolean withBranchPart) {
    if (!isTag() && version < 0 && !head && !allVersions && !isRange())
      throw new InvalidRef("Not a valid ref"); //Should never happen, only if this class has a bug

    if (isTag())
      return tag;

    String versionPart;
    if (isHead())
      versionPart = HEAD;
    else if (isAllVersions())
      versionPart = ALL_VERSIONS;
    else if (isRange())
      //TODO: Also return the branch part here in future
      return getStart().toString(false) + OP_RANGE + getEnd().toString(false);
    else
      versionPart = String.valueOf(version);
    return (withBranchPart ? getBranch() + ":" : "") + versionPart;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Ref))
      return false;
    Ref otherRef = (Ref) o;
    return otherRef.toString().equals(toString());
  }

  private static boolean isValidBranchId(String branchName) {
    return !canBeParsedAsNonNegativeLong(branchName);
  }

  private static boolean isValidVersionOrRange(String versionPart) {
    return HEAD.equals(versionPart) || ALL_VERSIONS.equals(versionPart) || canBeParsedAsNonNegativeLong(versionPart)
        || versionPart.contains(OP_RANGE);
  }

  private static boolean canBeParsedAsNonNegativeLong(String maybeALong) {
    try {
      long result = Long.parseLong(maybeALong);
      return result >= 0;
    }
    catch (NumberFormatException e) {
      return false;
    }
  }

  public String getBranch() {
    return branch;
  }

  public boolean isMainBranch() {
    return MAIN.equals(branch);
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
      throw new InvalidRef("Ref is not depicting a version but a tag.");
    if (!isSingleVersion())
      throw new InvalidRef("Ref is not depicting a single version.");
    if (isHead())
      throw new InvalidRef("Version number of alias HEAD is not known for this ref.");
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
      throw new InvalidRef("Ref is not depicting a version range.");
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
