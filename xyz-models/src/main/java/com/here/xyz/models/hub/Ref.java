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
import org.apache.commons.lang3.StringUtils;

public class Ref implements XyzSerializable {

  public static final String HEAD = "HEAD";
  public static final String ALL_VERSIONS = "*";
  private long version = -1;
  private boolean head;
  private boolean allVersions;
  private String versionRef;
  private boolean resolved;

  @JsonCreator
  public Ref(String ref) {
    if (ref == null || ref.isEmpty() || HEAD.equals(ref)) {
      head = true;
      setResolved(true);
    }
    else if (ALL_VERSIONS.equals(ref)) {
      allVersions = true;
      setResolved(true);
    }
    else if (StringUtils.isNumeric(ref)) {
      try {
        setVersion(Long.parseLong(ref));
        setResolved(true);
      } catch (NumberFormatException e) {
        throw new InvalidRef("Invalid ref: the provided ref is not a valid ref or version: \"" + ref + "\"");
      }
    }
    else {
      this.versionRef = ref;
      setResolved(false);
    }
  }

  public Ref(long version) {
    setVersion(version);
  }

  public void setVersion(long version) {
    if (version < 0)
      throw new InvalidRef("Invalid ref: The provided version number may not be lower than 0");

    this.version = version;
    setResolved(true);
  }

  @JsonValue
  @Override
  public String toString() {
    if (version < 0 && !head && !allVersions && resolved)
      throw new IllegalArgumentException("Not a valid ref");
    if (head)
      return HEAD;
    if (allVersions)
      return ALL_VERSIONS;
    if (resolved)
      return String.valueOf(version);
    return versionRef;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof Ref otherRef && otherRef.toString().equals(toString());
  }

  public long getVersion() {
    if (!isSingleVersion())
      throw new NumberFormatException("Ref is not depicting a single version.");
    if (isHead())
      throw new NumberFormatException("Version number of alias HEAD is not known for this ref.");
    return version;
  }

  public boolean isHead() {
    return head;
  }

  public boolean isAllVersions() {
    return allVersions;
  }

  public boolean isSingleVersion() {
    return !isAllVersions();
  }

  public boolean isResolved() {
    return resolved;
  }

  public void setResolved(boolean resolved) {
    this.resolved = resolved;
  }

  public String getVersionRef() {
    return versionRef;
  }

  public static class InvalidRef extends IllegalArgumentException {

    private InvalidRef(String message) {
      super(message);
    }
  }
}
