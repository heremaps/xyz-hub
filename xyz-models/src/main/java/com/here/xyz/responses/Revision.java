/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.responses;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;

import com.here.xyz.responses.changesets.Changeset;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "Revision")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Revision extends XyzResponse<Revision> {
  /**
   * The revision number.
   */
  private int rev;

  /**
   * The author which caused the changes in the revision.
   */
  private String author;

  /**
   * The timestamp, when the revision was created.
   */
  private long createdAt;

  /**
   * The set of changes, including insertions, deletions and updates, that are part of this revision.
   */
  private Changeset changeset;

  public int getRev() {
    return rev;
  }

  public void setRev(int rev) {
    this.rev = rev;
  }

  public Revision withRev(int rev) {
    this.rev = rev;
    return this;
  }

  public String getAuthor() {
    return author;
  }

  public void setAuthor(String author) {
    this.author = author;
  }

  public Revision withAuthor(String author) {
    this.author = author;
    return this;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }

  public Revision withCreatedAt(long createdAt) {
    this.createdAt = createdAt;
    return this;
  }

  public Changeset getChangeset() {
    return changeset;
  }

  public void setChangeset(Changeset changeset) {
    this.changeset = changeset;
  }

  public Revision withChangeset(Changeset changeset) {
    this.changeset = changeset;
    return this;
  }
}
