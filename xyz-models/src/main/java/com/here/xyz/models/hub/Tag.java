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

package com.here.xyz.models.hub;

public class Tag {
  /**
   * The reader id.
   */
  private String id;

  /**
   * The space id.
   */
  private String spaceId;

  /**
   * The version pointer.
   * The version -2 is an invalid version.
   * The version -1 represents a tag pointing to no data in the space.
   * The version 0 points to the initial version of a space.
   */
  private long version = -2;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Tag withId(String id) {
    setId(id);
    return this;
  }

  public String getSpaceId() {
    return spaceId;
  }

  public void setSpaceId(String spaceId) {
    this.spaceId = spaceId;
  }

  public Tag withSpaceId(String spaceId) {
    setSpaceId(spaceId);
    return this;
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public Tag withVersion(long version) {
   setVersion(version);
   return this;
  }
}
