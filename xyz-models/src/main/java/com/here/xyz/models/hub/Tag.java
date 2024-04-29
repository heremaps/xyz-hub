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

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT;
import static com.here.xyz.models.hub.Ref.ALL_VERSIONS;
import static com.here.xyz.models.hub.Ref.HEAD;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonView;
import com.google.common.base.Strings;
import com.here.xyz.XyzSerializable;
import java.util.regex.Pattern;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Tag implements XyzSerializable {
  /**
   * The reader id.
   */
  @JsonView({Public.class, Static.class})
  private String id;

  /**
   * The space id.
   */
  @JsonView(Static.class)
  private String spaceId;

  /**
   * The version this tag is pointing to.
   *
   * @see Ref#getVersion()
   */
  @JsonView({Public.class, Static.class})
  private long version = -2;

  /**
   * The indicator that this tag is a system tag, which is not allowed to be deleted or modified by users.
   */
  @JsonView({Public.class, Static.class})
  @JsonInclude(NON_DEFAULT)
  private boolean system;

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

  public boolean isSystem() {
    return system;
  }

  public void setSystem(boolean system) {
    this.system = system;
  }

  public Tag withSystem(boolean system) {
   setSystem(system);
   return this;
  }

  public static boolean isValidId(String tagId) {
    return !Strings.isNullOrEmpty(tagId) && !HEAD.equals(tagId) && !ALL_VERSIONS.equals(tagId)
        && Pattern.matches("^[a-zA-Z][a-zA-Z0-9_-]{0,49}$", tagId);
  }
}
