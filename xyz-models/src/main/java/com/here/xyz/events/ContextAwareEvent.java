/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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


package com.here.xyz.events;

import static com.here.xyz.models.hub.Space.DEFAULT_VERSIONS_TO_KEEP;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_DEFAULT)
public abstract class ContextAwareEvent<T extends ContextAwareEvent> extends Event<T> {

  private SpaceContext context = SpaceContext.DEFAULT;
  private int versionsToKeep = DEFAULT_VERSIONS_TO_KEEP;

  public enum SpaceContext {
    EXTENSION,
    SUPER,
    DEFAULT,
    COMPOSITE_EXTENSION;

    public static SpaceContext of(String value) {
      if (value == null) {
        return null;
      }
      try {
        return valueOf(value);
      } catch (IllegalArgumentException e) {
        return null;
      }
    }
  }

  /**
   * @return The space context in which the command depicted by this event will be executed.
   *  In case of a space which extends another one, this value depicts whether to execute the action only on the extension or on
   *  the whole extending space.
   */
  public SpaceContext getContext() {
    return context;
  }

  public void setContext(SpaceContext context) {
    if (context == null)
      throw new NullPointerException("Context can not be null.");
    this.context = context;
  }

  public T withContext(SpaceContext context) {
    setContext(context);
    return (T) this;
  }

  public int getVersionsToKeep() {
    return versionsToKeep;
  }

  public void setVersionsToKeep(int versionsToKeep) {
    this.versionsToKeep = versionsToKeep;
  }

  public T withVersionsToKeep(int versionsToKeep) {
    setVersionsToKeep(versionsToKeep);
    return (T) this;
  }
}
