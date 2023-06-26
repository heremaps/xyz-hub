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
package com.here.naksha.lib.core.models.geojson.implementation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** The actions that are supported by Naksha. */
public final class Action {

  private Action(@NotNull String action) {
    value = action;
  }

  private final @NotNull String value;

  /**
   * Returns an action instance for the given value.
   *
   * @param value the value to check.
   * @return the action instance matching or {@code null}.
   */
  @JsonCreator
  public static @Nullable Action get(@Nullable String value) {
    if (value != null) {
      // Note:
      if (CREATE.value.equalsIgnoreCase(value)) {
        return CREATE;
      }
      if (UPDATE.value.equalsIgnoreCase(value)) {
        return UPDATE;
      }
      if (DELETE.value.equalsIgnoreCase(value)) {
        return DELETE;
      }
    }
    return null;
  }

  /**
   * The feature has just been created, the {@link XyzNamespace#getVersion() version} will be {@code
   * 1}.
   */
  public static final Action CREATE = new Action("CREATE");

  /**
   * The feature has been updated, the {@link XyzNamespace#getVersion() version} will be greater
   * than {@code 1}.
   */
  public static final Action UPDATE = new Action("UPDATE");

  /**
   * The feature has been deleted, the {@link XyzNamespace#getVersion() version} will be greater
   * than {@code 1}. No other state with a higher version should be possible.
   */
  public static final Action DELETE = new Action("DELETE");

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    return this == o;
  }

  /**
   * Tests if the given text represents this action.
   *
   * @param text the text to test.
   * @return true if the given text represents this action; false otherwise.
   */
  public boolean equals(@Nullable String text) {
    return value.equals(text);
  }

  public @NotNull String toString() {
    return value;
  }
}
