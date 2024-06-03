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
package com.here.naksha.lib.core.models.payload.events;

import org.jetbrains.annotations.NotNull;

public enum QueryParameterType {
  ANY(0),
  ANY_OR_NULL(1),

  STRING(2),
  STRING_OR_NULL(3),

  BOOLEAN(4),
  BOOLEAN_OR_NULL(5),

  LONG(6),
  LONG_OR_NULL(7),

  DOUBLE(8),
  DOUBLE_OR_NULL(9);

  QueryParameterType(int i) {
    this.nullable = (i & 1) == 1;
    this.maybeString = i < 2 || i == 2 || i == 3;
    this.maybeBoolean = i < 2 || i == 4 || i == 5;
    this.maybeNumber = i < 2 || i == 6 || i == 7 || i == 8 || i == 9;
    this.maybeLong = i < 2 || i == 6 || i == 7;
    this.maybeDouble = i < 2 || i == 8 || i == 9;
    if (i == 2 || i == 3) {
      string = "string";
    } else if (i == 4 || i == 5) {
      string = "boolean";
    } else if (i == 6 || i == 7) {
      string = "long";
    } else if (i == 8 || i == 9) {
      string = "double";
    } else {
      string = "any";
    }
  }

  public final boolean nullable;
  public final boolean maybeString;
  public final boolean maybeBoolean;
  public final boolean maybeNumber;
  public final boolean maybeLong;
  public final boolean maybeDouble;
  private final @NotNull String string;

  @Override
  public @NotNull String toString() {
    return string;
  }
}
