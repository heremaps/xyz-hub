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
package com.here.naksha.lib.core.models.storage;

import static com.here.naksha.lib.core.models.storage.POp.Constants.*;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class POp extends Op<POp> {

  public static class Constants {

    public static final int OP_EXISTS = 10;
    public static final int OP_STARTS_WITH = 11;
    public static final int OP_EQUALS = 12;
    public static final int OP_GT = 13;
    public static final int OP_GTE = 14;
    public static final int OP_LT = 15;
    public static final int OP_LTE = 16;
  }

  POp(int op, @NotNull POp... children) {
    super(op, children);
    this.propertyRef = null;
    this.value = null;
  }

  POp(int op, @NotNull PRef propertyRef, @Nullable Object value) {
    super(op);
    this.propertyRef = propertyRef;
    this.value = value;
  }

  private final @Nullable PRef propertyRef;
  private final @Nullable Object value;

  public @Nullable PRef propertyRef() {
    return propertyRef;
  }

  public @Nullable Object value() {
    return value;
  }

  public static @NotNull POp and(@NotNull POp... children) {
    return new POp(OP_AND, children);
  }

  public static @NotNull POp or(@NotNull POp... children) {
    return new POp(OP_OR, children);
  }

  public static @NotNull POp not(@NotNull POp op) {
    return new POp(OP_NOT, op);
  }

  public static @NotNull POp exists(@NotNull PRef propertyRef) {
    return new POp(OP_EXISTS, propertyRef, null);
  }

  public static @NotNull POp startsWith(@NotNull PRef propertyRef, @NotNull String prefix) {
    return new POp(OP_STARTS_WITH, propertyRef, prefix);
  }

  public static @NotNull POp eq(@NotNull PRef propertyRef, @NotNull String value) {
    return new POp(OP_EQUALS, propertyRef, value);
  }

  public static @NotNull POp eq(@NotNull PRef propertyRef, @NotNull Number value) {
    return new POp(OP_EQUALS, propertyRef, value);
  }

  public static @NotNull POp eq(@NotNull PRef propertyRef, @NotNull Boolean value) {
    return new POp(OP_EQUALS, propertyRef, value);
  }

  public static @NotNull POp gt(@NotNull PRef propertyRef, @NotNull Number value) {
    return new POp(OP_GT, propertyRef, value);
  }

  public static @NotNull POp gte(@NotNull PRef propertyRef, @NotNull Number value) {
    return new POp(OP_GTE, propertyRef, value);
  }

  public static @NotNull POp lt(@NotNull PRef propertyRef, @NotNull Number value) {
    return new POp(OP_LT, propertyRef, value);
  }

  public static @NotNull POp lte(@NotNull PRef propertyRef, @NotNull Number value) {
    return new POp(OP_LTE, propertyRef, value);
  }
}
