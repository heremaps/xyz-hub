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
package com.here.naksha.lib.core.models.storage;

import static com.here.naksha.lib.core.models.storage.OpType.AND;
import static com.here.naksha.lib.core.models.storage.OpType.NOT;
import static com.here.naksha.lib.core.models.storage.OpType.OR;
import static com.here.naksha.lib.core.models.storage.POpType.EQ;
import static com.here.naksha.lib.core.models.storage.POpType.EXISTS;
import static com.here.naksha.lib.core.models.storage.POpType.GT;
import static com.here.naksha.lib.core.models.storage.POpType.GTE;
import static com.here.naksha.lib.core.models.storage.POpType.LT;
import static com.here.naksha.lib.core.models.storage.POpType.LTE;
import static com.here.naksha.lib.core.models.storage.POpType.STARTS_WITH;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * All property operations.
 */
public class POp extends Op<POp> {

  POp(@NotNull OpType op, @NotNull POp... children) {
    super(op, children);
    this.propertyRef = null;
    this.value = null;
  }

  POp(@NotNull OpType op, @NotNull PRef propertyRef, @Nullable Object value) {
    super(op);
    this.propertyRef = propertyRef;
    this.value = value;
  }

  private final @Nullable PRef propertyRef;
  private final @Nullable Object value;

  /**
   * Returns the property reference.
   * @return the property reference.
   */
  public @Nullable PRef getPropertyRef() {
    return propertyRef;
  }

  public @Nullable Object getValue() {
    return value;
  }

  public static @NotNull POp and(@NotNull POp... children) {
    return new POp(AND, children);
  }

  public static @NotNull POp or(@NotNull POp... children) {
    return new POp(OR, children);
  }

  public static @NotNull POp not(@NotNull POp op) {
    return new POp(NOT, op);
  }

  public static @NotNull POp exists(@NotNull PRef propertyRef) {
    return new POp(EXISTS, propertyRef, null);
  }

  public static @NotNull POp startsWith(@NotNull PRef propertyRef, @NotNull String prefix) {
    return new POp(STARTS_WITH, propertyRef, prefix);
  }

  public static @NotNull POp eq(@NotNull PRef propertyRef, @NotNull String value) {
    return new POp(EQ, propertyRef, value);
  }

  public static @NotNull POp eq(@NotNull PRef propertyRef, @NotNull Number value) {
    return new POp(EQ, propertyRef, value);
  }

  public static @NotNull POp eq(@NotNull PRef propertyRef, @NotNull Boolean value) {
    return new POp(EQ, propertyRef, value);
  }

  public static @NotNull POp gt(@NotNull PRef propertyRef, @NotNull Number value) {
    return new POp(GT, propertyRef, value);
  }

  public static @NotNull POp gte(@NotNull PRef propertyRef, @NotNull Number value) {
    return new POp(GTE, propertyRef, value);
  }

  public static @NotNull POp lt(@NotNull PRef propertyRef, @NotNull Number value) {
    return new POp(LT, propertyRef, value);
  }

  public static @NotNull POp lte(@NotNull PRef propertyRef, @NotNull Number value) {
    return new POp(LTE, propertyRef, value);
  }
}
