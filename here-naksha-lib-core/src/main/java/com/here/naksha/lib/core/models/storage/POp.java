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

import static com.here.naksha.lib.core.models.storage.OpType.AND;
import static com.here.naksha.lib.core.models.storage.OpType.NOT;
import static com.here.naksha.lib.core.models.storage.OpType.OR;
import static com.here.naksha.lib.core.models.storage.POpType.CONTAINS;
import static com.here.naksha.lib.core.models.storage.POpType.EQ;
import static com.here.naksha.lib.core.models.storage.POpType.EXISTS;
import static com.here.naksha.lib.core.models.storage.POpType.GT;
import static com.here.naksha.lib.core.models.storage.POpType.GTE;
import static com.here.naksha.lib.core.models.storage.POpType.LT;
import static com.here.naksha.lib.core.models.storage.POpType.LTE;
import static com.here.naksha.lib.core.models.storage.POpType.NOT_NULL;
import static com.here.naksha.lib.core.models.storage.POpType.NULL;
import static com.here.naksha.lib.core.models.storage.POpType.STARTS_WITH;

import java.util.Objects;
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

  public static @NotNull POp isNull(@NotNull PRef propertyRef) {
    return new POp(NULL, propertyRef, null);
  }

  public static @NotNull POp isNotNull(@NotNull PRef propertyRef) {
    return new POp(NOT_NULL, propertyRef, null);
  }

  /**
   * If your property is array then provided value also has to be an array.
   * <pre>{@code ["value"]}</pre> <br>
   * If your property is object then provided value also has to be an object.
   * <pre>{@code {"prop":"value"}}</pre> <br>
   * If your property is primitive value then provided value also has to be primitive.
   * <pre>{@code "value"}</pre> <br>
   * Only top level values search are supported. For json:
   * <pre>{@code
   * {
   *   "type": "Feature",
   *   "properties": {
   *     "reference": [
   *       {
   *         "id": "106003684",
   *         "prop":{"a":1},
   *       }
   *     ]
   *   }
   * }
   * }</pre>
   * <br>
   * You can query path ["properties","reference"] by direct children:
   * <pre>{@code
   *   [{"id":"106003684"}]
   *   and
   *   [{"prop":{"a":1}}]
   * }</pre>
   * <br>
   * But querying by sub property that is not direct child won't work:
   * <pre>{@code {"a":1} }</pre>
   *
   * Also have in mind that provided {@link PRef} can't contain array properties in the middle of path.
   * Array property is allowed only as last element of path.
   * This is correct:
   * <pre>{@code properties -> reference}</pre><br>
   *
   * This is not correct:
   * <pre>{@code properties -> reference -> id}</pre>
   * beacause `reference` is an array
   *
   * @param propertyRef
   * @param value
   * @return
   */
  public static @NotNull POp contains(@NotNull PRef propertyRef, @NotNull Object value) {
    return new POp(CONTAINS, propertyRef, value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    POp pOp = (POp) o;
    return Objects.equals(propertyRef, pOp.propertyRef) && Objects.equals(value, pOp.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(propertyRef, value);
  }
}
