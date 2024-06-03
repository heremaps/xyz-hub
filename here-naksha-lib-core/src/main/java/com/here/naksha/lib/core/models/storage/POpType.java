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

import org.jetbrains.annotations.Nullable;

public class POpType extends OpType {

  public static final POpType EXISTS = defIgnoreCase(POpType.class, "exists");
  public static final POpType STARTS_WITH = defIgnoreCase(POpType.class, "startsWith");
  public static final POpType EQ = defIgnoreCase(POpType.class, "eq").with(POpType.class, (self) -> self.op = "=");
  public static final POpType GT = defIgnoreCase(POpType.class, "gt").with(POpType.class, (self) -> self.op = ">");
  public static final POpType GTE = defIgnoreCase(POpType.class, "gte").with(POpType.class, (self) -> self.op = ">=");
  public static final POpType LT = defIgnoreCase(POpType.class, "lt").with(POpType.class, (self) -> self.op = "<");
  public static final POpType LTE = defIgnoreCase(POpType.class, "lte").with(POpType.class, (self) -> self.op = "<=");
  public static final POpType NULL = defIgnoreCase(POpType.class, "null");
  public static final POpType NOT_NULL = defIgnoreCase(POpType.class, "notNull");
  public static final POpType CONTAINS =
      defIgnoreCase(POpType.class, "contains").with(POpType.class, (self) -> self.op = "@>");

  private @Nullable String op;

  public @Nullable String op() {
    return op;
  }
}
