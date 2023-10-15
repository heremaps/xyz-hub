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

import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unchecked")
public abstract class Op<T extends Op<T>> {

  protected Op(int op) {
    this.op = op;
    this.children = null;
  }

  @SuppressWarnings({"ManualArrayToCollectionCopy", "UseBulkOperation"})
  protected Op(int op, @NotNull T... children) {
    this.op = op;
    if (children != null && children.length > 0) {
      this.children = new ArrayList<>(children.length);
      for (final T child : children) {
        this.children.add(child);
      }
    } else {
      this.children = null;
    }
  }

  public static final int OP_AND = 1;
  public static final int OP_OR = 2;
  public static final int OP_NOT = 3;

  final int op;
  final @Nullable List<@NotNull T> children;

  public int op() {
    return op;
  }

  public @Nullable List<@NotNull T> children() {
    return children;
  }

  public @NotNull T add(@NotNull T child) {
    assert children != null;
    children.add(child);
    return (T) this;
  }
}
