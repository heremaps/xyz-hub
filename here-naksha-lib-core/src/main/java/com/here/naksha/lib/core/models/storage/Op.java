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

/**
 * Base class for all operations. Only operations of the same type can be combined.
 *
 * @param <SELF> The type of the operation itself.
 */
@SuppressWarnings("unchecked")
public abstract class Op<SELF extends Op<SELF>> {

  Op(@NotNull OpType op) {
    this.op = op;
    this.children = null;
  }

  @SuppressWarnings({"ManualArrayToCollectionCopy", "UseBulkOperation"})
  Op(@NotNull OpType op, @NotNull SELF... children) {
    this.op = op;
    if (children != null && children.length > 0) {
      this.children = new ArrayList<>(children.length);
      for (final SELF child : children) {
        this.children.add(child);
      }
    } else {
      this.children = null;
    }
  }

  final @NotNull OpType op;
  final @Nullable List<@NotNull SELF> children;

  /**
   * Returns the operation.
   * @return the operation.
   */
  public @NotNull OpType op() {
    return op;
  }

  /**
   * Returns all children of this operation.
   *
   * @return all children of this operation.
   */
  public @Nullable List<@NotNull SELF> children() {
    return children;
  }

  /**
   * Adds the given children.
   * @param child The children to add.
   * @return this.
   */
  public @NotNull SELF add(@NotNull SELF child) {
    assert children != null;
    children.add(child);
    return (SELF) this;
  }
}
