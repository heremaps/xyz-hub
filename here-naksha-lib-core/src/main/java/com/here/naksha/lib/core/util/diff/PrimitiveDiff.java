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
package com.here.naksha.lib.core.util.diff;

import java.util.Objects;

/** A common interface for primitive changes. */
public abstract class PrimitiveDiff implements Difference {

  private final Object newValue;
  private final Object oldValue;

  public PrimitiveDiff(Object oldValue, Object newValue) {
    this.oldValue = oldValue;
    this.newValue = newValue;
  }

  /**
   * Returns the old value.
   *
   * @return the old value.
   */
  public Object oldValue() {
    return oldValue;
  }

  /**
   * Returns the new value.
   *
   * @return the new value.
   */
  public Object newValue() {
    return newValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PrimitiveDiff that = (PrimitiveDiff) o;
    return Objects.equals(newValue, that.newValue) && Objects.equals(oldValue, that.oldValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(newValue, oldValue);
  }
}
