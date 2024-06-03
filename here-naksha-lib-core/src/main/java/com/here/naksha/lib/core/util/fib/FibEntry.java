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
package com.here.naksha.lib.core.util.fib;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.util.ILike;
import java.util.Objects;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An entry in the {@link FibSet}.
 */
@AvailableSince(NakshaVersion.v2_0_5)
public class FibEntry<K> implements ILike {

  /**
   * Create a new entry for a {@link FibSet}.
   * @param key the key.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public FibEntry(@NotNull K key) {
    this.key = key;
  }

  /**
   * The key of the entry.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  public final @NotNull K key;

  /**
   * The default implementation just invokes {@link ILike#equals(Object, Object)} to the {@link #key} and the given {@code key} object.
   *
   * @param key the key to compare this entry against.
   * @return {@code true}, if this entry is like the given key.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  @Override
  public boolean isLike(@Nullable Object key) {
    return ILike.equals(this.key, key);
  }

  @Override
  public final int hashCode() {
    return key.hashCode();
  }

  @Override
  public final boolean equals(@Nullable Object other) {
    if (other == key) {
      return true;
    }
    if (other == null || other.getClass() != key.getClass()) {
      return false;
    }
    return Objects.equals(key, other);
  }

  @Override
  public @NotNull String toString() {
    return key.toString();
  }
}
