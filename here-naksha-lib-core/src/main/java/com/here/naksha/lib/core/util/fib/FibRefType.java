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
package com.here.naksha.lib.core.util.fib;

import com.here.naksha.lib.core.NakshaVersion;
import java.lang.ref.Reference;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link FibSet} reference type.
 */
@AvailableSince(NakshaVersion.v2_0_5)
public interface FibRefType {

  /**
   * Tests if the given reference should be upgraded.
   *
   * @param raw either the {@link FibEntry} or a {@link Reference} to the entry.
   * @return {@code true} if the reference should be upgraded; {@code false} otherwise.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  @SuppressWarnings({"rawtypes", "unchecked"})
  default boolean upgradeRaw(@NotNull Object raw) {
    if (raw instanceof Reference) {
      Reference ref = (Reference) raw;
      return upgradeRef(ref);
    }
    if (raw instanceof FibEntry) {
      FibEntry entry = (FibEntry) raw;
      return upgradeStrong(entry);
    }
    // This must not happen!
    assert false;
    return false;
  }

  /**
   * Tests if the given strong reference to the given entry should be upgraded.
   *
   * @param entry the entry.
   * @return {@code true} if the reference should be upgraded; {@code false} otherwise.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  boolean upgradeStrong(@NotNull FibEntry<?> entry);

  /**
   * Tests if the given reference to the given entry should be upgraded.
   *
   * @param reference the reference to the entry.
   * @return {@code true} if the reference should be upgraded; {@code false} otherwise.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  boolean upgradeRef(@NotNull Reference<FibEntry<?>> reference);

  /**
   * Creates a new reference for the given entry.
   *
   * @param entry the entry for which to return a new reference.
   * @return either a {@link Reference} or the given {@code entry} again, if a strong reference wanted.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  @NotNull
  Object newRef(@NotNull FibEntry<?> entry);
}
