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

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.util.json.JsonEnum;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The write operations that can be performed.
 */
@SuppressWarnings("unused")
@AvailableSince(NakshaVersion.v2_0_7)
public class EWriteOp extends JsonEnum {

  /**
   * Returns the write operation that matches the given character sequence.
   *
   * @param chars The character sequence to translate.
   * @return The write operation.
   */
  public static @NotNull EWriteOp get(@Nullable CharSequence chars) {
    return get(EWriteOp.class, chars);
  }

  /**
   * A helper to detect {@code null} values, which are not allowed.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final EWriteOp NULL = def(EWriteOp.class, null);

  /**
   * Create a new feature or collection. Fails if the feature or collection exist.
   */
  public static final @NotNull EWriteOp CREATE = def(EWriteOp.class, "CREATE");

  /**
   * Update a feature or collection. Fails if the feature or collection does not exist.
   */
  public static final @NotNull EWriteOp UPDATE = def(EWriteOp.class, "UPDATE");

  /**
   * Create a feature or collection. If the feature or collection exist, update it.
   */
  public static final @NotNull EWriteOp PUT = def(EWriteOp.class, "PUT");

  /**
   * Delete a feature or collection. Does not fail, if the feature or collection do not exist (are already deleted).
   */
  public static final @NotNull EWriteOp DELETE = def(EWriteOp.class, "DELETE");

  /**
   * Delete a feature or collection in a way that makes it impossible to restore it. Does not fail, if the feature or collection do not
   * exist (are already deleted).
   */
  public static final @NotNull EWriteOp PURGE = def(EWriteOp.class, "PURGE");

  /**
   * Restore a deleted collection. This operation is only allowed in an {@link WriteCollections} request and fails, if the collection is not
   * deleted or if applied in a {@link WriteFeatures} request.
   */
  public static final @NotNull EWriteOp RESTORE = def(EWriteOp.class, "RESTORE");

  @Override
  protected void init() {
    register(EWriteOp.class);
  }
}
