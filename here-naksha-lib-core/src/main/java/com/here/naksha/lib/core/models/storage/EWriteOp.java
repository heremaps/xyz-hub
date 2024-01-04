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

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.util.json.JsonEnum;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.ApiStatus.Experimental;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The write operations that should be performed.
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
   * A helper to detect {@code null} values. This is not an allowed real operation.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final EWriteOp NULL = def(EWriteOp.class, null);

  /**
   * Create a new feature. Requires that the {@link FeatureCodec#feature} is provided as parameter. Before being executed by the storage,
   * the storage will invoke {@link FeatureCodec#decodeParts(boolean)} to disassemble the feature into its parts. If no
   * {@link FeatureCodec#id} is decoded, the storage will generate a random identifier for the feature.
   * <p>
   *  If a feature with this {@code id} exists, the operation will fail with {@link EExecutedOp#ERROR} and the error detail will be
   *  {@link XyzError#CONFLICT}.
   */
  public static final @NotNull EWriteOp CREATE = def(EWriteOp.class, "CREATE");

  /**
   * Update an existing feature. Requires that the {@link FeatureCodec#feature} is provided as parameter. Before being executed by the
   * storage, the storage will invoke {@link FeatureCodec#decodeParts(boolean)} to disassemble the feature into its parts.
   *
   * <p>Requires that an {@link FeatureCodec#id} is decoded, otherwise an error will be the result. If a {@link FeatureCodec#uuid} is
   * decoded form the {@link FeatureCodec#feature}, then the operation becomes atomic. That means, it requires that the current version is
   * exactly the one referred to by the given {@link FeatureCodec#uuid}. If this is not the case, it will fail. If {@link FeatureCodec#uuid}
   * is {@code null}, then the operation only requires that the features exist, no matter in what state, it will be overridden by the new
   * version.
   */
  public static final @NotNull EWriteOp UPDATE = def(EWriteOp.class, "UPDATE");
  // When not exists: ERROR + XyzError.NOT_FOUND
  // When conflict: ERROR + XyzError.CONFLICT
  // When other: ERROR + XyzError.EXCEPTION

  /**
   * Create or update a feature. Requires that the {@link FeatureCodec#feature} is provided as parameter. Before being executed by the
   * storage, the storage will invoke {@link FeatureCodec#decodeParts(boolean)} to disassemble the feature into its parts.
   *
   * <p>If no {@link FeatureCodec#id} is decoded, a random {@code id} will be generated. This operation will first try to create the
   * feature, if this fails, because a feature with the same {@code id} exist already, it will try to update the existing feature. In this
   * case, it will exactly behave like described in {@link #UPDATE}. So only in this case, the {@link FeatureCodec#uuid} is taken into
   * consideration.
   */
  public static final @NotNull EWriteOp PUT = def(EWriteOp.class, "PUT");

  /**
   * Delete a feature. If a {@link FeatureCodec#feature} is provided as parameter, then before being executed by the storage, the storage
   * will invoke {@link FeatureCodec#decodeParts(boolean)} to disassemble the feature into its parts. However, if no
   * {@link FeatureCodec#feature} is provided (so being {@code null}), the operation requires that at least an {@link FeatureCodec#id} is
   * provided. If additionally an {@link FeatureCodec#uuid} is provided, it makes the operation atomic.
   *
   * <p>If not being atomic, so no {@link FeatureCodec#uuid} ({@code null}) given, the feature with the given {@link FeatureCodec#id} will
   * be deleted, no matter in which version it exists. If being atomic, so a {@link FeatureCodec#uuid} was given, then the feature with the
   * given {@link FeatureCodec#id} will only be deleted, if it exists in the requested version. If it does not exist, the atomic delete
   * will not fail, but return a {@link EExecutedOp#RETAINED} with the feature being {@code null}, the same way the none-atomic version
   * would do.
   *
   * <p>The operation is generally treated as successful, when the outcome of the operation is that the feature is eventually deleted. So,
   * if the feature did not exist (was already deleted), the operation will return as the executed operation {@link EExecutedOp#RETAINED}
   * with the {@link FeatureCodec#feature} returned being {@code null}. If the features existed, then two outcomes are possible. Either the
   * operation succeeds, returning the executed operation {@link EExecutedOp#DELETED} with the version of the feature that was deleted
   * {@link FeatureCodec#feature}, or it returns {@link EExecutedOp#ERROR} with the current version of the feature being returned. This may
   * actually only happen, when the operation is atomic and the given expected {@link FeatureCodec#uuid} does not match the one of the
   * current version stored in the storage.
   */
  public static final @NotNull EWriteOp DELETE = def(EWriteOp.class, "DELETE");
  // When okay: RETAINED (feature = null)
  // When okay: DELETED (feature = set)
  // When conflict: ERROR + XyzError.CONFLICT

  /**
   * Eventually delete a feature. If a {@link FeatureCodec#feature} is provided as parameter, then before being executed by the storage, the
   * storage will invoke {@link FeatureCodec#decodeParts(boolean)} to disassemble the feature into its parts. However, if no
   * {@link FeatureCodec#feature} is provided (so being {@code null}), the operation requires that at least an {@link FeatureCodec#id} is
   * provided. If additionally an {@link FeatureCodec#uuid} is provided, it makes the operation atomic. This is a two-in-one operation.
   *
   * <p>The first part will execute a normal {@link #DELETE} operation. The result of this {@link #DELETE} will only be part of the result
   * set, if the feature was actually deleted. In that case an additional result with the operation being {@link EExecutedOp#DELETED} is
   * added to the cursor. If the feature was already deleted, the implied delete does not return any result. If the implied delete fails,
   * which can only happen for an atomic operation with a {@code uuid} provided, the operation will skip the purge. If an actual
   * {@link EExecutedOp#DELETED} happened and a {@link FeatureCodec#uuid uuid} was provided, the {@code uuid} is updated to the new one of
   * the created deleted state before performing the actual purge, so that the purge will not fail. In a nutshell this means, the purge is
   * only executed if the feature either was already deleted (no result), or the implied delete succeeds (returns the deleted version).
   *
   * <p>The <b>purge</b> itself will remove the deleted feature from the storage cache and eliminate the possibility to perform an
   * {@link #RESTORE} or to retrieve the feature via the {@link ReadFeatures#returnDeleted} parameter in {@link ReadFeatures} requests. The
   * purge itself has three outcomes: Either successfully executed, returning {@link EExecutedOp#PURGED} with the
   * {@link FeatureCodec#feature} version that was removed from the storage, or, if there is no deleted version available in the storage,
   * with a {@link EExecutedOp#RETAINED} with {@link FeatureCodec#feature} being {@code null}. The last outcome can only happen if the purge
   * is executed atomically, so a {@link FeatureCodec#uuid} was provided. In that case {@link EExecutedOp#ERROR} will be returned with the
   * {@link FeatureCodec#feature} version that was not deleted, because of a conflict.
   */
  public static final @NotNull EWriteOp PURGE = def(EWriteOp.class, "PURGE");

  /**
   * Tries to restore a deleted feature.
   */
  @Experimental
  public static final @NotNull EWriteOp RESTORE = def(EWriteOp.class, "RESTORE");

  @Override
  protected void init() {
    register(EWriteOp.class);
  }
}
