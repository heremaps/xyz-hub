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
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.EXyzAction;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.util.json.JsonEnum;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.ApiStatus.Experimental;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The operation that was actually executed.
 */
@SuppressWarnings("unused")
@AvailableSince(NakshaVersion.v2_0_7)
public class EExecutedOp extends JsonEnum {

  /**
   * Returns the execution operation that matches the given character sequence.
   *
   * @param chars The character sequence to translate.
   * @return The execution operation.
   */
  public static @NotNull EExecutedOp get(@Nullable CharSequence chars) {
    return get(EExecutedOp.class, chars);
  }

  /**
   * A helper to detect {@code null} values, which are not allowed.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final EExecutedOp NULL = def(EExecutedOp.class, null);

  /**
   * The state in the database was not changed by the operation. The returned {@link FeatureCodec#feature} will reflect the current version
   * being in the database, which may actually be {@code null}, when the feature does not exist.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final EExecutedOp RETAINED = def(EExecutedOp.class, "RETAINED");

  /**
   * A read operation was executed. The read version is returned in the {@link FeatureCodec#feature}. Note that features that do not exist,
   * are simply not returned from the storage. Therefore, {@link FeatureCodec#feature} should never be {@code null}, when this executed
   * operation is returned.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final EExecutedOp READ = def(EExecutedOp.class, "READ");

  /**
   * A new feature was created. The created feature is returned in the {@link FeatureCodec#feature}.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final EExecutedOp CREATED = def(EExecutedOp.class, "CREATED");

  /**
   * A feature was updated. The new version, after the update, is returned in the {@link FeatureCodec#feature}.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final EExecutedOp UPDATED = def(EExecutedOp.class, "UPDATED");

  /**
   * The feature was deleted. The new (deleted) version is returned in the {@link FeatureCodec#feature}, with must have the
   * {@link XyzNamespace#getAction()} set to {@link EXyzAction#DELETE}.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final EExecutedOp DELETED = def(EExecutedOp.class, "DELETED");

  /**
   * The feature was purged. The purged version is returned in the {@link FeatureCodec#feature}, this feature must have the
   * {@link XyzNamespace#getAction()} set to {@link EXyzAction#DELETE}. Note that purge does not create a new state, it just removes the
   * deleted version from a deleted features cache of the storage.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final EExecutedOp PURGED = def(EExecutedOp.class, "PURGED");

  /**
   * A feature was restored. The restored version is returned in {@link FeatureCodec#feature}.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @Experimental
  public static final EExecutedOp RESTORED = def(EExecutedOp.class, "RESTORED");

  /**
   * The operation failed. The error details are encoded in the {@link FeatureCodec#err}, which will hold a {@link XyzError} that may be
   * used for more details. For example, if a conflict occurred in an atomic operation, the {@link XyzError#CONFLICT} will be returned.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final EExecutedOp ERROR = def(EExecutedOp.class, "ERROR");

  @Override
  protected void init() {
    register(EExecutedOp.class);
  }
}
