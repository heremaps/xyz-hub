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
package com.here.naksha.lib.core.exceptions;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.XyzError;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * An exception thrown if locking a specific feature in a not existing collection failed, basically, because the collection does not exist.
 */
@AvailableSince(NakshaVersion.v2_0_7)
public class StorageLockNoSuchCollection extends StorageLockException {

  /**
   * Create a new exception that locking a feature failed, because the collection does not exist.
   *
   * @param collectionId The collection that was tried to lock.
   * @param featureId    The feature that was tried to lock.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public StorageLockNoSuchCollection(@NotNull String collectionId, @NotNull String featureId) {
    super(XyzError.COLLECTION_NOT_FOUND);
    this.collectionId = collectionId;
    this.featureId = featureId;
  }

  /**
   * The collection for which the lock failed.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @NotNull String collectionId;

  /**
   * The feature for which the lock failed.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @NotNull String featureId;
}
