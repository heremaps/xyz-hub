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
package com.here.naksha.lib.core.storage;

import static com.here.naksha.lib.core.NakshaVersion.v2_0_5;

import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Delete the entity with the given identifier in the given state. The delete will fail, if the entity is not in the desired state.
 */
@Deprecated
@AvailableSince(v2_0_5)
public class DeleteOp {

  private final String id;
  private final String uuid;

  /**
   * @param id   the identifier of the feature to delete.
   * @param uuid the UUID of the state to delete, {@code null}, if any state is acceptable.
   */
  public DeleteOp(@NotNull String id, @Nullable String uuid) {
    this.id = id;
    this.uuid = uuid;
  }

  public String getId() {
    return id;
  }

  public String getUuid() {
    return uuid;
  }

  @AvailableSince(v2_0_5)
  public DeleteOp(@NotNull String id) {
    this(id, null);
  }
}
