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
import org.jetbrains.annotations.Nullable;

@AvailableSince(NakshaVersion.v2_0_7)
public class StorageLockException extends StorageException {

  @AvailableSince(NakshaVersion.v2_0_8)
  public StorageLockException(@NotNull XyzError reason) {
    super(reason);
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public StorageLockException(@NotNull String message) {
    super(message);
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public StorageLockException(@NotNull String message, @Nullable Throwable cause) {
    super(message, cause);
  }
}
