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
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * When a request failed, this returns the error details.
 */
@AvailableSince(NakshaVersion.v2_0_7)
public class ErrorResult extends Result {

  @AvailableSince(NakshaVersion.v2_0_7)
  public ErrorResult(@NotNull XyzError reason, @NotNull String message) {
    this(reason, message, null);
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public ErrorResult(@NotNull XyzError reason, @NotNull String message, @Nullable Throwable exception) {
    this.reason = reason;
    this.message = message;
    this.exception = exception;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public final @NotNull XyzError reason;

  @AvailableSince(NakshaVersion.v2_0_7)
  public final @NotNull String message;

  @AvailableSince(NakshaVersion.v2_0_7)
  public final @Nullable Throwable exception;

  @Override
  @AvailableSince(NakshaVersion.v2_0_7)
  public String toString() {
    return "ErrorResult{" + "reason=" + reason + ", message='" + message + '\'' + ", exception=" + exception + '}';
  }
}
