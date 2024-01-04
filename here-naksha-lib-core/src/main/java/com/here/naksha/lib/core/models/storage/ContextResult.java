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
import java.util.List;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ContextResult<FEATURE, CTX_TYPE, V_TYPE, CODEC extends FeatureCodec<FEATURE, CODEC>>
    extends SuccessResult {

  /**
   * The list of features to be returned as context
   */
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_11)
  private @Nullable List<@NotNull CTX_TYPE> context;

  /**
   * The list of violations to be returned as context
   */
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_11)
  private @Nullable List<@NotNull V_TYPE> violations;

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_11)
  public ContextResult(final @Nullable ForwardCursor<FEATURE, CODEC> cursor) {
    this.cursor = cursor;
  }

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_11)
  public @Nullable List<CTX_TYPE> getContext() {
    return context;
  }

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_11)
  public void setContext(@Nullable List<CTX_TYPE> context) {
    this.context = context;
  }

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_11)
  public @Nullable List<V_TYPE> getViolations() {
    return violations;
  }

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_11)
  public void setViolations(@Nullable List<V_TYPE> violations) {
    this.violations = violations;
  }
}
