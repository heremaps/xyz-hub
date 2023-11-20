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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.XyzError;
import javax.annotation.concurrent.ThreadSafe;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * A special class to signal an encoder or decoder error.
 */
@ThreadSafe
@AvailableSince(NakshaVersion.v2_0_7)
public class CodecError {

  @JsonCreator
  public CodecError(@JsonProperty("err") @NotNull XyzError err, @JsonProperty("msg") @NotNull String msg) {
    this.err = err;
    this.msg = msg;
  }

  /**
   * The error code
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonProperty("err")
  public final @NotNull XyzError err;

  /**
   * A human readable error message.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonProperty("msg")
  public final @NotNull String msg;
}
