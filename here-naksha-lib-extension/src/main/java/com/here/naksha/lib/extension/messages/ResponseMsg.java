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
package com.here.naksha.lib.extension.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * Send by the extension to Naksha-Hub, when an extension is done with processing an event. Send as well by Naksha-Hub as response to a
 * {@link SendUpstreamMsg}.
 */
@AvailableSince(NakshaVersion.v2_0_3)
@JsonTypeName(value = "naksha.ext.rpc.v1.returnResponse")
public class ResponseMsg extends ExtensionMessage {

  public static final String RESPONSE = "response";

  @AvailableSince(NakshaVersion.v2_0_3)
  @JsonCreator
  public ResponseMsg(@JsonProperty(RESPONSE) @NotNull XyzResponse response) {
    this.response = response;
  }

  /**
   * The response to return.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  @JsonProperty(RESPONSE)
  public final @NotNull XyzResponse response;
}
