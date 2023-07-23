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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.Typed;
import com.here.naksha.lib.core.util.json.JsonObject;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/**
 * Base class of all Naksha extension protocol messages.
 */
@AvailableSince(NakshaVersion.v2_0_3)
@JsonSubTypes({
  @JsonSubTypes.Type(value = ProcessEventMsg.class),
  @JsonSubTypes.Type(value = ResponseMsg.class),
  @JsonSubTypes.Type(value = SendUpstreamMsg.class)
})
public class ExtensionMessage extends JsonObject implements Typed {
  // Note: We may want in the future to allow Naksha extensions to invoke new tasks on the Naksha-Hub. For this
  // purpose we only
  //       new to create a new message type, the result will anyway still be a ResponseMsg.
}
