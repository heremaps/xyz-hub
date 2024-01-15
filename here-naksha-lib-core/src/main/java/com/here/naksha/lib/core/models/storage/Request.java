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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.Typed;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * All requests send to a storage should extend this base class.
 *
 * @param <SELF> the self-type.
 */
@JsonTypeInfo(use = Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = ReadRequest.class),
  @JsonSubTypes.Type(value = WriteRequest.class),
  @JsonSubTypes.Type(value = Notification.class)
})
@AvailableSince(NakshaVersion.v2_0_7)
public abstract class Request<SELF extends Request<SELF>> implements Typed {

  /**
   * Helper to return this.
   *
   * @return returns this.
   */
  @SuppressWarnings("unchecked")
  protected final @NotNull SELF self() {
    return (SELF) this;
  }

  protected Request<SELF> shallowClone() {
    throw new NotImplementedException("Override shallowClone in your use case.");
  }
}
