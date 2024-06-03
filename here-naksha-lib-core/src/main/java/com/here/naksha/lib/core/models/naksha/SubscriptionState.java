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
package com.here.naksha.lib.core.models.naksha;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.NakshaVersion;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The subscription state. Used to track the last state of a subscription pipeline to resume operation from.
 */
@SuppressWarnings("unused")
@JsonTypeName(value = "SubscriptionState")
@AvailableSince(NakshaVersion.v2_0_16)
public final class SubscriptionState extends NakshaFeature {

  /** Indicates the last seqNumber, upto (and including) which the respective subscription pipeline has completed the transaction processing. */
  @AvailableSince(NakshaVersion.v2_0_16)
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  private @Nullable Long seqNumber;

  /**
   * Create a new subscription state.
   *
   * @param id the identifier.
   */
  @JsonCreator
  @AvailableSince(NakshaVersion.v2_0_16)
  public SubscriptionState(@JsonProperty(ID) @NotNull String id) {
    super(id);
  }

  @AvailableSince(NakshaVersion.v2_0_16)
  public @Nullable Long getSeqNumber() {
    return seqNumber;
  }

  @AvailableSince(NakshaVersion.v2_0_16)
  public void setSeqNumber(final @Nullable Long seqNumber) {
    this.seqNumber = seqNumber;
  }
}
