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
package com.here.naksha.lib.core.models.features;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.EXyzAction;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/** A signal, that a collection itself was modified. */
@AvailableSince(NakshaVersion.v2_0_0)
@JsonTypeName(value = "TxModifyCollection")
public class TxModifyCollection extends TxSignal {

  @AvailableSince(NakshaVersion.v2_0_0)
  public static final String ACTION = "action";

  /**
   * Create a new collection modification signal.
   *
   * @param id the local identifier of the event.
   * @param storageId the storage identifier.
   * @param collection the collection impacted.
   * @param txn the transaction number.
   * @param action the action.
   */
  @AvailableSince(NakshaVersion.v2_0_0)
  @JsonCreator
  public TxModifyCollection(
      @JsonProperty(ID) @NotNull String id,
      @JsonProperty(STORAGE_ID) @NotNull String storageId,
      @JsonProperty(COLLECTION) @NotNull String collection,
      @JsonProperty(XyzNamespace.TXN) @NotNull String txn,
      @JsonProperty(ACTION) @NotNull EXyzAction action) {
    super(id, storageId, collection, txn);
    assert !id.equals(collection) && id.startsWith("col:");
    this.action = action;
  }

  /** The action done to the collection. */
  @JsonProperty(ACTION)
  public @NotNull EXyzAction action;
}
