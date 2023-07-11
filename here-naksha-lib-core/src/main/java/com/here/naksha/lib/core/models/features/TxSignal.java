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
package com.here.naksha.lib.core.models.features;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A transaction persists out of multiple signals. Each signal represents something that has happened in a {@link Storage storage}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("unused")
@AvailableSince(INaksha.v2_0_0)
@JsonTypeName(value = "TxSignal")
@JsonSubTypes({
  @JsonSubTypes.Type(value = TxMessage.class),
  @JsonSubTypes.Type(value = TxModifyFeatures.class),
  @JsonSubTypes.Type(value = TxModifyCollection.class)
})
public class TxSignal extends XyzFeature {

  @AvailableSince(INaksha.v2_0_0)
  public static final String STORAGE_ID = "storageId";

  @AvailableSince(INaksha.v2_0_0)
  public static final String COLLECTION = "collection";

  @AvailableSince(INaksha.v2_0_0)
  public static final String TS = "ts";

  @AvailableSince(INaksha.v2_0_0)
  public static final String PUBLISH_ID = "publishId";

  @AvailableSince(INaksha.v2_0_0)
  public static final String PUBLISH_TS = "publishTs";

  /**
   * Create a new transaction signal.
   *
   * @param id the local identifier of the event.
   * @param storageId the storage identifier.
   * @param collection the collection impacted.
   * @param txn the transaction number.
   */
  @AvailableSince(INaksha.v2_0_0)
  @JsonCreator
  public TxSignal(
      @JsonProperty(ID) @NotNull String id,
      @JsonProperty(STORAGE_ID) @NotNull String storageId,
      @JsonProperty(COLLECTION) @NotNull String collection,
      @JsonProperty(XyzNamespace.TXN) @NotNull String txn) {
    super(id);
    this.storageId = storageId;
    this.collection = collection;
    this.txn = txn;
  }

  /**
   * The unique transaction number, as stored within the XYZ namespace {@link XyzNamespace#getTxn()
   * txn}. All items of a transaction have the same transaction number.
   */
  @AvailableSince(INaksha.v2_0_0)
  @JsonProperty(XyzNamespace.TXN)
  public @NotNull String txn;

  /** The collection that this transaction item is related to. */
  @JsonProperty(COLLECTION)
  public @NotNull String collection;

  /** The storage-id to which the transaction belongs. */
  @JsonProperty(STORAGE_ID)
  public @NotNull String storageId;

  /**
   * The transaction time stamp as epoch timestamp in milliseconds (when the transaction started).
   */
  @JsonProperty(TS)
  public long ts;

  /** The application that caused the event. */
  @JsonProperty(XyzNamespace.APP_ID)
  public @Nullable String appId;

  /** The author that caused the event. */
  @JsonProperty(XyzNamespace.AUTHOR)
  public @Nullable String author;

  /**
   * The unique sequential publishing identifier of the transaction, set as soon as the transaction
   * becomes visible. This is a sequential number without holes, with the lowest valid number being
   * 1. It is set by the maintenance thread. All items of a transaction will have the same publish
   * identifier.
   */
  @JsonProperty(PUBLISH_ID)
  public @Nullable Long publishId;

  /**
   * The epoch timestamp in milliseconds of when the transaction became publicly visible, set by the
   * maintenance client.
   */
  @JsonProperty(PUBLISH_TS)
  public @Nullable Long publishTs;
}
