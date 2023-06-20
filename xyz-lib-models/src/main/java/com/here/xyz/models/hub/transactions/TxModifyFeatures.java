package com.here.xyz.models.hub.transactions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.INaksha;
import com.here.xyz.models.geojson.implementation.namespaces.XyzNamespace;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * A transaction log event to signal that the features of a collection have been modified. For the features the action is not part of the
 * event, because normally all changes will only create a single entry.
 */
@AvailableSince(INaksha.v2_0)
@JsonTypeName(value = "TxModifyFeatures")
public class TxModifyFeatures extends TxEvent {

  /**
   * Create a new transaction event. For this event the “id” must be the same as the “collection”.
   *
   * @param id         the local identifier of the event.
   * @param storageId  the storage identifier.
   * @param collection the collection impacted.
   * @param txn        the transaction number.
   */
  @AvailableSince(INaksha.v2_0)
  @JsonCreator
  public TxModifyFeatures(
      @JsonProperty(ID) @NotNull String id,
      @JsonProperty(STORAGE_ID) @NotNull String storageId,
      @JsonProperty(COLLECTION) @NotNull String collection,
      @JsonProperty(XyzNamespace.TXN) @NotNull String txn
  ) {
    super(id, storageId, collection, txn);
    assert id.equals(collection);
  }
}