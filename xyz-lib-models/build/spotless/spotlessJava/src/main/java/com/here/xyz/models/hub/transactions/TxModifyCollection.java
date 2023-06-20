package com.here.xyz.models.hub.transactions;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.INaksha;
import com.here.xyz.models.geojson.implementation.Action;
import com.here.xyz.models.geojson.implementation.namespaces.XyzNamespace;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/** A signal, that a collection itself was modified. */
@AvailableSince(INaksha.v2_0)
@JsonTypeName(value = "TxModifyCollection")
public class TxModifyCollection extends TxSignal {

  @AvailableSince(INaksha.v2_0)
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
  @AvailableSince(INaksha.v2_0)
  @JsonCreator
  public TxModifyCollection(
      @JsonProperty(ID) @NotNull String id,
      @JsonProperty(STORAGE_ID) @NotNull String storageId,
      @JsonProperty(COLLECTION) @NotNull String collection,
      @JsonProperty(XyzNamespace.TXN) @NotNull String txn,
      @JsonProperty(ACTION) @NotNull Action action) {
    super(id, storageId, collection, txn);
    assert !id.equals(collection) && id.startsWith("col:");
    this.action = action;
  }

  /** The action done to the collection. */
  @JsonProperty(ACTION)
  public @NotNull Action action;
}
