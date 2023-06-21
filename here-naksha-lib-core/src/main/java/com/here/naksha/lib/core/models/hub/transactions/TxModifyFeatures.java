package com.here.naksha.lib.core.models.hub.transactions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * A transaction signal, that the features of a collection have been modified. The individual change
 * done to a features is not part of the signal, because normally all changes done to the same
 * collection only create one signal in the transaction log.
 */
@AvailableSince(INaksha.v2_0_0)
@JsonTypeName(value = "TxModifyFeatures")
public class TxModifyFeatures extends TxSignal {

    /**
     * Create a new transaction signal. For this event the “id” must be the same as the “collection”.
     *
     * @param id the local identifier of the event.
     * @param storageId the storage identifier.
     * @param collection the collection impacted.
     * @param txn the transaction number.
     */
    @AvailableSince(INaksha.v2_0_0)
    @JsonCreator
    public TxModifyFeatures(
            @JsonProperty(ID) @NotNull String id,
            @JsonProperty(STORAGE_ID) @NotNull String storageId,
            @JsonProperty(COLLECTION) @NotNull String collection,
            @JsonProperty(XyzNamespace.TXN) @NotNull String txn) {
        super(id, storageId, collection, txn);
        assert id.equals(collection);
    }
}
