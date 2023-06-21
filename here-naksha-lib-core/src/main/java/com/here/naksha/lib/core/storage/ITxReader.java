package com.here.naksha.lib.core.storage;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import com.here.naksha.lib.core.models.hub.StorageCollection;
import java.util.List;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Interface to grant read-access to a storage. */
@AvailableSince(INaksha.v2_0_0)
public interface ITxReader extends AutoCloseable {
    /**
     * Returns the current transaction number, if none has been created yet, creating a new one.
     *
     * @throws Exception if access to the storage failed or any other error occurred.
     */
    @AvailableSince(INaksha.v2_0_0)
    @NotNull
    String getTransactionNumber() throws Exception;

    @AvailableSince(INaksha.v2_0_0)
    @Override
    void close();

    /**
     * Returns all collections from the storage.
     *
     * @return all collections from the storage.
     * @throws Exception if access to the storage failed or any other error occurred.
     */
    @AvailableSince(INaksha.v2_0_0)
    @NotNull
    List<@NotNull StorageCollection> getAllCollections() throws Exception;

    /**
     * Returns the collection with the given id.
     *
     * @param id the identifier of the collection to return.
     * @return the collection or {@code null}, if no such collection exists.
     * @throws Exception if access to the storage failed or any other error occurred.
     */
    @AvailableSince(INaksha.v2_0_0)
    @Nullable
    StorageCollection getCollectionById(@NotNull String id) throws Exception;

    /**
     * Returns the reader for the given feature-type and collection.
     *
     * @param featureClass the class of the feature-type to read.
     * @param collection the collection to read.
     * @param <F> the feature-type.
     * @return the feature reader.
     * @throws Exception if access to the storage failed or any other error occurred.
     */
    <F extends Feature> @NotNull IFeatureReader<F> readFeatures(
            @NotNull Class<F> featureClass, @NotNull StorageCollection collection) throws Exception;
}
