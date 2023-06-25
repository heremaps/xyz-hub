package com.here.naksha.lib.core.storage;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import com.here.naksha.lib.core.models.features.StorageCollection;
import java.util.Iterator;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** The read API of a transaction. */
@AvailableSince(INaksha.v2_0_0)
public interface IReadTransaction extends AutoCloseable {
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
   * Iterate all collections from the storage.
   *
   * @return the iterator above all storage collections.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @AvailableSince(INaksha.v2_0_0)
  @NotNull
  Iterator<@NotNull StorageCollection> iterateCollections() throws Exception;

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
   * @param <FEATURE> the feature-type.
   * @return the feature reader.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  <FEATURE extends Feature> @NotNull IFeatureReader<FEATURE> readFeatures(
      @NotNull Class<FEATURE> featureClass, @NotNull StorageCollection collection) throws Exception;
}
