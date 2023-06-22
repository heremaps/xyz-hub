package com.here.naksha.lib.core.storage;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import com.here.naksha.lib.core.models.hub.StorageCollection;
import java.sql.SQLException;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/** Interface to grant write-access to a storage. */
public interface ITxWriter extends ITxReader {

  /**
   * Commit all changes.
   *
   * @throws SQLException If any error occurred.
   */
  @AvailableSince(INaksha.v2_0_0)
  void commit() throws Exception;

  /** Abort the transaction, revert all pending changes. */
  @AvailableSince(INaksha.v2_0_0)
  void rollback();

  /** Rollback everything that is still pending and close the writer. */
  @AvailableSince(INaksha.v2_0_0)
  @Override
  void close();

  /**
   * Create a new collection, fails if the collection exists already.
   *
   * @param collection the collection to create.
   * @return the created collection.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @AvailableSince(INaksha.v2_0_0)
  @NotNull
  StorageCollection createCollection(@NotNull StorageCollection collection) throws Exception;

  /**
   * Update the collection.
   *
   * @param collection The collection to update.
   * @return the updated collection.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @AvailableSince(INaksha.v2_0_0)
  @NotNull
  StorageCollection updateCollection(@NotNull StorageCollection collection) throws Exception;

  /**
   * Update or insert the collection.
   *
   * @param collection The collection to update or insert.
   * @return the updated or inserted collection.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @AvailableSince(INaksha.v2_0_0)
  @NotNull
  StorageCollection upsertCollection(@NotNull StorageCollection collection) throws Exception;

  /**
   * Deletes the collection including the history.
   *
   * @param collection The collection to delete.
   * @param deleteAt the unix epoch timestamp in milliseconds when to delete the table, must be
   *     greater than zero.
   * @return the dropped collection.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @AvailableSince(INaksha.v2_0_0)
  @NotNull
  StorageCollection dropCollection(@NotNull StorageCollection collection, long deleteAt) throws Exception;

  /**
   * Enable the history for the given collection.
   *
   * @param collection the collection on which to enable the history.
   * @return the modified collection.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @AvailableSince(INaksha.v2_0_0)
  @NotNull
  StorageCollection enableHistory(@NotNull StorageCollection collection) throws Exception;

  /**
   * Disable the history for the given collection.
   *
   * @param collection The collection on which to disable the history.
   * @return the modified collection.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @AvailableSince(INaksha.v2_0_0)
  @NotNull
  StorageCollection disableHistory(@NotNull StorageCollection collection) throws Exception;

  /**
   * Returns the writer for the given feature-type and collection.
   *
   * @param featureClass the class of the feature-type to read.
   * @param collection the collection to read.
   * @param <F> the feature-type.
   * @return the feature writer.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  <F extends Feature> @NotNull IFeatureWriter<F> writeFeatures(
      @NotNull Class<F> featureClass, @NotNull StorageCollection collection) throws Exception;
}
