package com.here.xyz.storage;

import com.here.xyz.models.hub.StorageCollection;
import java.sql.SQLException;
import org.jetbrains.annotations.NotNull;

public interface IStorageWriter extends IStorageReader {

  /**
   * Create a new collection.
   *
   * @param collection the collection to create.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  void createCollection(@NotNull StorageCollection collection) throws Exception;

  /**
   * Deletes the collection including the history.
   *
   * @param collection The collection to delete.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  void dropCollection(@NotNull StorageCollection collection) throws Exception;

  /**
   * Update the collection.
   *
   * @param collection The collection to update.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  void updateCollection(@NotNull StorageCollection collection) throws Exception;

  /**
   * Deletes the collection including the history.
   *
   * @param collection The collection to delete.
   * @param deleteAt   the unix epoch timestamp in milliseconds when to delete the table, must be greater than zero.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  void dropCollectionAt(@NotNull StorageCollection collection, long deleteAt) throws Exception;

  /**
   * Enable the history for the given collection.
   *
   * @param collection the collection on which to enable the history.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  void enableHistory(@NotNull StorageCollection collection) throws Exception;

  /**
   * Disable the history for the given collection.
   *
   * @param collection The collection on which to disable the history.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  void disableHistory(@NotNull StorageCollection collection) throws Exception;

  /**
   * Perform the given operations as bulk operation and return the results.
   *
   * @param req the modification request.
   * @return the modification result with the features that have been inserted, update and deleted.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @NotNull StorageModifyFeaturesResp modifyFeatures(@NotNull StorageModifyFeaturesReq req) throws Exception;

  /**
   * Commit all changes.
   *
   * @throws SQLException If any error occurred.
   */
  void commit() throws Exception;

  /**
   * Abort the transaction.
   */
  void rollback();

  /**
   * Rollback everything that is still pending and close the writer.
   */
  @Override
  void close();
}