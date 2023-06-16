package com.here.xyz.storage;

import com.here.xyz.models.hub.Storage;
import com.here.xyz.models.hub.StorageCollection;
import java.io.Closeable;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

/**
 * Storage API to gain access to storages.
 */
public interface IStorageEngine extends Closeable {

  /**
   * Perform maintenance tasks, for example garbage collect features that are older than the set {@link StorageCollection#maxAge}. This task
   * is at least called ones every 12 hours. It is guaranteed that this is only executed on one Naksha instances at a given time, so there
   * is no concurrent execution.
   *
   * @param storage the storage object that represents this storage.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  void maintain(@NotNull Storage storage) throws Exception;

  /**
   * Opens a storage reader.
   *
   * @return the reader.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @NotNull IStorageReader startRead() throws Exception;

  /**
   * Opens a storage mutator.
   *
   * @return the mutator.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @NotNull IStorageWriter startWrite() throws Exception;

  /**
   * Closes the storage, may block for cleanup work.
   *
   * @throws IOException if access to the storage failed or any other error occurred.
   */
  void close() throws IOException;
}