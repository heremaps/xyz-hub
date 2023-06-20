package com.here.xyz.storage;


import com.here.xyz.lambdas.Pe1;
import com.here.xyz.models.hub.StorageCollection;
import com.here.xyz.models.hub.transactions.TxSignalSet;
import java.io.Closeable;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

/** Storage API to gain access to storages. */
public interface IStorage extends Closeable {
  // TODO: - Add transaction log access.
  //       - Add history access.

  /**
   * Perform maintenance tasks, for example garbage collect features that are older than the set
   * {@link StorageCollection#maxAge}. This task is at least called ones every 12 hours. It is
   * guaranteed that this is only executed on one Naksha instances at a given time, so there is no
   * concurrent execution.
   *
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  void maintain() throws Exception;

  /**
   * Opens a storage reader.
   *
   * @return the reader.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @NotNull ITxReader startRead() throws Exception;

  /**
   * Opens a storage mutator.
   *
   * @return the mutator.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @NotNull ITxWriter startWrite() throws Exception;

  /**
   * Add a listener to be called, when something changes in the storage.
   *
   * @param listener the change listener to invoke, receiving the transaction set. If the listener
   *     throws an exception, it should be called again after some time.
   */
  void addListener(@NotNull Pe1<@NotNull TxSignalSet> listener);

  /**
   * Remove the given lisener.
   *
   * @param listener the change listener to remove.
   * @return {@code true} if the listener was removed; {@code false} otherwise.
   */
  boolean removeListener(@NotNull Pe1<@NotNull TxSignalSet> listener);

  /**
   * Closes the storage, may block for cleanup work.
   *
   * @throws IOException if access to the storage failed or any other error occurred.
   */
  void close() throws IOException;
}
