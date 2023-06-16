package com.here.xyz.storage;

import com.here.xyz.models.hub.Storage;
import org.jetbrains.annotations.NotNull;

/**
 * An API implemented by storage engines.
 */
public interface IStorage {

  /**
   * Initialize the storage engine, invoked from the Naksha-Hub when creating a new instance of the storage. This should ensure that the
   * storage is accessible and in a good state. If the method fails, it is invoked again after a couple of minutes. This method is invoked
   * at least ones for every service start and therefore must be concurrency safe, because it may be called in parallel by multiple
   * Naksha-Hub instances.
   *
   * @param storage the storage object that represents this storage.
   * @return the initialized storage instance.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @NotNull IStorageEngine init(@NotNull Storage storage) throws Exception;
}