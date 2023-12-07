package com.here.naksha.lib.core.exceptions;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.XyzError;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/**
 * An exception thrown when the storage is not initialized.
 */
@AvailableSince(NakshaVersion.v2_0_8)
public class StorageNotInitialized extends StorageException {

  /**
   * Exception when the storage is not initialized.
   */
  public StorageNotInitialized() {
    super(XyzError.STORAGE_NOT_INITIALIZED);
  }
}
