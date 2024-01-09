package com.here.naksha.lib.core.exceptions;

import com.here.naksha.lib.core.models.XyzError;
import org.jetbrains.annotations.NotNull;

public class StorageNotFoundException extends StorageException {

  public StorageNotFoundException(@NotNull String storageId) {
    super(XyzError.NOT_FOUND, "Could not find storage with id: " + storageId);
  }
}
