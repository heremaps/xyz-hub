package com.here.xyz.psql;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The read replica (read-only) data-source of a Naksha space.
 */
public class PsqlSpaceReplicaDataSource extends PsqlStorageDataSource {

  public PsqlSpaceReplicaDataSource(@NotNull PsqlStorageParams params, @NotNull String spaceId, @Nullable String collection) {
    super(params, spaceId, collection, true);
  }
}
