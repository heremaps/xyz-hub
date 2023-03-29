package com.here.mapcreator.ext.naksha;

import com.here.xyz.models.hub.psql.PsqlStorageParams;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The read replica (read-only) data-source of a Naksha space.
 */
public class PsqlSpaceReplicaDataSource extends PsqlSpaceDataSource {

  public PsqlSpaceReplicaDataSource(@NotNull PsqlStorageParams params, @NotNull String applicationName, @NotNull String spaceId) {
    super(params, applicationName, spaceId, true, null, null);
  }

  public PsqlSpaceReplicaDataSource(
      @NotNull PsqlStorageParams params,
      @NotNull String applicationName,
      @NotNull String spaceId,
      @Nullable String table,
      @Nullable String historyTable) {
    super(params, applicationName, spaceId, true, table, historyTable);
  }
}
