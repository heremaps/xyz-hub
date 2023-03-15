package com.here.mapcreator.ext.naksha;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The read replica (read-only) data-source of a Naksha space.
 */
public class NPsqlConnectorSpaceReplica extends NPsqlConnectorSpaceSource {

  public NPsqlConnectorSpaceReplica(@NotNull NPsqlConnectorParams params, @NotNull String applicationName, @NotNull String spaceId) {
    super(params, applicationName, spaceId, true, null, null);
  }

  public NPsqlConnectorSpaceReplica(
      @NotNull NPsqlConnectorParams params,
      @NotNull String applicationName,
      @NotNull String spaceId,
      @Nullable String table,
      @Nullable String historyTable) {
    super(params, applicationName, spaceId, true, table, historyTable);
  }
}
