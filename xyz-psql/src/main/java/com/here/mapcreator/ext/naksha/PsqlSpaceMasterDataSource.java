package com.here.mapcreator.ext.naksha;

import com.here.xyz.models.hub.psql.PsqlProcessorParams;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The master (writable) data-source of a Naksha space.
 */
public class PsqlSpaceMasterDataSource extends PsqlSpaceDataSource {

  public PsqlSpaceMasterDataSource(@NotNull PsqlProcessorParams params, @NotNull String applicationName, @NotNull String spaceId) {
    super(params, applicationName, spaceId, false, null, null);
  }

  public PsqlSpaceMasterDataSource(
      @NotNull PsqlProcessorParams params,
      @NotNull String applicationName,
      @NotNull String spaceId,
      @Nullable String table,
      @Nullable String historyTable) {
    super(params, applicationName, spaceId, false, table, historyTable);
  }
}
