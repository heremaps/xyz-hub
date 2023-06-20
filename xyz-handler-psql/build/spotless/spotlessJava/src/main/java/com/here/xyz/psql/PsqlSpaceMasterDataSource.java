package com.here.xyz.psql;


import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** The master (writable) data-source of a Naksha space. */
public class PsqlSpaceMasterDataSource extends PsqlHandlerDataSource {

  public PsqlSpaceMasterDataSource(
      @NotNull PsqlHandlerParams params, @NotNull String spaceId, @Nullable String collection) {
    super(params, spaceId, collection, false);
  }
}
