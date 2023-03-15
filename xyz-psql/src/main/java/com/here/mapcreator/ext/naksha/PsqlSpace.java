package com.here.mapcreator.ext.naksha;

import org.jetbrains.annotations.NotNull;

public class PsqlSpace {
  public PsqlSpace(@NotNull String spaceId, @NotNull String schema, @NotNull String table) {
    this(spaceId, schema, table, table + "_hst");
  }

  public PsqlSpace(
      @NotNull String spaceId,
      @NotNull String schema,
      @NotNull String table,
      @NotNull String historyTable) {
    this.spaceId = spaceId;
    this.schema = schema;
    this.table = table;
    this.historyTable = historyTable;
  }

  public final @NotNull String spaceId;
  public final @NotNull String schema;
  public final @NotNull String table;
  public final @NotNull String historyTable;
}
