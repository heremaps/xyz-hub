package com.here.mapcreator.ext.naksha;

import com.here.xyz.models.hub.psql.PsqlProcessorParams;
import com.here.xyz.models.hub.psql.PsqlPoolConfig;
import java.util.List;
import org.apache.commons.lang3.RandomUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The data-source to a Naksha space.
 */
public class PsqlSpaceDataSource extends AbstractPsqlDataSource<PsqlSpaceDataSource> {

  private static @NotNull PsqlPool readOnlyPool(@NotNull PsqlProcessorParams params) {
    final List<@NotNull PsqlPoolConfig> dbReplicas = params.getDbReplicas();
    final int SIZE = dbReplicas.size();
    if (SIZE == 0) {
      final PsqlPoolConfig dbConfig = params.getDbConfig();
      return PsqlPool.get(dbConfig);
    }
    final int replicaIndex = RandomUtils.nextInt(0, SIZE);
    final PsqlPoolConfig dbConfig = dbReplicas.get(replicaIndex);
    return PsqlPool.get(dbConfig);
  }

  /**
   * Create a new data source for the given connection pool and application.
   *
   * @param params          the PostgresQL connector parameters.
   * @param applicationName the application name.
   * @param spaceId         the space identifier.
   * @param readOnly        true if the connection should use a read-replica, if available; false otherwise.
   * @param table           the database table; if null the space-id is used.
   * @param historyTable    the history table; if null table plus "_hst".
   */
  public PsqlSpaceDataSource(
      @NotNull PsqlProcessorParams params,
      @NotNull String applicationName,
      @NotNull String spaceId,
      boolean readOnly,
      @Nullable String table,
      @Nullable String historyTable) {
    super(readOnly ? readOnlyPool(params) : PsqlPool.get(params.getDbConfig()), applicationName);
    this.readOnly = readOnly;
    this.connectorParams = params;
    setSchema(params.getSpaceSchema());
    setRole(params.getSpaceRole());
    this.spaceId = spaceId;
    this.table = table != null ? table : spaceId;
    this.historyTable = historyTable != null ? historyTable : this.table + "_hst";
  }

  /**
   * The connector parameters used to create this data source.
   */
  public final @NotNull PsqlProcessorParams connectorParams;

  /**
   * The space identifier.
   */
  public final @NotNull String spaceId;

  /**
   * The database table.
   */
  public final @NotNull String table;

  /**
   * The name of the history table.
   */
  public final @NotNull String historyTable;

  /**
   * True if this is a read-only source; false otherwise.
   */
  public final boolean readOnly;
}
