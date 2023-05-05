package com.here.xyz.psql;

import com.here.mapcreator.ext.naksha.AbstractPsqlDataSource;
import com.here.mapcreator.ext.naksha.PsqlPool;
import com.here.mapcreator.ext.naksha.PsqlPoolConfig;
import java.util.List;
import org.apache.commons.lang3.RandomUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The data-source used by the {@link PsqlStorage}.
 */
public class PsqlStorageDataSource extends AbstractPsqlDataSource<PsqlStorageDataSource> {

  private static @NotNull PsqlPool readOnlyPool(@NotNull PsqlStorageParams params) {
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

  @Override
  protected @NotNull String defaultSchema() {
    return "postgres";
  }

  /**
   * Create a new data source for the given connection pool and application.
   *
   * @param params     the PostgresQL connector parameters.
   * @param spaceId    the space identifier.
   * @param readOnly   true if the connection should use a read-replica, if available; false otherwise.
   * @param collection the collection to use; if {@code null}, the space-id is used.
   */
  public PsqlStorageDataSource(
      @NotNull PsqlStorageParams params,
      @NotNull String spaceId,
      @Nullable String collection,
      boolean readOnly) {
    super(readOnly ? readOnlyPool(params) : PsqlPool.get(params.getDbConfig()), spaceId);
    this.readOnly = readOnly;
    this.connectorParams = params;
    setSchema(params.getSchema());
    setRole(params.getRole());
    this.spaceId = spaceId;
    this.table = collection != null ? collection : spaceId;
    this.historyTable = collection + "_hst";
    this.deletionTable = collection + "_del";
  }

  /**
   * The connector parameters used to create this data source.
   */
  public final @NotNull PsqlStorageParams connectorParams;

  /**
   * The space identifier.
   */
  public final @NotNull String spaceId;

  /**
   * The database table, called “collection” in the event.
   */
  public final @NotNull String table;

  /**
   * The name of the history table.
   */
  public final @NotNull String historyTable;

  /**
   * The name of the deletion table.
   */
  public final @NotNull String deletionTable;

  /**
   * True if this is a read-only source; false otherwise.
   */
  public final boolean readOnly;
}
