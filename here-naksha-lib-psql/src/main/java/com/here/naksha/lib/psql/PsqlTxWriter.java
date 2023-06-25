package com.here.naksha.lib.psql;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import com.here.naksha.lib.core.models.hub.StorageCollection;
import com.here.naksha.lib.core.storage.IMasterTransaction;
import java.sql.SQLException;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/** A Naksha PostgresQL database transaction that can be used to read and mutate data. */
public class PsqlTxWriter extends PsqlTxReader implements IMasterTransaction {

  /**
   * Creates a new transaction for the given PostgresQL client.
   *
   * @param psqlClient the PostgresQL client for which to create a new transaction.
   */
  PsqlTxWriter(@NotNull PsqlStorage psqlClient) {
    super(psqlClient);
  }

  @Override
  public @NotNull StorageCollection createCollection(@NotNull StorageCollection collection) throws SQLException {
    throw new UnsupportedOperationException("createCollection");
  }

  @Override
  public @NotNull StorageCollection updateCollection(@NotNull StorageCollection collection) throws SQLException {
    throw new UnsupportedOperationException("updateCollection");
  }

  @AvailableSince(INaksha.v2_0_0)
  public @NotNull StorageCollection upsertCollection(@NotNull StorageCollection collection) throws SQLException {
    throw new UnsupportedOperationException("updateCollection");
  }

  @Override
  public @NotNull StorageCollection dropCollection(@NotNull StorageCollection collection, long deleteAt)
      throws SQLException {
    throw new UnsupportedOperationException("dropCollection");
  }

  @Override
  public @NotNull StorageCollection enableHistory(@NotNull StorageCollection collection) throws SQLException {
    throw new UnsupportedOperationException("enableHistory");
  }

  @Override
  public @NotNull StorageCollection disableHistory(@NotNull StorageCollection collection) throws SQLException {
    throw new UnsupportedOperationException("disableHistory");
  }

  @Override
  public @NotNull <F extends Feature> PsqlFeatureWriter<F> writeFeatures(
      @NotNull Class<F> featureClass, @NotNull StorageCollection collection) {
    // TODO: Optimize by tracking the write, no need to create a new instance for every call!
    return new PsqlFeatureWriter<>(this, featureClass, collection);
  }

  /**
   * Commit all changes.
   *
   * @throws SQLException If any error occurred.
   */
  public void commit() throws SQLException {
    throw new UnsupportedOperationException("commit");
  }

  /** Abort the transaction. */
  public void rollback() {}

  /** Close the transaction, which effectively will roll back the transaction. */
  @Override
  public void close() {
    rollback();
  }
}
