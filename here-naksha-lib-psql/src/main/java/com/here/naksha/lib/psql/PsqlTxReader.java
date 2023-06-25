package com.here.naksha.lib.psql;

import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import com.here.naksha.lib.core.models.features.StorageCollection;
import com.here.naksha.lib.core.storage.IReadTransaction;
import java.sql.SQLException;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A Naksha PostgresQL transaction that can be used to read data, optionally using a read-replica,
 * if opened as read-only transaction.
 */
public class PsqlTxReader implements IReadTransaction {

  /**
   * Creates a new transaction for the given PostgresQL client.
   *
   * @param psqlClient the PostgresQL client for which to create a new transaction.
   */
  PsqlTxReader(@NotNull PsqlStorage psqlClient) {
    this.psqlClient = psqlClient;
  }

  /** The PostgresQL client to which this transaction is bound. */
  protected final @NotNull PsqlStorage psqlClient;

  /**
   * Returns the client to which the transaction is bound.
   *
   * @return the client to which the transaction is bound.
   */
  public @NotNull PsqlStorage getPsqlClient() {
    return psqlClient;
  }

  @Override
  public @NotNull String getTransactionNumber() throws SQLException {
    throw new UnsupportedOperationException("getTransactionNumber");
  }

  @Override
  public @NotNull List<@NotNull StorageCollection> iterateCollections() throws SQLException {
    throw new UnsupportedOperationException("getAllCollections");
  }

  @Override
  public @Nullable StorageCollection getCollectionById(@NotNull String id) throws SQLException {
    throw new UnsupportedOperationException("getCollectionById");
  }

  @Override
  public @NotNull <F extends Feature> PsqlFeatureReader<F> readFeatures(
      @NotNull Class<F> featureClass, @NotNull StorageCollection collection) {
    // TODO: Optimize by tracking the read, no need to create a new instance for every call!
    return new PsqlFeatureReader<>(this, featureClass, collection);
  }

  @Override
  public void close() {}
}
