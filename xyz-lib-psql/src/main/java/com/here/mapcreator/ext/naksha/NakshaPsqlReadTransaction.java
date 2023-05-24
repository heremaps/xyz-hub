package com.here.mapcreator.ext.naksha;

import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.hub.Space;
import java.sql.SQLException;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A Naksha PostgresQL transaction that can be used to read data, optionally using a read-replica, if opened as read-only transaction.
 *
 * @param <DATASOURCE> The data-source to read from.
 */
public class NakshaPsqlReadTransaction<DATASOURCE extends AbstractPsqlDataSource<DATASOURCE>> implements AutoCloseable {

  /**
   * Creates a new transaction for the given PostgresQL client.
   *
   * @param nakshaPsqlClient The PostgresQL client for which to create a new transaction.
   */
  NakshaPsqlReadTransaction(@NotNull NakshaPsqlClient<DATASOURCE> nakshaPsqlClient) {
    this.nakshaPsqlClient = nakshaPsqlClient;
  }

  /**
   * The PostgresQL client to which this transaction is bound.
   */
  protected final @NotNull NakshaPsqlClient<DATASOURCE> nakshaPsqlClient;

  /**
   * Returns the transaction identifier of this transaction.
   *
   * @throws SQLException If any error occurred.
   */
  public @NotNull String getTransactionId() throws SQLException {
    throw new SQLException("Not Implemented");
  }

  /**
   * Returns all collections of the underlying storage.
   *
   * @return All collections of the underlying storage.
   * @throws SQLException If any error occurred.
   */
  public @NotNull List<@NotNull NakshaCollection> getAllCollections() throws SQLException {
    throw new SQLException("Not implemented");
  }

  /**
   * Returns the collection with the given identifier.
   *
   * @param id The collection identifier.
   * @return The collection with the given identifier; {@code null} if no collection with the given identifier exists.
   * @throws SQLException If any error occurred.
   */
  public @Nullable NakshaCollection getCollection(@NotNull String id) throws SQLException {
    throw new SQLException("Not implemented");
  }

  // TODO: Read the transaction table and garbage collect the transaction table (it should be the same as any other history).
  //       We need a way to read a single transaction and to iterate all transactions.

  /**
   * Returns a list of features with the given identifier.
   *
   * @param space        The space from which to read the features.
   * @param featureClass The class of the feature to read.
   * @param ids          The identifiers of the features to read.
   * @param <F>          The feature-type to return.
   * @return the list of read features, the order is insignificant.
   * @throws SQLException If any error occurred.
   */
  public <F extends Feature> @NotNull List<F> getFeaturesById(
      @NotNull Space space,
      @NotNull Class<F> featureClass,
      @NotNull List<@NotNull String> ids
  ) throws SQLException {
    throw new SQLException("Not Implemented");
  }

  @Override
  public void close() {
  }
}
