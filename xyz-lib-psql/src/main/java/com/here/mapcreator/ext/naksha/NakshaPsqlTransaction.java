package com.here.mapcreator.ext.naksha;

import com.here.xyz.models.geojson.implementation.Feature;
import java.sql.SQLException;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A Naksha PostgresQL database transaction that can be used to read and mutate data.
 *
 * @param <DATASOURCE> The data-source to read from.
 */
public class NakshaPsqlTransaction<DATASOURCE extends AbstractPsqlDataSource<DATASOURCE>> extends NakshaPsqlReadTransaction<DATASOURCE> {

  /**
   * Creates a new transaction for the given PostgresQL client.
   *
   * @param nakshaPsqlClient The PostgresQL client for which to create a new transaction.
   */
  NakshaPsqlTransaction(@NotNull NakshaPsqlClient<DATASOURCE> nakshaPsqlClient) {
    super(nakshaPsqlClient);
  }

  /**
   * A small wrapper for a delete operation.
   */
  public static class DeleteOp {

    /**
     * Delete the entity with the given identifier, no matter what the current state is.
     *
     * @param id The entity identifier.
     */
    public DeleteOp(@NotNull String id) {
      this.id = id;
    }

    /**
     * Delete the entity with the given identifier in the given state. The delete will fail, if the entity is not in the desired state.
     *
     * @param id   The entity identifier.
     * @param uuid The entity state identifier.
     */
    public DeleteOp(@NotNull String id, @NotNull String uuid) {
      this.id = id;
      this.uuid = uuid;
    }

    /**
     * The unique identifier of the feature to delete.
     */
    public final @NotNull String id;

    /**
     * The state to be deleted, if {@code null}, then the feature is deleted unsafe.
     */
    public @Nullable String uuid;
  }

  @Override
  public @NotNull String getTransactionId() throws SQLException {
    throw new SQLException("Not Implemented");
  }

  /**
   * Create a new collection.
   *
   * @param id            The unique identifier of the collection.
   * @param enableHistory If the history of the collection should be enabled.
   * @return The collection information.
   * @throws SQLException If any error occurred.
   */
  public @NotNull NakshaCollection createCollection(@NotNull String id, boolean enableHistory) throws SQLException {
    throw new SQLException("Not implemented");
  }

  /**
   * Deletes the collection including the history.
   *
   * @param collection The collection to delete.
   * @throws SQLException If any error occurred.
   */
  public void dropCollection(@NotNull NakshaCollection collection) throws SQLException {
    throw new SQLException("Not implemented");
  }

  /**
   * Enable the history for the given collection.
   *
   * @param collection The collection on which to enable the history.
   * @param maxAge     The maximum age to store history for given in days.
   * @throws SQLException If any error occurred.
   */
  public void enableHistory(@NotNull NakshaCollection collection, long maxAge) throws SQLException {
    throw new SQLException("Not implemented");
  }

  /**
   * Disable the history for the given collection.
   *
   * @param collection The collection on which to disable the history.
   * @throws SQLException If any error occurred.
   */
  public void disableHistory(@NotNull NakshaCollection collection) throws SQLException {
    throw new SQLException("Not implemented");
  }

  /**
   * Perform the given operations as bulk operation and return the results.
   *
   * @param insert The features to insert; if any.
   * @param update The features to update; if any.
   * @param delete The features to delete; if any.
   * @return The inserted, update and deleted features in the same order as given.
   * @throws SQLException If any error occurred.
   */
  public @NotNull List<List<@Nullable Feature>> modifyFeatures(
      @Nullable List<@NotNull Feature> insert,
      @Nullable List<@NotNull Feature> update,
      @Nullable List<@NotNull DeleteOp> delete
  ) throws SQLException {
    throw new SQLException("Not Implemented");
  }

  /**
   * Commit all changes.
   *
   * @throws SQLException If any error occurred.
   */
  public void commit() throws SQLException {
    throw new SQLException("Not Implemented");
  }

  /**
   * Abort the transaction.
   */
  public void rollback() {
  }

  /**
   * Close the transaction, which effectively will roll back the transaction.
   */
  @Override
  public void close() {
    rollback();
  }
}