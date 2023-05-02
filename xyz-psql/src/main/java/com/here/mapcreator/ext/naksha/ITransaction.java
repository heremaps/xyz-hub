package com.here.mapcreator.ext.naksha;

import com.here.xyz.Extensible;
import com.here.xyz.models.geojson.implementation.AbstractFeature;
import com.here.xyz.models.hub.Space;
import java.io.IOException;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An abstract collection transaction.
 */
public interface ITransaction extends AutoCloseable {

  /**
   * A small wrapper for the delete operation.
   */
  class DeleteOp {

    public DeleteOp(@NotNull String id) {
      this.id = id;
    }

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

  /**
   * Returns the transaction identifier of this transaction.
   */
  @NotNull String getTransactionId();

  /**
   * Returns a list of features with the given identifier.
   *
   * @param space        The space from which to read the features.
   * @param featureClass The class of the feature to read.
   * @param ids          The identifiers of the features to read.
   * @param <P>          The properties type.
   * @param <F>          The feature type.
   * @return the list of read features, the order is insignificant.
   * @throws IOException If any error occurred.
   */
  <P extends Extensible<P>, F extends AbstractFeature<P, F>> @NotNull List<F> getFeaturesById(
      @NotNull Space space, @NotNull Class<F> featureClass, @NotNull List<@NotNull String> ids) throws IOException;

  /**
   * Perform the given operations as bulk operation and return the results.
   *
   * @param insert The features to insert; if any.
   * @param update The features to update; if any.
   * @param delete The features to delete; if any.
   * @return The inserted, update and deleted features in the same order as given.
   * @throws IOException If any error occurred.
   */
  @NotNull List<List<@Nullable AbstractFeature<?, ?>>> modifyFeatures(
      @Nullable List<@NotNull AbstractFeature<?, ?>> insert,
      @Nullable List<@NotNull AbstractFeature<?, ?>> update,
      @Nullable List<@NotNull DeleteOp> delete
  ) throws IOException;

  /**
   * Abort the transaction.
   */
  void rollback();

  /**
   * Commit all changes.
   */
  void commit();

  /**
   * Close the transaction, which effectively will roll back the transaction.
   */
  @Override
  default void close() {
    rollback();
  }
}
