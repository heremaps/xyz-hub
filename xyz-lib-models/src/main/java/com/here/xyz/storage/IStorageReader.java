package com.here.xyz.storage;

import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.hub.StorageCollection;
import java.sql.SQLException;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The reader API to be implemented by storages.
 */
public interface IStorageReader extends AutoCloseable {

  /**
   * Returns the transaction identifier of this transaction. This will be a UUID as specified in the documentation.
   *
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @NotNull String transactionId() throws Exception;

  // TODO: Add methods to access the transaction logs.

  /**
   * Returns all collections from the storage.
   *
   * @return all collections from the storage.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @NotNull List<@NotNull StorageCollection> getAllCollections() throws Exception;

  /**
   * Returns the collection with the given id.
   *
   * @param id the identifier of the collection to return.
   * @return the collection or {@code null}, if no such collection exists.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @Nullable StorageCollection getCollectionById(@NotNull String id) throws Exception;

  /**
   * Returns a list of features with the given identifier from the HEAD collection.
   *
   * @param collection   the collection to read from.
   * @param featureClass the class of the feature-type to read.
   * @param ids          the identifiers of the features to read.
   * @param <F>          the feature-type to return.
   * @return the list of read features, the order is insignificant. Features that where not found, are simply not part of the result-set.
   * @throws SQLException If any error occurred.
   */
  <F extends Feature> @NotNull List<@NotNull F> getFeaturesById(
      @NotNull StorageCollection collection,
      @NotNull Class<F> featureClass,
      @NotNull List<@NotNull String> ids
  ) throws Exception;

  @Override
  void close();
}