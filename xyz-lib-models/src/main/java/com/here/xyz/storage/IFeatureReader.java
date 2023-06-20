package com.here.xyz.storage;

import com.here.xyz.models.geojson.implementation.Feature;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * API to read features from a collection.
 */
public interface IFeatureReader<FEATURE extends Feature> {

  /**
   * Returns a list of features with the given identifier from the HEAD collection.
   *
   * @param ids the identifiers of the features to read.
   * @return the list of read features, the order is insignificant. Features that where not found, are simply not part of the result-set.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @NotNull List<@NotNull FEATURE> getFeaturesById(@NotNull List<@NotNull String> ids) throws Exception;
}