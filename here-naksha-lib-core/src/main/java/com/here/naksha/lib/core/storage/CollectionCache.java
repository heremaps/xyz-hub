package com.here.naksha.lib.core.storage;

import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import com.here.naksha.lib.core.models.features.StorageCollection;
import org.jetbrains.annotations.NotNull;

/**
 * An in-memory cache for a storage collection. It can be configured to only keep parts of the
 * underlying storage collection in memory using weak references or to keep all features of the
 * storage in memory.
 */
public abstract class CollectionCache<FEATURE extends Feature> {
  // TODO: Implement me!

  /**
   * Returns the collection this caches operates on.
   *
   * @return the collection this caches operates on.
   */
  public abstract @NotNull StorageCollection collection();

  /**
   * Returns the feature reader for this collection.
   *
   * @return the feature reader for this collection.
   */
  public abstract @NotNull IFeatureReader<FEATURE> featureReader();
}
