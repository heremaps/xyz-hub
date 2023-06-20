package com.here.mapcreator.ext.naksha;

import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.hub.StorageCollection;
import com.here.xyz.storage.IFeatureReader;
import java.sql.SQLException;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class PsqlFeatureReader<FEATURE extends Feature> implements IFeatureReader<FEATURE> {

  PsqlFeatureReader(@NotNull PsqlTxReader storageReader, @NotNull Class<FEATURE> featureClass, @NotNull StorageCollection collection) {
    this.storageReader = storageReader;
    this.featureClass = featureClass;
    this.collection = collection;
  }

  final @NotNull PsqlTxReader storageReader;
  final @NotNull Class<FEATURE> featureClass;
  final @NotNull StorageCollection collection;

  @Override
  public @NotNull List<@NotNull FEATURE> getFeaturesById(@NotNull List<@NotNull String> ids) throws SQLException {
    throw new UnsupportedOperationException("getFeaturesById");
  }

}
