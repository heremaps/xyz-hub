package com.here.mapcreator.ext.naksha;


import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.hub.StorageCollection;
import com.here.xyz.storage.IFeatureWriter;
import com.here.xyz.storage.ModifyFeaturesReq;
import com.here.xyz.storage.ModifyFeaturesResp;
import java.sql.SQLException;
import org.jetbrains.annotations.NotNull;

public class PsqlFeatureWriter<FEATURE extends Feature> extends PsqlFeatureReader<FEATURE>
        implements IFeatureWriter<FEATURE> {

    PsqlFeatureWriter(
            @NotNull PsqlTxWriter storageWriter,
            @NotNull Class<FEATURE> featureClass,
            @NotNull StorageCollection collection) {
        super(storageWriter, featureClass, collection);
        this.storageWriter = storageWriter;
        assert this.storageReader == this.storageWriter;
    }

    final @NotNull PsqlTxWriter storageWriter;

    @Override
    public @NotNull ModifyFeaturesResp<FEATURE> modifyFeatures(@NotNull ModifyFeaturesReq<FEATURE> req)
            throws SQLException {
        throw new UnsupportedOperationException("modifyFeatures");
    }
}
