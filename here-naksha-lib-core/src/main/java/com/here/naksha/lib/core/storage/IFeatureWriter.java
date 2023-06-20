package com.here.naksha.lib.core.storage;


import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import org.jetbrains.annotations.NotNull;

/**
 * Interface to grant write-access to features in a collection.
 *
 * @param <FEATURE> the feature-type to modify.
 */
public interface IFeatureWriter<FEATURE extends Feature> extends IFeatureReader<FEATURE> {

    /**
     * Perform the given operations as bulk operation and return the results.
     *
     * @param req the modification request.
     * @return the modification result with the features that have been inserted, update and deleted.
     * @throws Exception if access to the storage failed or any other error occurred.
     */
    @NotNull ModifyFeaturesResp<FEATURE> modifyFeatures(@NotNull ModifyFeaturesReq<FEATURE> req) throws Exception;
}
