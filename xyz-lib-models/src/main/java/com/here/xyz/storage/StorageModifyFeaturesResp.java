package com.here.xyz.storage;

import com.here.xyz.models.geojson.implementation.Feature;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public record StorageModifyFeaturesResp(
    @NotNull List<@Nullable Feature> inserted,
    @NotNull List<@Nullable Feature> updated,
    @NotNull List<@Nullable Feature> deleted
) {
}