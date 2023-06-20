package com.here.xyz.storage;

import com.here.xyz.models.geojson.implementation.Feature;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public record ModifyFeaturesResp<FEATURE extends Feature>(
    @NotNull List<@Nullable FEATURE> inserted,
    @NotNull List<@Nullable FEATURE> updated,
    @NotNull List<@Nullable FEATURE> deleted
) {
}