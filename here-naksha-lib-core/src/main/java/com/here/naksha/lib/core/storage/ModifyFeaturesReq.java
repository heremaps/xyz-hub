package com.here.naksha.lib.core.storage;

import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public record ModifyFeaturesReq<FEATURE extends Feature>(
        @NotNull List<@NotNull FEATURE> insert,
        @NotNull List<@NotNull FEATURE> update,
        @NotNull List<@NotNull FEATURE> upsert,
        @NotNull List<@NotNull FEATURE> delete) {

    /**
     * Delete the entity with the given identifier in the given state. The delete will fail, if the
     * entity is not in the desired state.
     *
     * @param id the identifier of the feature to delete.
     * @param uuid the UUID of the state to delete, {@code null}, if any state is acceptable.
     */
    public record Delete(@NotNull String id, @Nullable String uuid) {

        public Delete(@NotNull String id) {
            this(id, null);
        }
    }
}
