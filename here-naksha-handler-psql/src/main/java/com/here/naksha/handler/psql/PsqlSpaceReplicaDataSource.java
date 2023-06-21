package com.here.naksha.handler.psql;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** The read replica (read-only) data-source of a Naksha space. */
public class PsqlSpaceReplicaDataSource extends PsqlHandlerDataSource {

    public PsqlSpaceReplicaDataSource(
            @NotNull PsqlHandlerParams params, @NotNull String spaceId, @Nullable String collection) {
        super(params, spaceId, collection, true);
    }
}
