package com.here.mapcreator.ext.naksha;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.here.xyz.INaksha;
import com.here.xyz.models.geojson.implementation.Properties;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/** A PostgresQL database configuration as used by the {@link PsqlStorage}. */
@AvailableSince(INaksha.v2_0)
public class PsqlStorageProperties extends Properties {

    @AvailableSince(INaksha.v2_0)
    public static final String CONFIG = "config";

    /**
     * Create new PostgresQL storage configuration properties.
     *
     * @param config the database configuration to use.
     */
    @AvailableSince(INaksha.v2_0)
    @JsonCreator
    public PsqlStorageProperties(@NotNull PsqlConfig config) {
        this.config = config;
    }

    /** The configuration of the PostgresQL database. */
    @AvailableSince(INaksha.v2_0)
    public @NotNull PsqlConfig config;
}
