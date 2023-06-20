package com.here.mapcreator.ext.naksha;


import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** A fully pre-configured PostgresQL data-source. */
public class PsqlDataSource extends AbstractPsqlDataSource<PsqlDataSource> {

    /**
     * Create a new data source for the given connection pool and application.
     *
     * @param pool the PSQL pool.
     * @param appName The application name to set, when connecting to the database.
     */
    public PsqlDataSource(@NotNull PsqlPool pool, @NotNull String appName) {
        super(pool, appName);
        this.config = null;
    }

    /**
     * Create a new data source for the given PostgresQL configuration.
     *
     * @param config the configuration to use.
     */
    public PsqlDataSource(@NotNull PsqlConfig config) {
        super(PsqlPool.get(config), config.appName);
        this.config = config;
        setSchema(config.schema);
        setSearchPath(config.searchPath);
        setRole(config.role);
    }

    /** The PostgresQL configuration. */
    private final @Nullable PsqlConfig config;

    @Override
    public @NotNull PsqlPoolConfig getConfig() {
        return config != null ? config : super.getConfig();
    }

    @Override
    protected @NotNull String defaultSchema() {
        return config != null ? config.schema : "postgres";
    }

    @Override
    protected @NotNull String defaultSearchPath() {
        return config != null && config.searchPath != null ? config.searchPath : super.defaultSearchPath();
    }
}
