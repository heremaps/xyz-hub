package com.here.mapcreator.ext.naksha;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An extension to the {@link PsqlPoolConfig} that adds information, which are not needed for the PostgresQL connection pool, but generally
 * are required by applications.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("unused")
public class PsqlConfig extends PsqlPoolConfig {

  @JsonCreator
  PsqlConfig(
      @JsonProperty @NotNull String host,
      @JsonProperty Integer port,
      @JsonProperty @NotNull String db,
      @JsonProperty @NotNull String user,
      @JsonProperty @NotNull String password,
      @JsonProperty Long connTimeout,
      @JsonProperty Long stmtTimeout,
      @JsonProperty Long lockTimeout,
      @JsonProperty Integer minPoolSize,
      @JsonProperty Integer maxPoolSize,
      @JsonProperty Long idleTimeout,
      @JsonProperty @NotNull String schema,
      @JsonProperty @NotNull String appName,
      @JsonProperty @Nullable String role,
      @JsonProperty @Nullable String searchPath
  ) {
    super(host, port, db, user, password, connTimeout, stmtTimeout, lockTimeout, minPoolSize, maxPoolSize, idleTimeout);
    this.schema = schema;
    this.appName = appName;
    this.role = role;
    this.searchPath = searchPath != null && !searchPath.isEmpty() ? searchPath : null;
  }

  /**
   * Create a new PostgresQL configuration from a pool configuration.
   *
   * @param poolConfig The pool configuration upon which this PostgresQL configuration is based.
   * @param schema     The database schema to use.
   * @param appName    The application name to set, when connecting to the database.
   */
  public PsqlConfig(@NotNull PsqlPoolConfig poolConfig, @NotNull String schema, @NotNull String appName) {
    this(poolConfig, schema, appName, null, null);
  }

  /**
   * Create a new PostgresQL configuration from a pool configuration.
   *
   * @param poolConfig The pool configuration upon which this PostgresQL configuration is based.
   * @param schema     The database schema to use.
   * @param appName    The application name to set, when connecting to the database.
   * @param role       The role to use after connection; if {@code null}, then the {@link #user} is used.
   */
  public PsqlConfig(@NotNull PsqlPoolConfig poolConfig, @NotNull String schema, @NotNull String appName, @Nullable String role) {
    this(poolConfig, schema, appName, role, null);
  }

  /**
   * Create a new PostgresQL configuration from a pool configuration.
   *
   * @param poolConfig The pool configuration upon which this PostgresQL configuration is based.
   * @param schema     The database schema to use.
   * @param appName    The application name to set, when connecting to the database.
   * @param role       The role to use after connection; if {@code null}, then the {@link #user} is used.
   * @param searchPath The search path to set; if {@code null}, automatically set.
   */
  public PsqlConfig(
      @NotNull PsqlPoolConfig poolConfig,
      @NotNull String schema,
      @NotNull String appName,
      @Nullable String role,
      @Nullable String searchPath
  ) {
    super(poolConfig.host, poolConfig.port, poolConfig.db, poolConfig.user, poolConfig.password, poolConfig.connTimeout,
        poolConfig.stmtTimeout, poolConfig.lockTimeout, poolConfig.minPoolSize, poolConfig.maxPoolSize, poolConfig.idleTimeout);
    this.schema = schema;
    this.appName = appName;
    this.role = role;
    this.searchPath = searchPath;
  }

  /**
   * The database schema to use.
   */
  public final @NotNull String schema;

  /**
   * The application name to set, when connecting to the database.
   */
  public final @NotNull String appName;

  /**
   * The role to use after connection; if {@code null}, then the {@link #user} is used.
   */
  public final @Nullable String role;

  /**
   * The search path to set; if {@code null}, automatically set.
   */
  public final @Nullable String searchPath;
}
