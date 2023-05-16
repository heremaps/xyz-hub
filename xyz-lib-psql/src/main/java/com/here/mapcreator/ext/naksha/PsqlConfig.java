package com.here.mapcreator.ext.naksha;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.xyz.lambdas.P;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An extension to the {@link PsqlPoolConfig} that adds information, which are not needed for the PostgresQL connection pool, but generally
 * are required by applications.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("unused")
public class PsqlConfig extends PsqlPoolConfig {

  public static final String HOST = "host";
  public static final String PORT = "port";
  public static final String DB = "db";
  public static final String USER = "user";
  public static final String PASSWORD = "password";
  public static final String CONN_TIMEOUT = "connTimeout";
  public static final String STMT_TIMEOUT = "stmtTimeout";
  public static final String LOCK_TIMEOUT = "lockTimeout";
  public static final String MIN_POOL_SIZE = "minPoolSize";
  public static final String MAX_POOL_SIZE = "maxPoolSize";
  public static final String IDLE_TIMEOUT = "idleTimeout";
  public static final String SCHEMA = "schema";
  public static final String APP_NAME = "appName";
  public static final String ROLE = "role";
  public static final String SEARCH_PATH = "searchPath";

  @JsonCreator
  PsqlConfig(
      @JsonProperty(HOST) @NotNull String host,
      @JsonProperty(PORT) Integer port,
      @JsonProperty(DB) @NotNull String db,
      @JsonProperty(USER) @NotNull String user,
      @JsonProperty(PASSWORD) @NotNull String password,
      @JsonProperty(CONN_TIMEOUT) Long connTimeout,
      @JsonProperty(STMT_TIMEOUT) Long stmtTimeout,
      @JsonProperty(LOCK_TIMEOUT) Long lockTimeout,
      @JsonProperty(MIN_POOL_SIZE) Integer minPoolSize,
      @JsonProperty(MAX_POOL_SIZE) Integer maxPoolSize,
      @JsonProperty(IDLE_TIMEOUT) Long idleTimeout,
      @JsonProperty(SCHEMA) @NotNull String schema,
      @JsonProperty(APP_NAME) @NotNull String appName,
      @JsonProperty(ROLE) @Nullable String role,
      @JsonProperty(SEARCH_PATH) @Nullable String searchPath
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
