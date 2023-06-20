package com.here.mapcreator.ext.naksha;

import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;

/** Immutable POJO for holding PostgresQL database pool configuration. */
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("unused")
public class PsqlPoolConfig {

    @JsonIgnore
    public final String driverClass = "org.postgresql.Driver";

    /** The database host to connect against. */
    @JsonProperty
    public final @NotNull String host;

    /** The database port, defaults to 5432. */
    @JsonProperty(defaultValue = "5432")
    @JsonInclude(Include.NON_DEFAULT)
    public final int port;

    /** The database to open. */
    @JsonProperty
    public final @NotNull String db;

    /** The user. */
    @JsonProperty
    public final @NotNull String user;

    /** The password. */
    @JsonProperty
    public final @NotNull String password;

    // Check: https://www.postgresql.org/docs/current/runtime-config-client.html

    /** The timeout in milliseconds when trying to establish a new connection to the database. */
    @JsonProperty
    public final long connTimeout;

    /**
     * Abort any statement that takes more than the specified amount of milliseconds. A value of zero
     * (the default) disables the timeout.
     *
     * <p>The timeout is measured from the time a command arrives at the server until it is completed
     * by the server. If multiple SQL statements appear in a single simple-Query message, the timeout
     * is applied to each statement separately. (PostgreSQL versions before 13 usually treated the
     * timeout as applying to the whole query string). In extended query protocol, the timeout starts
     * running when any query-related message (Parse, Bind, Execute, Describe) arrives, and it is
     * canceled by completion of an Execute or Sync message.
     */
    @JsonProperty
    public final long stmtTimeout;

    /**
     * Abort any statement that waits longer than the specified amount of milliseconds while
     * attempting to acquire a lock on a table, index, row, or other database object. The time limit
     * applies separately to each lock acquisition attempt. The limit applies both to explicit locking
     * requests (such as LOCK TABLE, or SELECT FOR UPDATE without NOWAIT) and to implicitly-acquired
     * locks. A value of zero (the default) disables the timeout.
     *
     * <p>Unlike statement_timeout, this timeout can only occur while waiting for locks. Note that if
     * statement_timeout is nonzero, it is rather pointless to set lock_timeout to the same or larger
     * value, since the statement timeout would always trigger first. If log_min_error_statement is
     * set to ERROR or lower, the statement that timed out will be logged.
     *
     * <p>Setting lock_timeout in postgresql.conf is not recommended because it would affect all
     * sessions.
     */
    public final long lockTimeout;

    /** The minimal connections to keep alive. */
    @JsonProperty
    public final int minPoolSize;

    /** The maximal connections to use. */
    @JsonProperty
    public final int maxPoolSize;

    /**
     * This property controls the maximum amount of time (in milliseconds) that a connection is
     * allowed to sit idle in the pool. Whether a connection is retired as idle or not is subject to a
     * maximum variation of +30 seconds, and average variation of +15 seconds. A connection will never
     * be retired as idle before this timeout. A value of 0 means that idle connections are never
     * removed from the pool.
     */
    @JsonProperty
    public final long idleTimeout;

    @JsonIgnore
    public final @NotNull String url;

    @JsonIgnore
    private final int hashCode;

    @JsonCreator
    PsqlPoolConfig(
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
            @JsonProperty Long idleTimeout) {
        this.host = host;
        this.port = port == null ? 5432 : port;
        this.db = db;
        this.user = user;
        this.password = password;
        this.connTimeout = connTimeout != null && connTimeout > 0L ? max(connTimeout, 1000L) : SECONDS.toMillis(15);
        this.stmtTimeout = stmtTimeout != null && stmtTimeout > 0L ? max(stmtTimeout, 1000L) : SECONDS.toMillis(60);
        this.lockTimeout = lockTimeout != null && lockTimeout > 0L ? max(lockTimeout, 1000L) : SECONDS.toMillis(15);
        this.minPoolSize = minPoolSize != null && minPoolSize > 0 ? minPoolSize : 1;
        this.maxPoolSize =
                maxPoolSize != null && maxPoolSize >= this.minPoolSize ? maxPoolSize : max(this.minPoolSize, 200);
        this.idleTimeout = idleTimeout != null && idleTimeout > 0L ? max(idleTimeout, 60L) : MINUTES.toSeconds(10);
        this.url = "jdbc:postgresql://" + host + (this.port != 5432 ? "" : ":" + this.port) + "/" + db;
        this.hashCode = Objects.hash(
                url,
                this.user,
                this.password,
                this.connTimeout,
                this.stmtTimeout,
                this.lockTimeout,
                this.minPoolSize,
                this.maxPoolSize,
                this.idleTimeout);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PsqlPoolConfig that = (PsqlPoolConfig) o;
        return url.equals(that.url)
                && user.equals(that.user)
                && password.equals(that.password)
                && connTimeout == that.connTimeout
                && stmtTimeout == that.stmtTimeout
                && lockTimeout == that.lockTimeout
                && minPoolSize == that.minPoolSize
                && maxPoolSize == that.maxPoolSize
                && idleTimeout == that.idleTimeout;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return "PsqlConnectionParams{"
                + "driverClass='"
                + driverClass
                + "'"
                + ",url='"
                + url
                + "'"
                + ",user='"
                + user
                + "'"
                + ",connTimeout="
                + connTimeout
                + ",stmtTimeout="
                + stmtTimeout
                + ",lockTimeout="
                + lockTimeout
                + ",minPoolSize="
                + minPoolSize
                + ",maxPoolSize="
                + maxPoolSize
                + ",idleTimeout="
                + idleTimeout
                + '}';
    }
}
