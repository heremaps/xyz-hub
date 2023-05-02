package com.here.mapcreator.ext.naksha;

import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

/**
 * Configuration builder.
 */
@SuppressWarnings("unused")
public class PsqlPoolConfigBuilder {

  public PsqlPoolConfigBuilder() {
  }

  String driverClass = "org.postgresql.Driver";
  int port = 5432;
  String host;
  String db;
  String user;
  String password;

  // Hikari connection pool configuration
  long connTimeout;
  long stmtTimeout;
  long lockTimeout;
  int minPoolSize;
  int maxPoolSize;
  long idleTimeout;

  public String getDriverClass() {
    return driverClass;
  }

  public void setDriverClass(String driverClass) {
    this.driverClass = driverClass;
  }

  public PsqlPoolConfigBuilder withDriveClass(String driveClass) {
    setDriverClass(driveClass);
    return this;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public @NotNull PsqlPoolConfigBuilder withHost(String host) {
    setHost(host);
    return this;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public @NotNull PsqlPoolConfigBuilder withPort(int port) {
    setPort(port);
    return this;
  }

  public String getDb() {
    return db;
  }

  public void setDb(String db) {
    this.db = db;
  }

  public @NotNull PsqlPoolConfigBuilder withDb(String db) {
    setDb(db);
    return this;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public @NotNull PsqlPoolConfigBuilder withUser(String user) {
    setUser(user);
    return this;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public @NotNull PsqlPoolConfigBuilder withPassword(String password) {
    setPassword(password);
    return this;
  }

  public long getConnTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(connTimeout, TimeUnit.MILLISECONDS);
  }

  public void setConnTimeout(long connTimeout, @NotNull TimeUnit timeUnit) {
    this.connTimeout = TimeUnit.MILLISECONDS.convert(connTimeout, timeUnit);
  }

  public @NotNull PsqlPoolConfigBuilder withConnTimeout(long connTimeout, @NotNull TimeUnit timeUnit) {
    setConnTimeout(connTimeout, timeUnit);
    return this;
  }

  public long getStatementTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(stmtTimeout, TimeUnit.MILLISECONDS);
  }

  public void setStatementTimeout(long stmtTimeout, @NotNull TimeUnit timeUnit) {
    this.stmtTimeout = TimeUnit.MILLISECONDS.convert(stmtTimeout, timeUnit);
  }

  public @NotNull PsqlPoolConfigBuilder withStatementTimeout(long stmtTimeout, @NotNull TimeUnit timeUnit) {
    setStatementTimeout(stmtTimeout, timeUnit);
    return this;
  }

  public long getLockTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(lockTimeout, TimeUnit.MILLISECONDS);
  }

  public void setLockTimeout(long lockTimeout, @NotNull TimeUnit timeUnit) {
    this.lockTimeout = TimeUnit.MILLISECONDS.convert(lockTimeout, timeUnit);
  }

  public @NotNull PsqlPoolConfigBuilder withLockTimeout(long lockTimeout, @NotNull TimeUnit timeUnit) {
    setLockTimeout(lockTimeout, timeUnit);
    return this;
  }

  public int getMinPoolSize() {
    return minPoolSize;
  }

  public void setMinPoolSize(int minPoolSize) {
    this.minPoolSize = minPoolSize;
  }

  public @NotNull PsqlPoolConfigBuilder withMinPoolSize(int minPoolSize) {
    setMinPoolSize(minPoolSize);
    return this;
  }

  public int getMaxPoolSize() {
    return maxPoolSize;
  }

  public void setMaxPoolSize(int maxPoolSize) {
    this.maxPoolSize = maxPoolSize;
  }

  public @NotNull PsqlPoolConfigBuilder withMaxPoolSize(int maxPoolSize) {
    setMaxPoolSize(maxPoolSize);
    return this;
  }

  public long getIdleTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(idleTimeout, TimeUnit.MILLISECONDS);
  }

  public void setIdleTimeout(long idleTimeout, @NotNull TimeUnit timeUnit) {
    this.idleTimeout = TimeUnit.MILLISECONDS.convert(idleTimeout, timeUnit);
  }

  public @NotNull PsqlPoolConfigBuilder withIdleTimeout(long idleTimeout, @NotNull TimeUnit timeUnit) {
    setIdleTimeout(idleTimeout, timeUnit);
    return this;
  }

  public @NotNull PsqlPoolConfig build() throws NullPointerException {
    if (db == null) {
      throw new NullPointerException("db");
    }
    if (user == null) {
      throw new NullPointerException("user");
    }
    if (password == null) {
      throw new NullPointerException("password");
    }
    return new PsqlPoolConfig(
        host,
        port,
        db,
        user,
        password,
        connTimeout,
        stmtTimeout,
        lockTimeout,
        minPoolSize,
        maxPoolSize,
        idleTimeout
    );
  }

}
