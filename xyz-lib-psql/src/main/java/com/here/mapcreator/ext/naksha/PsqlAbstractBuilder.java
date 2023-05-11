package com.here.mapcreator.ext.naksha;

import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

/**
 * Configuration builder.
 */
@SuppressWarnings("unused")
abstract class PsqlAbstractBuilder<TARGET, SELF extends PsqlAbstractBuilder<TARGET, SELF>> {

  @SuppressWarnings("unchecked")
  protected final @NotNull SELF self() {
    return (SELF) this;
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

  public @NotNull SELF withDriveClass(String driveClass) {
    setDriverClass(driveClass);
    return self();
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public @NotNull SELF withHost(String host) {
    setHost(host);
    return self();
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public @NotNull SELF withPort(int port) {
    setPort(port);
    return self();
  }

  public String getDb() {
    return db;
  }

  public void setDb(String db) {
    this.db = db;
  }

  public @NotNull SELF withDb(String db) {
    setDb(db);
    return self();
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public @NotNull SELF withUser(String user) {
    setUser(user);
    return self();
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public @NotNull SELF withPassword(String password) {
    setPassword(password);
    return self();
  }

  public long getConnTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(connTimeout, TimeUnit.MILLISECONDS);
  }

  public void setConnTimeout(long connTimeout, @NotNull TimeUnit timeUnit) {
    this.connTimeout = TimeUnit.MILLISECONDS.convert(connTimeout, timeUnit);
  }

  public @NotNull SELF withConnTimeout(long connTimeout, @NotNull TimeUnit timeUnit) {
    setConnTimeout(connTimeout, timeUnit);
    return self();
  }

  public long getStatementTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(stmtTimeout, TimeUnit.MILLISECONDS);
  }

  public void setStatementTimeout(long stmtTimeout, @NotNull TimeUnit timeUnit) {
    this.stmtTimeout = TimeUnit.MILLISECONDS.convert(stmtTimeout, timeUnit);
  }

  public @NotNull SELF withStatementTimeout(long stmtTimeout, @NotNull TimeUnit timeUnit) {
    setStatementTimeout(stmtTimeout, timeUnit);
    return self();
  }

  public long getLockTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(lockTimeout, TimeUnit.MILLISECONDS);
  }

  public void setLockTimeout(long lockTimeout, @NotNull TimeUnit timeUnit) {
    this.lockTimeout = TimeUnit.MILLISECONDS.convert(lockTimeout, timeUnit);
  }

  public @NotNull SELF withLockTimeout(long lockTimeout, @NotNull TimeUnit timeUnit) {
    setLockTimeout(lockTimeout, timeUnit);
    return self();
  }

  public int getMinPoolSize() {
    return minPoolSize;
  }

  public void setMinPoolSize(int minPoolSize) {
    this.minPoolSize = minPoolSize;
  }

  public @NotNull SELF withMinPoolSize(int minPoolSize) {
    setMinPoolSize(minPoolSize);
    return self();
  }

  public int getMaxPoolSize() {
    return maxPoolSize;
  }

  public void setMaxPoolSize(int maxPoolSize) {
    this.maxPoolSize = maxPoolSize;
  }

  public @NotNull SELF withMaxPoolSize(int maxPoolSize) {
    setMaxPoolSize(maxPoolSize);
    return self();
  }

  public long getIdleTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(idleTimeout, TimeUnit.MILLISECONDS);
  }

  public void setIdleTimeout(long idleTimeout, @NotNull TimeUnit timeUnit) {
    this.idleTimeout = TimeUnit.MILLISECONDS.convert(idleTimeout, timeUnit);
  }

  public @NotNull SELF withIdleTimeout(long idleTimeout, @NotNull TimeUnit timeUnit) {
    setIdleTimeout(idleTimeout, timeUnit);
    return self();
  }

  public abstract @NotNull TARGET build() throws NullPointerException;

}
