package com.here.mapcreator.ext.naksha;

import org.jetbrains.annotations.NotNull;

/**
 * Configuration builder.
 */
@SuppressWarnings("unused")
public final class PsqlPoolConfigBuilder extends PsqlAbstractBuilder<PsqlPoolConfig, PsqlPoolConfigBuilder> {

  public PsqlPoolConfigBuilder() {
  }

  @Override
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
