package com.here.naksha.lib.psql;

import com.here.naksha.lib.core.storage.ITransactionSettings;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class PsqlTransactionSettings implements ITransactionSettings {

  /**
   * Internally called by the {@link PsqlTxReader} to convert the given transaction settings into PSQL settings that automatically convert
   * changes for statement- or lock-timeout to the underlying connection.
   *
   * @param settings The settings to convert.
   * @param reader   The reader to bind.
   * @return PSQL settings bound to the given reader; if the given settings are PSQL settings, then only binding the reader.
   */
  static PsqlTransactionSettings of(@NotNull ITransactionSettings settings, @NotNull PsqlTxReader reader) {
    if (settings instanceof PsqlTransactionSettings psqlSettings) {
      psqlSettings.reader = reader;
      return psqlSettings;
    }
    final PsqlTransactionSettings psqlSettings = new PsqlTransactionSettings(
        settings.getStatementTimeout(TimeUnit.MILLISECONDS),
        settings.getLockTimeout(TimeUnit.MILLISECONDS)
    );
    psqlSettings.reader = reader;
    return psqlSettings;
  }

  PsqlTransactionSettings(long statementTimeout, long lockTimeout) {
    this.stmtTimeout = statementTimeout;
    this.lockTimeout = lockTimeout;
  }

  @Nullable PsqlTxReader reader;
  long stmtTimeout;
  long lockTimeout;

  @Override
  public long getStatementTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(stmtTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public @NotNull ITransactionSettings withStatementTimeout(long timeout, @NotNull TimeUnit timeUnit) throws Exception {
    final long stmtTimeout = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);
    if (reader != null) {
      try (final var stmt = reader.createStatement()) {
        stmt.execute("SET SESSION statement_timeout TO " + stmtTimeout);
      }
    }
    this.stmtTimeout = stmtTimeout;
    return this;
  }

  @Override
  public long getLockTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(lockTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public @NotNull ITransactionSettings withLockTimeout(long timeout, @NotNull TimeUnit timeUnit) throws Exception {
    final long lockTimeout = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);
    if (reader != null) {
      try (final var stmt = reader.createStatement()) {
        stmt.execute("SET SESSION lock_timeout TO " + lockTimeout);
      }
    }
    this.lockTimeout = lockTimeout;
    return this;
  }
}
