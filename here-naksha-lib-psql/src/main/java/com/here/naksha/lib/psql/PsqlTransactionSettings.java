/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */
package com.here.naksha.lib.psql;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

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
        settings.getStatementTimeout(TimeUnit.MILLISECONDS), settings.getLockTimeout(TimeUnit.MILLISECONDS));
    psqlSettings.reader = reader;
    return psqlSettings;
  }

  PsqlTransactionSettings(long statementTimeout, long lockTimeout) {
    this.stmtTimeout = statementTimeout;
    this.lockTimeout = lockTimeout;
  }

  @Nullable
  PsqlTxReader reader;

  long stmtTimeout;
  long lockTimeout;
  String appId;
  String author;

  @Override
  public long getStatementTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(stmtTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public @NotNull ITransactionSettings withStatementTimeout(long timeout, @NotNull TimeUnit timeUnit) {
    final long stmtTimeout = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);
    if (reader != null) {
      try (final var stmt = reader.createStatement()) {
        stmt.execute("SET SESSION statement_timeout TO " + stmtTimeout);
      } catch (final Throwable t) {
        throw unchecked(t);
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
  public @NotNull ITransactionSettings withLockTimeout(long timeout, @NotNull TimeUnit timeUnit) {
    final long lockTimeout = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);
    if (reader != null) {
      try (final var stmt = reader.createStatement()) {
        stmt.execute("SET SESSION lock_timeout TO " + lockTimeout);
      } catch (final Throwable t) {
        throw unchecked(t);
      }
    }
    this.lockTimeout = lockTimeout;
    return this;
  }

  @Override
  public @NotNull String getAppId() {
    if (appId == null) {
      throw new NullPointerException();
    }
    return appId;
  }

  @Override
  public @NotNull ITransactionSettings withAppId(@NotNull String app_id) {
    this.appId = app_id;
    return this;
  }

  @Override
  public @Nullable String getAuthor() {
    return author;
  }

  @Override
  public @NotNull ITransactionSettings withAuthor(@Nullable String author) {
    this.author = author;
    return this;
  }
}
