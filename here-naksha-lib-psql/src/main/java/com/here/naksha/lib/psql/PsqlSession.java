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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.storage.IReadSession;
import java.sql.Connection;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

/**
 * The base proxy for the internal PostgresQL session.
 */
abstract class PsqlSession implements IReadSession, AutoCloseable {

  PsqlSession(
      @NotNull PostgresStorage storage,
      @NotNull NakshaContext context,
      @NotNull Connection connection,
      boolean readOnly) {
    this.session = new PostgresSession(this, storage, context, connection, readOnly);
  }

  private PostgresSession session;

  final @NotNull PostgresSession session() {
    final PostgresSession session = this.session;
    if (session == null || session.isClosed()) {
      throw new IllegalStateException("Session is closed");
    }
    return session;
  }

  @Override
  public boolean isMasterConnect() {
    return session().readOnly;
  }

  @Override
  public synchronized void close() {
    final PostgresSession session = this.session;
    if (session != null) {
      session.close();
      this.session = null;
    }
  }

  @Override
  public int getFetchSize() {
    return session().fetchSize;
  }

  @Override
  public void setFetchSize(int size) {
    if (size <= 1) {
      throw new IllegalArgumentException("The fetchSize must be greater than zero");
    }
    session().fetchSize = size;
  }

  @Override
  public long getStatementTimeout(@NotNull TimeUnit timeUnit) {
    final PostgresSession session = session();
    return timeUnit.convert(
        session.stmtTimeout > -1 ? session.stmtTimeout : session.config.stmtTimeout, MILLISECONDS);
  }

  @Override
  public void setStatementTimeout(long timeout, @NotNull TimeUnit timeUnit) {
    if (timeout < 0) {
      throw new IllegalArgumentException("The timeout must be greater/equal zero");
    }
    session().stmtTimeout = MILLISECONDS.convert(timeout, timeUnit);
  }

  @Override
  public long getLockTimeout(@NotNull TimeUnit timeUnit) {
    final PostgresSession session = session();
    return timeUnit.convert(
        session.lockTimeout > -1 ? session.lockTimeout : session.config.lockTimeout, MILLISECONDS);
  }

  @Override
  public void setLockTimeout(long timeout, @NotNull TimeUnit timeUnit) {
    if (timeout < 0) {
      throw new IllegalArgumentException("The timeout must be greater/equal zero");
    }
    session().lockTimeout = MILLISECONDS.convert(timeout, timeUnit);
  }

  @Override
  public @NotNull NakshaContext getNakshaContext() {
    return session().context;
  }
}
