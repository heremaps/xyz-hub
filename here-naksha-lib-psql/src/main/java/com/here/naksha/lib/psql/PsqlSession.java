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

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.storage.IReadSession;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

/**
 * The base proxy for the internal PostgresQL session.
 */
abstract class PsqlSession implements IReadSession, AutoCloseable {

  PsqlSession(@NotNull PostgresStorage storage, @NotNull NakshaContext context, @NotNull PsqlConnection connection) {
    this.session = new PostgresSession(this, storage, context, connection);
  }

  private final @NotNull PostgresSession session;

  final @NotNull PostgresSession session() {
    final PostgresSession session = this.session;
    if (session.isClosed()) {
      throw new IllegalStateException("Session is closed");
    }
    return session;
  }

  @Override
  public boolean isMasterConnect() {
    return session().readOnly;
  }

  @Override
  public void close() {
    session.close();
  }

  @Override
  public int getFetchSize() {
    return session().getFetchSize();
  }

  @Override
  public void setFetchSize(int size) {
    session().setFetchSize(size);
  }

  @Override
  public long getStatementTimeout(@NotNull TimeUnit timeUnit) {
    return session().getStatementTimeout(timeUnit);
  }

  @Override
  public void setStatementTimeout(long timeout, @NotNull TimeUnit timeUnit) {
    try {
      session().setStatementTimeout(timeout, timeUnit);
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  @Override
  public long getLockTimeout(@NotNull TimeUnit timeUnit) {
    return session().getLockTimeout(timeUnit);
  }

  @Override
  public void setLockTimeout(long timeout, @NotNull TimeUnit timeUnit) {
    try {
      session().setLockTimeout(timeout, timeUnit);
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  @Override
  public @NotNull NakshaContext getNakshaContext() {
    return session().context;
  }
}
