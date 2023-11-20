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

import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;

import com.here.naksha.lib.core.util.CloseableResource;
import java.sql.SQLException;
import java.util.Properties;
import org.jetbrains.annotations.NotNull;
import org.postgresql.PGProperty;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.HostSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An extension of a real PostgresQL connection that supports pooling.
 */
final class PostgresConnection extends CloseableResource<PostgresInstance> {

  private static final Logger log = LoggerFactory.getLogger(PostgresConnection.class);

  // TODO: We need another constructor that can reuse an pooled PgConnection!

  PostgresConnection(
      @NotNull PsqlConnection proxy,
      @NotNull PostgresInstance postgresInstance,
      @NotNull String applicationName,
      @NotNull String schema,
      int fetchSize,
      long connTimeoutInMillis,
      long sockedReadTimeoutInMillis,
      long cancelSignalTimeoutInMillis,
      long receiveBufferSize,
      long sendBufferSize)
      throws SQLException {
    super(proxy, postgresInstance);

    final PsqlInstanceConfig config = postgresInstance.config;
    final Properties props = new Properties();
    props.setProperty(PGProperty.PG_DBNAME.getName(), config.db);
    props.setProperty(PGProperty.USER.getName(), config.user);
    props.setProperty(PGProperty.PASSWORD.getName(), config.password);
    props.setProperty(PGProperty.APPLICATION_NAME.getName(), applicationName);
    props.setProperty(PGProperty.CURRENT_SCHEMA.getName(), schema);
    props.setProperty(PGProperty.DEFAULT_ROW_FETCH_SIZE.getName(), Integer.toString(fetchSize));
    props.setProperty(PGProperty.BINARY_TRANSFER.getName(), "true");
    if (config.readOnly) {
      props.setProperty(PGProperty.READ_ONLY.getName(), "true");
    }
    props.setProperty(
        PGProperty.CONNECT_TIMEOUT.getName(),
        Long.toString(Math.min(Integer.MAX_VALUE, connTimeoutInMillis / 1000L)));
    props.setProperty(
        PGProperty.SOCKET_TIMEOUT.getName(),
        Long.toString(Math.min(Integer.MAX_VALUE, sockedReadTimeoutInMillis / 1000L)));
    props.setProperty(
        PGProperty.CANCEL_SIGNAL_TIMEOUT.getName(),
        Long.toString(Math.min(Integer.MAX_VALUE, cancelSignalTimeoutInMillis / 1000L)));
    props.setProperty(PGProperty.RECEIVE_BUFFER_SIZE.getName(), Long.toString(receiveBufferSize));
    props.setProperty(PGProperty.SEND_BUFFER_SIZE.getName(), Long.toString(sendBufferSize));
    props.setProperty(PGProperty.REWRITE_BATCHED_INSERTS.getName(), "true");

    pgConnection = new PgConnection(new HostSpec[] {config.hostSpec}, props, config.url);
    pgConnection.setAutoCommit(false);
    pgConnection.setReadOnly(config.readOnly);
    pgConnection.setHoldability(CLOSE_CURSORS_AT_COMMIT);
  }

  private final @NotNull PgConnection pgConnection;

  @Override
  protected @NotNull PostgresInstance parent() {
    final PostgresInstance parent = super.parent();
    assert parent != null;
    return parent;
  }

  @NotNull
  PgConnection get() throws SQLException {
    final PgConnection conn = pgConnection;
    if (isClosed()) {
      throw new SQLException("Connection already closed");
    }
    return conn;
  }

  @Override
  protected void destruct() {
    try {
      pgConnection.rollback();
    } catch (SQLException e) {
      log.info("Failed to rollback connection", e);
    }
    // TODO: Instead of closing the connection, put it back into the pool of the parent.
    try {
      pgConnection.close();
    } catch (SQLException e) {
      log.info("Failed to close connection", e);
    }
  }
}
