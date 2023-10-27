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

import static com.here.naksha.lib.core.NakshaLogger.currentLogger;
import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.core.exceptions.StorageLockException;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IStorageLock;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.psql.statement.PsqlCollectionWriter;
import com.here.naksha.lib.psql.statement.PsqlFeatureWriter;
import java.sql.*;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

public class PsqlWriteSession extends PsqlReadSession implements IWriteSession {

  private Long txn;

  PsqlWriteSession(@NotNull PsqlStorage storage, @NotNull Connection connection) {
    super(storage, connection);
    nakshaTxStart();
  }

  @Override
  public boolean isMasterConnect() {
    return true;
  }

  @Override
  public @NotNull Result execute(@NotNull WriteRequest writeRequest) {
    Result result = null;
    if (writeRequest instanceof WriteCollections writeCollections) {
      result = new PsqlCollectionWriter(connection, statementTimeout).writeCollections(writeCollections);
    } else if (writeRequest instanceof WriteFeatures<?> writeFeatures) {
      result = new PsqlFeatureWriter(connection, statementTimeout).writeFeatures(writeFeatures);
    }
    if (result == null) {
      throw new UnsupportedOperationException("WriteRequest not yet supported: " + writeRequest.getClass());
    }
    return result;
  }

  @Override
  public @NotNull IStorageLock lockFeature(
      @NotNull String collectionId, @NotNull String featureId, long timeout, @NotNull TimeUnit timeUnit)
      throws StorageLockException {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NotNull IStorageLock lockStorage(@NotNull String lockId, long timeout, @NotNull TimeUnit timeUnit)
      throws StorageLockException {
    return null;
  }

  @Override
  public void commit() {
    try {
      connection.commit();
    } catch (final Throwable t) {
      throw unchecked(t);
    } finally {
      // start a new transaction, this ensures that the app_id and author are set.
      nakshaTxStart();
    }
  }

  @Override
  public void rollback() {
    try {
      connection.rollback();
    } catch (final Throwable t) {
      currentLogger().atWarn("Automatic rollback failed").setCause(t).log();
    } finally {
      // start a new transaction, this ensures that the app_id and author are set.
      nakshaTxStart();
    }
  }

  protected void nakshaTxStart() {
    if (isMasterConnect()) {
      try (final PreparedStatement stmt = connection.prepareStatement("SELECT naksha_txn();")) {
        ResultSet rs = stmt.executeQuery();
        rs.next();
        txn = rs.getLong(1);
      } catch (final Exception e) {
        throw unchecked(e);
      }
    }
  }
}
