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

import com.here.naksha.lib.core.exceptions.StorageLockException;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteRequest;
import com.here.naksha.lib.core.storage.IStorageLock;
import com.here.naksha.lib.core.storage.IWriteSession;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.postgresql.PGConnection;

public class PsqlWriteSession extends PsqlReadSession implements IWriteSession {

  PsqlWriteSession(@NotNull PsqlStorage storage, @NotNull PGConnection connection) {
    super(storage, connection);
  }

  @Override
  public boolean isMasterConnect() {
    return true;
  }

  @Override
  public @NotNull Result execute(@NotNull WriteRequest writeRequest) {
    return null;
  }

  @Override
  public @NotNull IStorageLock lockFeature(
      @NotNull String collectionId, @NotNull String featureId, long timeout, @NotNull TimeUnit timeUnit)
      throws StorageLockException {
    return null;
  }

  @Override
  public @NotNull IStorageLock lockStorage(@NotNull String lockId, long timeout, @NotNull TimeUnit timeUnit)
      throws StorageLockException {
    return null;
  }

  @Override
  public void commit() {}

  @Override
  public void rollback() {}
}
