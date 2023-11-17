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

import com.here.naksha.lib.core.storage.ClosableIterator;
import com.here.naksha.lib.core.storage.CollectionInfo;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.view.ViewDeserialize;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@Deprecated
class UtCollectionInfoResultSet implements ClosableIterator<CollectionInfo> {

  static final String STATEMENT = "SELECT id, jsondata FROM naksha_collection_get_all();";

  UtCollectionInfoResultSet(@NotNull Statement stmt, @NotNull ResultSet rs) {
    this.stmt = stmt;
    this.rs = rs;
    fetchNext();
  }

  private Statement stmt;
  private ResultSet rs;

  private @NotNull ResultSet rs() {
    if (rs == null) {
      throw new IllegalStateException("ResultSet closed.");
    }
    return rs;
  }

  private void fetchNext() {
    try (final Json json = Json.get()) {
      //noinspection resource
      final ResultSet rs = rs();
      while (next == null && rs.next()) {
        final String jsondata = rs.getString(2);
        next = json.reader(ViewDeserialize.Storage.class)
            .forType(CollectionInfo.class)
            .readValue(jsondata);
      }
    } catch (final Throwable t) {
      throw unchecked(t);
    }
  }

  private @Nullable CollectionInfo next;

  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public CollectionInfo next() {
    final CollectionInfo next = this.next;
    if (next == null) {
      throw new NoSuchElementException();
    }
    this.next = null;
    fetchNext();
    return next;
  }

  @Override
  public void close() {
    try {
      rs.close();
    } catch (final Throwable t) {
      currentLogger().atWarn("Failed to close result-set").setCause(t).log();
    } finally {
      rs = null;
    }
    try {
      stmt.close();
    } catch (Throwable t) {
      currentLogger().atWarn("Failed to close statement").setCause(t).log();
    } finally {
      stmt = null;
    }
  }
}
