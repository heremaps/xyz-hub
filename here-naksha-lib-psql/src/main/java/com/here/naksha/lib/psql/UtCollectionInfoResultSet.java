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

import static com.here.naksha.lib.core.NakshaContext.currentLogger;

import com.here.naksha.lib.core.storage.ClosableIterator;
import com.here.naksha.lib.core.storage.CollectionInfo;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.view.ViewDeserialize;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class UtCollectionInfoResultSet implements ClosableIterator<CollectionInfo> {

  static final String STATEMENT = "SELECT id, jsondata FROM naksha_collection_get_all();";

  UtCollectionInfoResultSet(@NotNull ResultSet rs) {
    this.rs = rs;
    fetchNext();
  }

  private @Nullable ResultSet rs;

  private @NotNull ResultSet rs() {
    if (rs == null) {
      throw new IllegalStateException("ResultSet closed.");
    }
    return rs;
  }

  private void fetchNext() {
    try (final Json json = Json.open()) {
      //noinspection resource
      final ResultSet rs = rs();
      while (next == null && rs.next()) {
        String id = null, jsondata = null;
        try {
          id = rs.getString(1);
          jsondata = rs.getString(2);
          next = json.reader(ViewDeserialize.Storage.class)
              .forType(CollectionInfo.class)
              .readValue(jsondata);
        } catch (Exception e) {
          currentLogger()
              .warn(
                  "Failed to deserialize collection information for id '{}', json: {}",
                  id,
                  jsondata,
                  e);
        }
      }
    } catch (SQLException e) {
      currentLogger().warn("Unexpected exception while iterating result-set", e);
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
    if (rs != null) {
      try {
        rs.close();
      } catch (Exception e) {
        currentLogger(getClass()).warn("Failed to close result-set", e);
      } finally {
        rs = null;
      }
    }
  }
}
