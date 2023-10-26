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
package com.here.naksha.lib.psql.statement;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.util.json.Json;
import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class PsqlCollectionWriter extends StatementCreator {

  public PsqlCollectionWriter(@NotNull Connection connection, @NotNull Duration timeout) {
    super(connection, timeout);
  }

  public @NotNull WriteResult writeCollections(@NotNull WriteCollections writeCollections) {
    try (final PreparedStatement stmt = preparedStatement("SELECT naksha_collection_upsert(?::jsonb);")) {
      stmt.setObject(1, toJsonb(writeCollections.queries));
      final ResultSet rs = stmt.executeQuery();
      List<WriteOpResult<StorageCollection>> writeOps = toWriteOps(rs);
      return new WriteResult(writeOps);
    } catch (final Throwable t) {
      throw unchecked(t);
    }
  }

  private List<WriteOpResult<StorageCollection>> toWriteOps(ResultSet rs) throws SQLException {
    List<WriteOpResult<StorageCollection>> operations = new LinkedList<>();
    while (rs.next()) {
      String featureJson = rs.getString("r_feature");
      String operation = rs.getString("r_op");
      try (final Json json = Json.get()) {
        ObjectReader reader = json.reader();
        operations.add(new WriteOpResult<>(
            ExecutedOp.valueOf(operation), reader.readValue(featureJson, StorageCollection.class)));
      } catch (final Throwable t) {
        throw unchecked(t);
      }
    }
    return operations;
  }

  private <T> Array toJsonb(List<T> writeOps) {
    try (final Json json = Json.get()) {
      ObjectWriter writer = json.writer();
      List<String> jsonOperations = new ArrayList<>(writeOps.size());
      for (T writeOp : writeOps) {
        jsonOperations.add(writer.writeValueAsString(writeOp));
      }
      return createArrayOf("jsonb", jsonOperations.toArray());
    } catch (final Throwable t) {
      throw unchecked(t);
    }
  }
}
