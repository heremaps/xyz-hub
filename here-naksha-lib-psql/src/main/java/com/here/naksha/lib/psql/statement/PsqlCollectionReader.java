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

import com.here.naksha.lib.core.models.naksha.NakshaFeature;
import com.here.naksha.lib.core.models.storage.ReadCollections;
import com.here.naksha.lib.psql.model.XyzFeatureReadResult;
import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Duration;
import org.jetbrains.annotations.NotNull;

public class PsqlCollectionReader extends StatementCreator {

  public PsqlCollectionReader(@NotNull Connection connection, @NotNull Duration timeout) {
    super(connection, timeout);
  }

  public XyzFeatureReadResult readCollections(@NotNull ReadCollections readCollections) {
    final String SQL = "SELECT naksha_read_collections(?,?);";
    try {
      final var stmt = preparedStatement(SQL);
      try {
        Array paramIds = createArrayOf("text", readCollections.getIds().toArray());
        stmt.setArray(1, paramIds);
        stmt.setBoolean(2, readCollections.readDeleted());
        final ResultSet rs = stmt.executeQuery();
        return new XyzFeatureReadResult<>(rs, NakshaFeature.class);
      } catch (Throwable t) {
        stmt.close();
        throw t;
      }
    } catch (final Throwable t) {
      throw unchecked(t);
    }
  }
}
