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
package com.here.naksha.handler.psql.query;

import com.here.naksha.handler.psql.PsqlHandler;
import com.here.naksha.handler.psql.QueryRunner;
import com.here.naksha.handler.psql.SQLQueryExt;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.jetbrains.annotations.NotNull;

/**
 * This class provides the utility to run a single database query which is described by an XYZ
 * {@link Event} and which returns an {@link XyzResponse}. It has the internal capability to build &
 * run the necessary {@link SQLQueryExt} and translate the resulting {@link ResultSet} into an
 * {@link XyzResponse}.
 *
 * @param <E> The event type
 * @param <R> The response type
 */
public abstract class XyzQueryRunner<E extends Event, R extends XyzResponse> extends QueryRunner<E, R> {

  public XyzQueryRunner(E event, final @NotNull PsqlHandler psqlConnector) throws SQLException {
    super(event, psqlConnector);
  }
}
