/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

package com.here.xyz.psql;

import com.here.xyz.events.Event;
import com.here.xyz.responses.XyzResponse;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.dbutils.ResultSetHandler;

/**
 * This class provides the utility to run a single database query which is described by an XYZ {@link Event}
 * and which returns an {@link XyzResponse}.
 * It has the internal capability to build & run the necessary {@link SQLQuery} and translate
 * the resulting {@link ResultSet} into an {@link XyzResponse}.
 * @param <E> The event type
 * @param <R> The response type
 */
public abstract class QueryRunner<E extends Event, R extends XyzResponse> implements ResultSetHandler<R> {

  protected static final String SCHEMA = "schema";
  protected static final String TABLE = "table";

  protected Event event;
  private final SQLQuery query;
  private boolean useReadReplica;
  protected DatabaseHandler dbHandler;

  public QueryRunner(E event, DatabaseHandler dbHandler) throws SQLException {
    this.event = event;
    this.dbHandler = dbHandler;
    query = buildQuery(event);
  }

  public R run() throws SQLException {
    query.replaceFragments();
    query.replaceVars();
    query.replaceNamedParameters();
    return dbHandler.executeQueryWithRetry(query, this, useReadReplica);
  }

  public int write() throws SQLException {
    query.replaceFragments();
    query.replaceVars();
    query.replaceNamedParameters();
    return dbHandler.executeUpdateWithRetry(query);
  }

  protected abstract SQLQuery buildQuery(E event) throws SQLException;

  @Override
  public abstract R handle(ResultSet rs) throws SQLException;

  public boolean isUseReadReplica() {
    return useReadReplica;
  }

  public void setUseReadReplica(boolean useReadReplica) {
    this.useReadReplica = useReadReplica;
  }

  protected String getSchema() {
    return dbHandler.config.getDatabaseSettings().getSchema();
  }
}
