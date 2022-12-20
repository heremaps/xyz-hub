/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

import com.here.xyz.connectors.ErrorResponseException;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.dbutils.ResultSetHandler;

/**
 * This class provides the utility to run a single database query which is described by an incoming object
 * and which returns an some specified other object.
 * It has the internal capability to build & run the necessary {@link SQLQuery} and translate
 * the resulting {@link ResultSet} into the specified response object.
 * @param <E> The incoming object type, describing the query
 * @param <R> The outgoing object response type
 */
public abstract class QueryRunner<E extends Object, R extends Object> implements ResultSetHandler<R> {

  protected static final String SCHEMA = "schema";
  protected static final String TABLE = "table";

  private final SQLQuery query;
  private boolean useReadReplica;
  protected DatabaseHandler dbHandler;

  public QueryRunner(E input, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    this.dbHandler = dbHandler;
    query = buildQuery(input);
  }

  public R run() throws SQLException, ErrorResponseException {
    prepareQuery();
    return dbHandler.executeQueryWithRetry(query, this, useReadReplica);
  }

  public void write() throws SQLException, ErrorResponseException {
    prepareQuery();
    dbHandler.executeUpdateWithRetry(query);
  }

  private void prepareQuery() {
    query.substitute();
  }

  protected abstract SQLQuery buildQuery(E input) throws SQLException, ErrorResponseException;

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
