/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.runtime.FunctionRuntime;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class provides the utility to run a single database query which is described by an incoming object
 * and which returns some other object of specified type.
 * It has the internal capability to build & run the necessary {@link SQLQuery} and translate
 * the resulting {@link ResultSet} into the specified response object.
 * @param <E> The incoming object type, describing the query
 * @param <R> The outgoing object response type
 */
public abstract class QueryRunner<E extends Object, R extends Object> implements ResultSetHandler<R> {

  private static final Logger logger = LogManager.getLogger();
  private static final int MIN_REMAINING_TIME_FOR_RESULT_HANDLING = 2;
  private SQLQuery query;
  private boolean useReadReplica;
  private DataSourceProvider dataSourceProvider;

  /*
  NOTE:
  This field must stay private for performance reasons.
  The input may not be kept during execution of the query to keep the memory footprint low. This field will be cleared right after
  #buildQuery(E) has been called.
  The input may only be used during the building of the query and should not be stored on any instances of subclasses.
  If some parts of the event data are needed after the execution (e.g. in the result handling), parts of it can be
  stored on instances of subclasses.
   */
  private E input;

  public QueryRunner(E input) throws SQLException, ErrorResponseException {
    this.input = input;
  }

  private static int calculateTimeout() throws SQLException {
    final FunctionRuntime runtime = FunctionRuntime.getInstance();
    int timeout = runtime.getRemainingTime() / 1000 - MIN_REMAINING_TIME_FOR_RESULT_HANDLING;
    if (timeout <= 0) {
      logger.warn("{} Not enough time left to execute query: {}s", runtime.getStreamId(), timeout);
      throw new SQLException("No time left to execute query.", "54000");
    }
    return timeout;
  }

  protected R run(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {
    return prepareQuery().run(dataSourceProvider, this, isUseReadReplica());
  }

  public final R run() throws SQLException, ErrorResponseException {
    R response = run(getDataSourceProvider());
    if (response instanceof ErrorResponse errorResponse)
      throw new ErrorResponseException(errorResponse);
    return response;
  }

  protected R write(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {
    SQLQuery query = prepareQuery();
    return handleWrite(query.isBatch() ? query.writeBatch(dataSourceProvider) : new int[]{query.write(dataSourceProvider)});
  }

  public final R write() throws SQLException, ErrorResponseException {
    return write(getDataSourceProvider());
  }

  private SQLQuery prepareQuery() throws SQLException, ErrorResponseException {
    if (query == null)
      query = buildQuery(input);
    return query
        .withQueryId(FunctionRuntime.getInstance().getStreamId())
        .withTimeout(calculateTimeout())
        .withMaximumRetries(2);
  }

  protected abstract SQLQuery buildQuery(E input) throws SQLException, ErrorResponseException;

  @Override
  public abstract R handle(ResultSet rs) throws SQLException;

  protected R handleWrite(int[] rowCounts) throws ErrorResponseException {
    return null;
  }

  public DataSourceProvider getDataSourceProvider() {
    if (dataSourceProvider == null)
      //Fail quickly in this case
      throw new NullPointerException("No dataSourceProvider was defined for this QueryRunner.");
    return dataSourceProvider;
  }

  public void setDataSourceProvider(DataSourceProvider dataSourceProvider) {
    this.dataSourceProvider = dataSourceProvider;
  }

  public <T extends QueryRunner<E, R>> T withDataSourceProvider(DataSourceProvider dataSourceProvider) {
    setDataSourceProvider(dataSourceProvider);
    return (T) this;
  }

  public boolean isUseReadReplica() {
    return useReadReplica;
  }

  public void setUseReadReplica(boolean useReadReplica) {
    this.useReadReplica = useReadReplica;
  }

  public QueryRunner withUseReadReplica(boolean useReadReplica) {
    setUseReadReplica(useReadReplica);
    return this;
  }

  private DatabaseSettings getDbSettings() {
    if (getDataSourceProvider().getDatabaseSettings() != null)
      return getDataSourceProvider().getDatabaseSettings();
    throw new IllegalStateException("The DataSourceProvider does not provide database settings.");
  }

  protected String getSchema() {
    return getDbSettings().getSchema();
  }
}
