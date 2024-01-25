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

package com.here.xyz.util.db;

import static com.here.xyz.util.db.SQLQuery.XyzSqlErrors.XYZ_FAILED_ATTEMPT;

import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.PooledDataSources;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.StatementConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A struct like object that contains the string for a prepared statement and the respective parameters for replacement.
 */
public class SQLQuery {
  private static final Logger logger = LogManager.getLogger();
  private static final String VAR_PREFIX = "\\$\\{";
  private static final String VAR_SUFFIX = "\\}";
  private static final String FRAGMENT_PREFIX = "${{";
  private static final String FRAGMENT_SUFFIX = "}}";
  private String statement = "";
  private List<Object> parameters = new ArrayList<>();
  private Map<String, Object> namedParameters;
  private Map<String, String> variables;
  private Map<String, SQLQuery> queryFragments;
  private boolean async = false;
  private int timeout = Integer.MAX_VALUE;
  private int maximumRetries;
  private HashMap<String, List<Integer>> namedParams2Positions = new HashMap<>();
  private PreparedStatement preparedStatement;
  private String queryId;

  public SQLQuery(String text) {
    if (text != null)
      statement = text;
  }

  /**
   * Creates a new {@link SQLQuery} which combines all specified queries into one.
   * The newly created query holds references to all provided queries.
   * @param queries The queries which should be used inside the joined query
   * @param delimiter The string to put in between all the joined queries
   * @return A newly created query which joins all the specified queries together
   */
  public static SQLQuery join(List<SQLQuery> queries, String delimiter) {
    return join(queries, delimiter, false);
  }

  /**
   * Creates a new {@link SQLQuery} which combines all specified queries into one.
   * The newly created query holds references to all provided queries.
   * @param queries The queries which should be used inside the joined query
   * @param delimiter The string to put in between all the joined queries
   * @param encloseInBrackets Whether to put brackets around the resulting query
   * @return A newly created query which joins all the specified queries together
   */
  public static SQLQuery join(List<SQLQuery> queries, String delimiter, boolean encloseInBrackets) {
    List<String> fragmentPlaceholders = new ArrayList<>();
    Map<String, SQLQuery> queryFragments = new HashMap<>();
    int i = 0;
    for (SQLQuery q : queries) {
      String fragmentName = "f" + i++;
      fragmentPlaceholders.add("${{" + fragmentName + "}}");
      queryFragments.put(fragmentName, q);
    }

    String joinedSql = String.join(delimiter, fragmentPlaceholders);
    if (encloseInBrackets)
      joinedSql = "(" + joinedSql + ")";
    SQLQuery joinedQuery = new SQLQuery(joinedSql);
    for (Entry<String, SQLQuery> queryFragment : queryFragments.entrySet())
      joinedQuery.setQueryFragment(queryFragment.getKey(), queryFragment.getValue());
    return joinedQuery;
  }

  /**
   * Returns the query text as it has been defined for this SQLQuery.
   * Use method {@link #substitute()} prior to this method to retrieve to substitute all placeholders of this query.
   * @return
   */
  public String text() {
    return statement;
  }

  @Override
  public String toString() {
    return text();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof SQLQuery))
      return false;

    SQLQuery query = (SQLQuery) o;

    if (!statement.equals(query.statement)
      || !Objects.equals(parameters, query.parameters)
      || !Objects.equals(namedParameters, query.namedParameters)
      || !Objects.equals(variables, query.variables))
      return false;
    return Objects.equals(queryFragments, query.queryFragments);
  }

  @Override
  public int hashCode() {
    int result = statement.hashCode();
    result = 31 * result + (parameters != null ? parameters.hashCode() : 0);
    result = 31 * result + (namedParameters != null ? namedParameters.hashCode() : 0);
    result = 31 * result + (variables != null ? variables.hashCode() : 0);
    result = 31 * result + (queryFragments != null ? queryFragments.hashCode() : 0);
    return result;
  }

  /**
   * @deprecated Please do not use this method anymore!
   * Instead, please use more flexible / configurable / isolation-keeping placeholders
   * in the form of variables / query-fragments / named-parameters.
   */
  @Deprecated
  //TODO: Make private
  public List<Object> parameters() {
    return parameters;
  }

  public synchronized SQLQuery substitute() {
    replaceVars();
    replaceFragments();
    replaceNamedParameters(!isAsync());
    return this;
  }

  public PreparedStatement prepareStatement(Connection connection) throws SQLException {
    Map<String, Object> namedParameters = this.namedParameters;
    if (preparedStatement == null)
      preparedStatement = connection.prepareStatement(substitute().text());
    if (namedParameters == null)
      return preparedStatement;
    //Assign named parameters to according positions in the prepared statement
    for (Entry<String, Object> namedParam : namedParameters.entrySet()) {
      List<Integer> paramPositions = namedParams2Positions.get(namedParam.getKey());
      if (paramPositions != null)
        for (int pos : paramPositions)
          preparedStatement.setObject(pos + 1, namedParam.getValue());
    }
    //Clear all named parameters
    this.namedParameters = null;
    return preparedStatement;
  }

  public SQLQuery closeStatement() throws SQLException {
    if (preparedStatement != null) {
      preparedStatement.close();
      preparedStatement = null;
    }
    return this;
  }

  /**
   * Quote the given string so that it can be inserted into an SQL statement.
   *
   * @param text the text to escape.
   * @return the escaped text surrounded with quotes.
   */
  private static String sqlQuote(final String text) {
    return text == null ? "" : '"' + text.replace("\"", "\"\"") + '"';
  }

  /**
   * Replaces #{namedVar} in the queryText with ? and appends the corresponding parameters from the specified map.
   */
  private void replaceNamedParametersInt(boolean usePlaceholders) {
    Pattern p = Pattern.compile("#\\{\\s*([^\\s\\}]+)\\s*\\}");
    Matcher m = p.matcher(text());

    while(m.find()) {
      String nParam = m.group(1);
      if (!namedParameters.containsKey(nParam))
        throw new IllegalArgumentException("sql: named Parameter ["+ nParam +"] missing");
      if (!namedParams2Positions.containsKey(nParam))
        namedParams2Positions.put(nParam, new ArrayList<>());
      namedParams2Positions.get(nParam).add(parameters.size());
      parameters.add(namedParameters.get(nParam));
      if (!usePlaceholders) {
        statement = m.replaceFirst(paramValueToString(namedParameters.get(nParam)));
        m = p.matcher(text());
      }
    }

    if (usePlaceholders)
      statement = m.replaceAll("?");
  }

  private String paramValueToString(Object paramValue) {
    if (paramValue instanceof String)
      return "'" + paramValue + "'";
    if (paramValue instanceof Number)
      return paramValue.toString();
    throw new RuntimeException("Only strings or numeric values are allowed for parameters of async queries. Provided: "
        + paramValue.getClass().getSimpleName());
  }

  private void replaceVars() {
    replaceAllSubVars(Collections.emptyMap());
  }

  private void replaceAllSubVars(Map<String, String> parentVariables) {
    //Combine the parent fragments and child fragments to use as lookup for replacement
    Map<String, String> variablesLookup = new HashMap<>();
    variablesLookup.putAll(parentVariables);
    if (variables != null)
      variablesLookup.putAll(variables);

    //First replace all variables in all sub-fragments
    if (queryFragments != null)
      queryFragments.values().forEach(fragment -> fragment.replaceAllSubVars(variablesLookup));
    //Now replace all direct variables
    if (variablesLookup.size() != 0)
      replaceVars(variablesLookup);
    //Clear all variables
    variables = null;
  }

  private void replaceVars(Map<String, String> variables) {
    String queryText = text();
    for (String key : variables.keySet())
      queryText = queryText.replaceAll(VAR_PREFIX + key + VAR_SUFFIX, sqlQuote(variables.get(key)));
    statement = queryText;
  }

  private void replaceFragments() {
    replaceAllSubFragments(Collections.emptyMap());
  }

  private void replaceAllSubFragments(Map<String, SQLQuery> parentFragments) {
    //Combine the parent fragments and child fragments to use as lookup for replacement
    Map<String, SQLQuery> fragmentLookup = new HashMap<>();
    fragmentLookup.putAll(parentFragments);
    if (queryFragments != null)
      fragmentLookup.putAll(queryFragments);
    if (fragmentLookup.size() == 0)
      return;

    //First replace all query fragments in all sub-fragments & incorporate all named parameters of the sub fragments into this query
    if (queryFragments != null)
      queryFragments.forEach((key, fragment) -> {
        fragment.replaceAllSubFragments(fragmentLookup);
        final String clashing = getClashing(namedParameters, fragment.namedParameters);
        if (clashing != null)
          throw new RuntimeException("Can not add substitute fragment ${{" + key + "}} into this query. "
              + "This query contains at least one named parameter (here: " + clashing + ") which clashes with a named parameter of the fragment.");
        setNamedParameters(fragment.namedParameters);
      });
    //Now replace all direct child fragments
    replaceChildFragments(fragmentLookup);
    //Clear all fragments
    queryFragments = null;
  }

  private void replaceChildFragments(Map<String, SQLQuery> fragments) {
    String queryText = text();
    for (String key : fragments.keySet())
      queryText = queryText.replace(FRAGMENT_PREFIX + key + FRAGMENT_SUFFIX, fragments.get(key).text());
    statement = queryText;
  }

  private void replaceNamedParameters(boolean usePlaceholders) {
    if (namedParameters == null || namedParameters.size() == 0)
      return;
    if (parameters() != null && parameters().size() != 0)
      throw new RuntimeException("No named parameters can be used inside queries which use un-named parameters. "
          + "Use only named parameters instead!");
    replaceNamedParametersInt(usePlaceholders);
    //Clear all named parameters
    namedParameters = null;
  }

  public Map<String, Object> getNamedParameters() {
    if (namedParameters == null)
      return Collections.emptyMap();
    return new HashMap<>(namedParameters);
  }

  private void initNamedParameters() {
    if (namedParameters == null)
      namedParameters = new HashMap<>();
  }

  public void setNamedParameters(Map<String, Object> namedParameters) {
    if (namedParameters == null)
      return;
    initNamedParameters();
    this.namedParameters.putAll(namedParameters);
  }

  public SQLQuery withNamedParameters(Map<String, Object> namedParameters) {
    setNamedParameters(namedParameters);
    return this;
  }

  public void setNamedParameter(String key, Object value) {
    initNamedParameters();
    namedParameters.put(key, value);
  }

  public SQLQuery withNamedParameter(String key, Object value) {
    setNamedParameter(key, value);
    return this;
  }

  public Map<String, String> getVariables() {
    if (variables == null)
      return Collections.emptyMap();
    return new HashMap<>(variables);
  }

  private void initVariables() {
    if (preparedStatement != null)
      throw new IllegalStateException("Variables can not be set anymore as query is already in use by a PreparedStatement.");
    if (variables == null)
      variables = new HashMap<>();
  }

  public void setVariables(Map<String, String> variables) {
    if (variables == null)
      return;
    initVariables();
    this.variables.putAll(variables);
  }

  public String getVariable(String key) {
    if (variables == null)
      return null;
    return variables.get(key);
  }

  public void setVariable(String key, String value) {
    initVariables();
    variables.put(key, value);
  }

  public SQLQuery withVariable(String key, String value) {
    setVariable(key, value);
    return this;
  }

  public Map<String, SQLQuery> getQueryFragments() {
    if (queryFragments == null)
      return Collections.emptyMap();
    return new HashMap<>(queryFragments);
  }

  //TODO: Can be removed after completion of refactoring
  @Deprecated
  private static void checkForUnnamedParametersInFragment(SQLQuery fragment) {
    if (fragment == null)
      return;
    if (fragment.parameters() != null && fragment.parameters().size() > 0)
      throw new RuntimeException("Query which use parameters can't be added as query fragment to a query. Use named parameters instead!");
    if (fragment.queryFragments != null)
      checkForUnnamedParametersInFragments(fragment.queryFragments.values());
  }

  //TODO: Can be removed after completion of refactoring
  @Deprecated
  private static void checkForUnnamedParametersInFragments(Collection<SQLQuery> fragments) {
    fragments.forEach(f -> checkForUnnamedParametersInFragment(f));
  }

  private void initQueryFragments() {
    if (queryFragments == null)
      queryFragments = new HashMap<>();
  }

  public void setQueryFragments(Map<String, SQLQuery> queryFragments) {
    if (queryFragments == null)
      return;
    initQueryFragments();
    checkForUnnamedParametersInFragments(queryFragments.values()); //TODO: Can be removed after completion of refactoring
    this.queryFragments.putAll(queryFragments);
  }

  public SQLQuery withQueryFragments(Map<String, SQLQuery> queryFragments) {
    setQueryFragments(queryFragments);
    return this;
  }

  public SQLQuery getQueryFragment(String key) {
    if (queryFragments == null)
      return null;
    return queryFragments.get(key);
  }

  public void setQueryFragment(String key, String fragmentText) {
    setQueryFragment(key, new SQLQuery(fragmentText));
  }

  public SQLQuery withQueryFragment(String key, String fragmentText) {
    setQueryFragment(key, fragmentText);
    return this;
  }

  public void setQueryFragment(String key, SQLQuery fragment) {
    if (preparedStatement != null)
      throw new IllegalStateException("Query fragments can not be set anymore as query is already in use by a PreparedStatement.");
    initQueryFragments();
    checkForUnnamedParametersInFragment(fragment); //TODO: Can be removed after completion of refactoring
    queryFragments.put(key, fragment);
  }

  public SQLQuery withQueryFragment(String key, SQLQuery fragment) {
    setQueryFragment(key, fragment);
    return this;
  }

  public boolean isAsync() {
    return async;
  }

  public void setAsync(boolean async) {
    this.async = async;
  }

  public SQLQuery withAsync(boolean async) {
    setAsync(async);
    return this;
  }

  public int getTimeout() {
    return timeout;
  }

  /**
   * Set a timeout (in seconds) for the execution of this query. That value must be >0.
   * If this value was set to a value `<= 0`, an execution of this query will throw an exception.
   * @param timeout The timeout in seconds. A value larger than 0.
   */
  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  /**
   * Set a timeout (in seconds) for the execution of this query. That value must be >0.
   * If this value was set to a value `<= 0`, an execution of this query will throw an exception.
   * @param timeout The timeout in seconds. A value larger than 0.
   * @return The SQLQuery object itself for chaining.
   */
  public SQLQuery withTimeout(int timeout) {
    setTimeout(timeout);
    return this;
  }

  public int getMaximumRetries() {
    return maximumRetries;
  }

  public void setMaximumRetries(int maximumRetries) {
    this.maximumRetries = maximumRetries;
  }

  public SQLQuery withMaximumRetries(int maximumRetries) {
    setMaximumRetries(maximumRetries);
    return this;
  }

  public String getQueryId() {
    if (queryId == null)
      setQueryId(UUID.randomUUID().toString());
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public SQLQuery withQueryId(String queryId) {
    setQueryId(queryId);
    return this;
  }

  private static String getClashing(Map<String, ?> map1, Map<String, ?> map2) {
    if (map1 == null || map2 == null)
      return null;
    for (String key : map1.keySet())
      if (map2.containsKey(key) && !Objects.deepEquals(map1.get(key), map2.get(key)))
        return key;
    return null;
  }

  public void run(DataSourceProvider dataSourceProvider) throws SQLException {
    run(dataSourceProvider, false);
  }

  public <R> R run(DataSourceProvider dataSourceProvider, ResultSetHandler<R> handler) throws SQLException {
    return run(dataSourceProvider, handler, false);
  }

  public void run(DataSourceProvider dataSourceProvider, boolean useReplica) throws SQLException {
    run(dataSourceProvider, rs -> null, useReplica);
  }

  public <R> R run(DataSourceProvider dataSourceProvider, ResultSetHandler<R> handler, boolean useReplica) throws SQLException {
    return (R) execute(dataSourceProvider, useReplica, handler, ExecutionOperation.QUERY,
        new ExecutionContext(getTimeout(), getMaximumRetries()));
  }

  public int write(DataSourceProvider dataSourceProvider) throws SQLException {
    return (int) execute(dataSourceProvider, false, null, ExecutionOperation.UPDATE,
        new ExecutionContext(getTimeout(), getMaximumRetries()));
  }

  private enum ExecutionOperation {
    QUERY,
    UPDATE
  }

  private Object execute(DataSourceProvider dataSourceProvider, boolean useReplica, ResultSetHandler<?> handler,
      ExecutionOperation operation, ExecutionContext executionContext) throws SQLException {
    //TODO: Add proper query logging including full structure, types and parameters
    String queryText = substitute().text();
    List<Object> queryParameters = parameters();

    if (isAsync()) {
      if (dataSourceProvider instanceof PooledDataSources pooledDataSources) {
        SQLQuery asyncQuery = new SQLQuery("SELECT asyncify(#{query}, #{password})")
            .withNamedParameter("query", queryText)
            .withNamedParameter("password", pooledDataSources.getDatabaseSettings().getPassword());
        queryText = asyncQuery.substitute().text();
      }
      else
        throw new RuntimeException("Async SQLQueries must be performed using an instance of PooledDataSources as DataSourceProvider");
    }

    final DataSource dataSource = useReplica ? dataSourceProvider.getReader() : dataSourceProvider.getWriter();
    StatementConfiguration statementConfig = executionContext.remainingQueryTimeout > 0
        ? new StatementConfiguration.Builder().queryTimeout(executionContext.remainingQueryTimeout).build()
        : null;
    executionContext.attemptExecution();
    final QueryRunner runner = new QueryRunner(dataSource, statementConfig);
    try {
      logger.debug("{} executeQuery: {} - Parameters: {}", getQueryId(), this, queryParameters);
      return switch (operation) {
        case QUERY -> runner.query(queryText, handler, queryParameters.toArray());
        case UPDATE -> runner.update(queryText, queryParameters.toArray());
      };
    }
    catch (SQLException e) {
      if (executionContext.mayRetry(e)) {
        logger.info("{} Retry Query permitted.", getQueryId());
        executionContext.addRetriedException(e);
        return execute(dataSourceProvider, useReplica, handler, operation, executionContext);
      }
      else
        throw e;
    }
    finally {
      final long endTs = System.currentTimeMillis();

      long usedTimeForAttempt = endTs - executionContext.lastAttemptTime;
      executionContext.consumeTime((int) usedTimeForAttempt / 1000);

      long overallTime = endTs - executionContext.startTime;
      String usedTimeMsg = "";
      if (executionContext.executionAttempts > 1)
        usedTimeMsg = "attempt time: " + usedTimeForAttempt + "ms, ";

      logger.info("{} query time: {}ms, {}dataSource: {}", getQueryId(), overallTime, usedTimeMsg,
          dataSource instanceof ComboPooledDataSource comboPooledDataSource ? comboPooledDataSource.getJdbcUrl() : "n/a");
    }
  }

  private class ExecutionContext {
    private int queryTimeout;
    private int remainingQueryTimeout;
    private int executionAttempts;
    private int maximumRetries;
    private long startTime;
    private long lastAttemptTime;
    private List<Exception> retriedExceptions;

    public ExecutionContext(int queryTimeout, int maximumRetries) {
      this.queryTimeout = remainingQueryTimeout = queryTimeout;
      this.maximumRetries = maximumRetries;
    }

    public void consumeTime(int usedTime) {
      if (queryTimeout <= 0)
        return;
      if (usedTime > remainingQueryTimeout) {
        logger.warn("{} Query used more time than its timeout.", getQueryId());
        remainingQueryTimeout = 0;
      }
      else remainingQueryTimeout -= usedTime;
    }

    public void attemptExecution() throws FailedExecutionAttempt {
      //NOTE: The 1st retry is already the 2nd execution attempt
      if (executionAttempts > maximumRetries)
        throw new FailedExecutionAttempt("Too many execution attempts.");
      if (remainingQueryTimeout <= 0) {
        logger.warn("{} No time left to execute query.", getQueryId());
        throw new FailedExecutionAttempt("No time left to execute query.", "54000");
      }
      executionAttempts++;
      lastAttemptTime = System.currentTimeMillis();
      if (executionAttempts == 1)
        startTime = lastAttemptTime;
    }

    public boolean mayRetry(Exception e) {
      int usedTimeForAttempt = (int) (System.currentTimeMillis() - lastAttemptTime) / 1000;
      return remainingQueryTimeout > usedTimeForAttempt / 1000 && isRecoverable(e);
    }

    private boolean isRecoverable(Exception e) {
      if (e instanceof SQLException sqlEx && sqlEx.getSQLState() != null && (
          /*
          If a timeout occurs right after the invocation it could be caused
          by a serverless aurora scaling. Then we should retry again.
          57014 - query_canceled
          57P01 - admin_shutdown
           */
          sqlEx.getSQLState().equalsIgnoreCase("57014")
              || sqlEx.getSQLState().equalsIgnoreCase("57P01")
              || sqlEx.getSQLState().equalsIgnoreCase("08003")
              || sqlEx.getSQLState().equalsIgnoreCase("08006")
      )) {
        logger.warn("{} Error based on serverless scaling detected! RemainingTime: {}", getQueryId(), remainingQueryTimeout, e);
        return true;
      }
      return false;
    }

    public void addRetriedException(Exception e) {
      if (retriedExceptions == null)
        retriedExceptions = new ArrayList<>();
      retriedExceptions.add(e);
    }

    public List<Exception> getRetriedExceptions() {
      return Collections.unmodifiableList(retriedExceptions);
    }

    public SQLQuery getQuery() {
      return SQLQuery.this;
    }

    public class FailedExecutionAttempt extends SQLException {
      public FailedExecutionAttempt(String message) {
        super(message, XYZ_FAILED_ATTEMPT.errorCode);
      }

      public FailedExecutionAttempt(String message, String sqlState) {
        super(message, sqlState);
      }

      public FailedExecutionAttempt(String message, SQLException failedAttemptCause) {
        super(message, XYZ_FAILED_ATTEMPT.errorCode, failedAttemptCause);
      }

      public ExecutionContext getExecutionContext() {
        return ExecutionContext.this;
      }
    }
  }

  public enum XyzSqlErrors {
    XYZ_CONFLICT("XYZ49", "xyz_conflict"),
    XYZ_UNEXPECTED_ERROR("XYZ50", "xyz_unexpected_error"),
    XYZ_FAILED_ATTEMPT("XYZ51", "xyz_failed_attempt");

    public final String errorCode;
    public final String errorName;

    XyzSqlErrors(String errorCode, String errorName) {
      this.errorCode = errorCode;
      this.errorName = errorName;
    }

    @Override
    public String toString() {
      return errorName;
    }
  }
}
