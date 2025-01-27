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

package com.here.xyz.util.db;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT;
import static com.here.xyz.util.db.SQLQuery.XyzSqlErrors.XYZ_FAILED_ATTEMPT;
import static com.here.xyz.util.db.pg.LockHelper.advisoryLock;
import static com.here.xyz.util.db.pg.LockHelper.advisoryUnlock;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.xyz.XyzSerializable;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.StatementConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A struct like object that contains the string for a prepared statement and the respective parameters for replacement.
 */
@JsonInclude(NON_DEFAULT)
public class SQLQuery {
  private static final Logger logger = LogManager.getLogger();
  private static final String VAR_PREFIX = "\\$\\{";
  private static final String VAR_SUFFIX = "\\}";
  private static final String FRAGMENT_PREFIX = "${{";
  private static final String FRAGMENT_SUFFIX = "}}";
  public static final String QUERY_ID = "queryId";
  public static final String TEXT_QUOTE = "$a$";
  private String statement = "";
  @JsonProperty
  private List<Object> parameters = new ArrayList<>();
  private Map<String, Object> namedParameters;
  private Map<String, String> variables;
  private Map<String, SQLQuery> queryFragments;
  private boolean async = false;
  private boolean asyncProcedure = false;
  private String lock;
  private int timeout = Integer.MAX_VALUE;
  private int maximumRetries;
  private HashMap<String, List<Integer>> namedParams2Positions = new HashMap<>();
  private PreparedStatement preparedStatement;
  private String queryId;
  private Map<String, String> labels = new HashMap<>();
  private Map<String, Object> context;
  private List<ExecutionContext> executions = new CopyOnWriteArrayList<>();
  private boolean labelsEnabled = true;
  private boolean loggingEnabled = true;
  private List<SQLQuery> queryBatch;

  private SQLQuery() {} //Only added as workaround for an issue with Jackson's Include.NON_DEFAULT setting

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
   * Returns whether this query is a batch of queries which can be executed
   * all at once by calling {@link #writeBatch(DataSourceProvider)}.
   *
   * @return True, if this query is a query batch
   */
  public boolean isBatch() {
    return queryBatch != null && !queryBatch.isEmpty();
  }

  /**
   * Adds another query to the collection of queries which form this query batch.
   * Calling the method {@link #writeBatch(DataSourceProvider)} will cause all queries of this query batch to be executed.
   * Whether an SQLQuery object is a batch or not, can be checked by calling the method {@link #isBatch()}.
   *
   * @param query The query to be added to this query batch
   */
  public void addBatch(SQLQuery query) {
    if (queryBatch == null)
      queryBatch = new ArrayList<>();
    queryBatch.add(query);
  }

  /**
   * Creates a new SQLQuery, which is a query batch of all the provided SQL queries.
   * All contained queries can be executed all at once by calling {@link #writeBatch(DataSourceProvider)}.
   * Whether an SQLQuery object is a batch or not, can be checked by calling the method {@link #isBatch()}.
   *
   * @param queries The queries to be added to the batch
   * @return An SQLQuery object which contains all the provided queries as batch
   */
  public static SQLQuery batchOf(List<SQLQuery> queries) {
    if (queries.size() == 0)
      throw new IllegalArgumentException("A batch of queries cannot be empty.");

    SQLQuery result = new SQLQuery();
    for (SQLQuery query : queries)
      result.addBatch(query);

    return result;
  }

  /**
   * Creates a new SQLQuery, which is a query batch of all the provided SQL queries.
   * All contained queries can be executed all at once by calling {@link #writeBatch(DataSourceProvider)}.
   * Whether an SQLQuery object is a batch or not, can be checked by calling the method {@link #isBatch()}.
   *
   * @param queries The queries to be added to the batch
   * @return An SQLQuery object which contains all the provided queries as batch
   */
  public static SQLQuery batchOf(SQLQuery... queries) {
    return batchOf(Arrays.asList(queries));
  }

  /**
   * Returns the query text as it has been defined for this SQLQuery.
   * Use method {@link #substitute()} prior to this method to retrieve to substitute all placeholders of this query.
   * @return
   */
  @JsonProperty
  public String text() {
    return statement;
  }

  public void setText(String text) {
    statement = text;
  }

  private List<String> batchTexts() {
    if (queryBatch == null || queryBatch.isEmpty())
      throw new IllegalStateException("This SQLQuery is not a query batch.");
    List<String> batchTexts = new ArrayList<>();
    batchTexts.add(text());
    batchTexts.addAll(queryBatch.stream().map(query -> query.text()).collect(Collectors.toList()));
    return batchTexts;
  }

  @Override
  public String toString() {
    return serializeForLogging();
  }

  /**
   * NOTE: This implementation does not produce an actually executable SQL query for all cases.
   *  So handle the usage of this method with care.
   * @return A representation of the query text with all parameters being replaced by
   *  their string-representation.
   */
  private String replaceUnnamedParametersForLogging() {
    if (parameters() == null || parameters().size() == 0)
      return text();
    String text = text();
    for (Object paramValue : parameters()) {
      Pattern p = Pattern.compile("\\?");
      text = text.replaceFirst(p.pattern(), paramValueToString(paramValue));
    }
    return text;
  }

  private String paramValueToString(Object paramValue) {
    if (paramValue == null)
      return "NULL";
    if (paramValue instanceof String stringParam)
      return escapeDollarSigns(customQuote(stringParam));
    if (paramValue instanceof Long)
      return paramValue + "::BIGINT";
    if (paramValue instanceof Number)
      return paramValue.toString();
    if (paramValue instanceof Boolean)
      return paramValue + "::BOOLEAN";
    if (paramValue instanceof Object[] arrayValue)
      return "ARRAY[" + Arrays.stream(arrayValue)
          .map(elementValue -> paramValueToString(elementValue))
          .collect(Collectors.joining(",")) + "]";
    return paramValue.toString();
  }

  private static String customQuote(String stringToQuote) {
    String quote = getEscapedCustomQuoteFor(stringToQuote, TEXT_QUOTE);
    return quote + stringToQuote + quote;
  }

  /**
   * Internal helper method that escapes $-signs, because they're treated as special chars when using the containing string as value
   * in a string / pattern-matching replacement.
   *
   * @see String#replaceAll(String, String)
   * @see Matcher#replaceFirst(String)
   *
   * @param containingString
   * @return A string that has all $-signs being
   */
  private static String escapeDollarSigns(String containingString) {
    return containingString.replaceAll("\\$", "\\\\\\$");
  }

  /**
   * Escapes custom quotes in the form of how they're being used within this class for string quoting. E.g.: `$a$`
   *
   * @param containingString
   * @param customQuoteToEscape
   * @return
   */
  private static String getEscapedCustomQuoteFor(String containingString, String customQuoteToEscape) {
    //Further escape the custom quote until finding one that is not in use yet
    while (containingString.contains(customQuoteToEscape))
      customQuoteToEscape = getEscapedCustomQuoteFor(customQuoteToEscape);

    return customQuoteToEscape;
  }

  private static String getEscapedCustomQuoteFor(String customQuoteToEscape) {
    return "$" + (char) (customQuoteToEscape.charAt(1) + 1) + "$";
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

  private List<List<Object>> batchParameters() {
    if (queryBatch == null || queryBatch.isEmpty())
      throw new IllegalStateException("This SQLQuery is not a query batch.");
    List<List<Object>> batchParameters = new ArrayList<>();
    batchParameters.add(parameters());
    batchParameters.addAll(queryBatch.stream().map(query -> query.parameters()).collect(Collectors.toList()));
    return batchParameters;
  }

  public SQLQuery substitute() {
    substitute(!isBatch());
    if (isBatch())
      queryBatch.forEach(query -> query.substitute(false));

    return this;
  }

  public String toExecutableQueryString() {
    return substitute().replaceUnnamedParametersForLogging();
  }

  private synchronized SQLQuery substitute(boolean usePlaceholders) {
    initQueryId();
    injectContext();
    replaceVars();
    replaceFragments();
    replaceNamedParameters(usePlaceholders && !isAsync());
    injectLabels();

    return this;
  }

  private void initQueryId() {
    if (getQueryId() == null)
      setQueryId(UUID.randomUUID().toString());
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

    try {
      while (m.find()) {
        String nParam = m.group(1);
        if (!namedParameters.containsKey(nParam))
          throw new IllegalArgumentException("sql: named Parameter [" + nParam + "] missing");
        if (!namedParams2Positions.containsKey(nParam))
          namedParams2Positions.put(nParam, new ArrayList<>());
        namedParams2Positions.get(nParam).add(parameters.size());
        parameters.add(namedParameters.get(nParam));
        if (!usePlaceholders) {
          statement = m.replaceFirst(paramValueToString(namedParameters.get(nParam)));
          m = p.matcher(text());
        }
      }
    }
    catch (Exception e) {
      System.out.println(e.getMessage());
    }

    if (usePlaceholders)
      statement = m.replaceAll("?");
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

  public boolean isAsyncProcedure() {
    return asyncProcedure;
  }

  public void setAsyncProcedure(boolean asyncProcedure) {
    setAsync(true);
    this.asyncProcedure = asyncProcedure;
  }

  public SQLQuery withAsyncProcedure(boolean asyncProcedure) {
    setAsyncProcedure(asyncProcedure);
    return this;
  }

  /**
   * The advisory lock key which was provided to be applied during the runtime of this query.
   *
   * @return The advisory lock key
   */
  public String getLock() {
    return lock;
  }

  /**
   * Can be used to apply an advisory lock on the provided lock key during the runtime of this query.
   *
   * @param lock The advisory lock key on which to apply the lock
   */
  public void setLock(String lock) {
    this.lock = lock;
  }

  /**
   * Can be used to apply an advisory lock on the provided lock key during the runtime of this query.
   *
   * @param lock The advisory lock key on which to apply the lock
   */
  public SQLQuery withLock(String lock) {
    setLock(lock);
    return this;
  }

  private String serializeForLogging() {
    initQueryId();
    return XyzSerializable.serialize(this);
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
    //TODO: Call initQueryId() here?
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
    withLabel(QUERY_ID, queryId);
  }

  public SQLQuery withQueryId(String queryId) {
    setQueryId(queryId);
    return this;
  }

  public SQLQuery withLabel(String labelIdentifier, String labelValue) {
    if (labelIdentifier.contains("*/") || labelValue.contains("*/"))
      throw new IllegalArgumentException("The char-sequence \"*/\" is not allowed in SQLQuery labels.");
    labels.put(labelIdentifier, labelValue);
    return this;
  }

  public Map<String, Object> getContext() {
    return context;
  }

  public void setContext(Map<String, Object> context) {
    this.context = new HashMap<>(context);
  }

  public SQLQuery withContext(Map<String, Object> context) {
    setContext(context);
    return this;
  }

  public Object context(String key) {
    if (context == null)
      return null;
    return context.get(key);
  }

  public SQLQuery context(String key, Object value) {
    if (context == null)
      setContext(Map.of(key, value));
    else
      context.put(key, value);
    return this;
  }

  private void injectContext() {
    if (context != null) {
      statement = (asyncProcedure ? "PERFORM " : "SELECT") +" context(#{context}::JSONB); " + statement;
      setNamedParameter("context", XyzSerializable.serialize(context));
    }
  }

  public boolean isLabelsEnabled() {
    return labelsEnabled;
  }

  public void setLabelsEnabled(boolean labelsEnabled) {
    this.labelsEnabled = labelsEnabled;
  }

  public SQLQuery withLabelsEnabled(boolean labelsEnabled) {
    setLabelsEnabled(labelsEnabled);
    return this;
  }

  public boolean isLoggingEnabled() {
    return loggingEnabled;
  }

  /**
   * Can be used to explicitly turn the logging off for this specific query.
   * @param loggingEnabled
   */
  public void setLoggingEnabled(boolean loggingEnabled) {
    this.loggingEnabled = loggingEnabled;
  }

  public SQLQuery withLoggingEnabled(boolean loggingEnabled) {
    setLoggingEnabled(loggingEnabled);
    return this;
  }

  private void injectLabels() {
    if (isLabelsEnabled() && !labels.isEmpty())
      statement = "/*labels(" + XyzSerializable.serialize(labels) + ")*/ " + statement;
  }

  public void cancel(long timeout) throws SQLException {
    killByLabel(QUERY_ID, getQueryId(), timeout);
  }

  public static void cancelByLabel(String labelIdentifier, String labelValue, long timeout, DataSourceProvider dataSourceProvider,
      boolean useReplica) throws SQLException {
    killByLabel(labelIdentifier, labelValue, timeout, dataSourceProvider, useReplica);
  }

  public void kill() throws SQLException {
    killByLabel(QUERY_ID, getQueryId(), 0);
  }

  public static void killByQueryId(String queryId, DataSourceProvider dataSourceProvider, boolean useReplica) throws SQLException {
    killByLabel(QUERY_ID, queryId, dataSourceProvider, useReplica);
  }

  public static void killByLabel(String labelIdentifier, String labelValue, DataSourceProvider dataSourceProvider, boolean useReplica)
      throws SQLException {
    killByLabel(labelIdentifier, labelValue, 0, dataSourceProvider, useReplica);
  }

  private void killByLabel(String labelIdentifier, String labelValue, long timeout) throws SQLException {
    for (ExecutionContext execution : executions)
      killByLabel(labelIdentifier, labelValue, timeout, execution.dataSourceProvider, execution.useReplica);
  }

  private static void killByLabel(String labelIdentifier, String labelValue, long timeout, DataSourceProvider dataSourceProvider,
      boolean useReplica) throws SQLException {
    new SQLQuery("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        + "WHERE state = 'active' "
        + "AND ${{labelMatching}} "
        + "AND pid != pg_backend_pid()")
        .withQueryFragment("labelMatching", buildLabelMatchQuery(labelIdentifier, labelValue))
        .run(dataSourceProvider, useReplica);
  }

  private static SQLQuery buildLabelMatchQuery(String labelIdentifier, String labelValue) {
    return new SQLQuery("strpos(query, '/*labels(') > 0 AND substring(query, "
        + "strpos(query, '/*labels(') + 9, "
        + "strpos(query, ')*/') - 9 - strpos(query, '/*labels('))::json->>#{labelIdentifier} = #{labelValue}")
        .withNamedParameter("labelIdentifier", labelIdentifier)
        .withNamedParameter("labelValue", labelValue);
  }

  public static boolean isRunning(DataSourceProvider dataSourceProvider, boolean useReplica, String queryId) throws SQLException {
    return isRunning(dataSourceProvider, useReplica, QUERY_ID, queryId);
  }

  /**
   * Checks whether there exists at least one running query on the target database that is matching the specified label value.
   * @param labelIdentifier
   * @param labelValue
   * @return Whether a running query exists with the specified label
   */
  public static boolean isRunning(DataSourceProvider dataSourceProvider, boolean useReplica, String labelIdentifier, String labelValue)
      throws SQLException {
    return new SQLQuery("""
        SELECT 1 FROM pg_stat_activity
          WHERE state = 'active' AND ${{labelMatching}} AND pid != pg_backend_pid()
        """)
        .withQueryFragment("labelMatching", buildLabelMatchQuery(labelIdentifier, labelValue))
        .withLoggingEnabled(false)
        .run(dataSourceProvider, rs -> rs.next(), useReplica);
  }

  private static String getClashing(Map<String, ?> map1, Map<String, ?> map2) {
    if (map1 == null || map2 == null)
      return null;
    for (String key : map1.keySet())
      if (map2.containsKey(key) && !Objects.deepEquals(map1.get(key), map2.get(key)))
        return key;
    return null;
  }

  /**
   * If this query is a reading query, use this method to execute it on the database writer.
   *
   * @param dataSourceProvider The data source provider depicting the target database to execute the query
   * @return Nothing, this method is just used for queries that do not produce a result or no result is needed
   * @throws SQLException
   */
  public void run(DataSourceProvider dataSourceProvider) throws SQLException {
    run(dataSourceProvider, false);
  }

  /**
   * If this query is a reading query, use this method to execute it on the database writer.
   *
   * @param dataSourceProvider The data source provider depicting the target database to execute the query
   * @param handler The handler to process the ResultSet of the query execution
   * @return The value which has been processed by the specified ResultSetHandler
   * @param <R> The type of the return value being produced by the ResultSetHandler
   * @throws SQLException
   */
  public <R> R run(DataSourceProvider dataSourceProvider, ResultSetHandler<R> handler) throws SQLException {
    return run(dataSourceProvider, handler, false);
  }

  /**
   * If this query is a reading query, use this method to execute it on the database reader or writer.
   *
   * @param dataSourceProvider The data source provider depicting the target database to execute the query
   * @param useReplica Whether to run on the reader of the data source provider
   * @return Nothing, this method is just used for queries that do not produce a result or no result is needed
   * @throws SQLException
   */
  public void run(DataSourceProvider dataSourceProvider, boolean useReplica) throws SQLException {
    run(dataSourceProvider, rs -> null, useReplica);
  }

  /**
   * If this query is a reading query, use this method to execute it on the database reader or writer.
   *
   * @param dataSourceProvider The data source provider depicting the target database to execute the query
   * @param handler The handler to process the ResultSet of the query execution
   * @param useReplica Whether to run on the reader of the data source provider
   * @return The value which has been processed by the specified ResultSetHandler
   * @param <R> The type of the return value being produced by the ResultSetHandler
   * @throws SQLException
   */
  public <R> R run(DataSourceProvider dataSourceProvider, ResultSetHandler<R> handler, boolean useReplica) throws SQLException {
    return (R) execute(dataSourceProvider, handler, ExecutionOperation.QUERY,
        new ExecutionContext(getTimeout(), getMaximumRetries(), dataSourceProvider, useReplica));
  }

  /**
   * If this query is an updating query, use this method to execute it on the database writer.
   *
   * @param dataSourceProvider The data source provider depicting the target database to execute the query
   * @return The update result (e.g. row counts)
   * @throws SQLException
   */
  public int write(DataSourceProvider dataSourceProvider) throws SQLException {
    return (int) execute(dataSourceProvider, rs -> -1, ExecutionOperation.UPDATE,
        new ExecutionContext(getTimeout(), getMaximumRetries(), dataSourceProvider, false));
  }

  /**
   * Executes all queries of a query batch at once on the database writer.
   * Batches of queries can be created by using the methods {@link #addBatch(SQLQuery)},
   * {@link #batchOf(SQLQuery...)} or {@link #batchOf(List)}.
   *
   * NOTE: This method will throw a {@link RuntimeException} if it is executed on a query which is not a batch.
   * If a query is actually a batch of queries can be checked using the {@link #isBatch()} method.
   *
   * @param dataSourceProvider The data source provider depicting the target database to execute the query
   * @return An array of update results (e.g. row counts)
   * @throws SQLException
   */
  public int[] writeBatch(DataSourceProvider dataSourceProvider) throws SQLException {
    return (int[]) execute(dataSourceProvider, null, ExecutionOperation.UPDATE_BATCH,
        new ExecutionContext(getTimeout(), getMaximumRetries(), dataSourceProvider, false));
  }

  private enum ExecutionOperation {
    QUERY,
    UPDATE,
    UPDATE_BATCH
  }

  private static List<String> PwdPrefixList =
   List.of("6URYTnCc", "pUNuBxnW", "JELgvJWS", "0n1UKjIv", "2YW9D4Kz", "3ZX9D4Kz", "9JwYhcgD", "qvukzFHW", "1CpZNKpG", "kwPU00Qy", "AhYtSea7", "AsSrbSE6");

  private static String hidePwds( String s )
  { return s.replaceAll("(" + String.join("|", PwdPrefixList ) + ")\\w*", "$1*******"); }

  private Object execute(DataSourceProvider dataSourceProvider, ResultSetHandler<?> handler, ExecutionOperation operation,
      ExecutionContext executionContext) throws SQLException {
    if (loggingEnabled)
      logger.info("Executing SQLQuery {}", hidePwds( ""+this ));
    substitute();

    final DataSource dataSource = executionContext.useReplica ? dataSourceProvider.getReader() : dataSourceProvider.getWriter();
    executionContext.attemptExecution();
    try {
      if (loggingEnabled)
        logger.info("Sending query to database {} {}, substituted query-text: {}",
            executionContext.useReplica ? "reader" : "writer",
            dataSourceProvider.getDatabaseSettings() != null
                ? dataSourceProvider.getDatabaseSettings().getId() : "unknown",
            hidePwds( replaceUnnamedParametersForLogging()) );

      if (isAsync())
        operation = ExecutionOperation.QUERY;
      return switch (operation) {
        case QUERY -> executeQuery(dataSource, executionContext, handler);
        case UPDATE -> executeUpdate(dataSource, executionContext);
        case UPDATE_BATCH -> executeBatchUpdate(dataSource, executionContext);
      };
    }
    catch (SQLException e) {
      if (executionContext.mayRetry(e)) {
        logger.info("{} Retry Query permitted.", getQueryId());
        executionContext.addRetriedException(e);
        return execute(dataSourceProvider, handler, operation, executionContext);
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

  private SQLQuery prepareFinalQuery(ExecutionContext executionContext) {
    if (isAsync()) {
      if (executionContext.dataSourceProvider.getDatabaseSettings() != null) {
        return new SQLQuery("SELECT asyncify(#{query}, #{password}, #{procedureCall})")
            .withNamedParameter("query", text())
            .withNamedParameter("password", executionContext.dataSourceProvider.getDatabaseSettings().getPassword())
            .withNamedParameter("procedureCall", isAsyncProcedure())
            .substitute();
      }
      else
        throw new RuntimeException("Async SQLQueries must be performed using an instance of PooledDataSources as DataSourceProvider");
    }
    return this;
  }

  private int executeUpdate(DataSource dataSource, ExecutionContext executionContext) throws SQLException {
    SQLQuery query = prepareFinalQuery(executionContext);
    return getRunner(dataSource, executionContext).update(query.text(), query.parameters().toArray());
  }

  private Object executeQuery(DataSource dataSource, ExecutionContext executionContext, ResultSetHandler<?> handler) throws SQLException {
    SQLQuery query = prepareFinalQuery(executionContext);

    if (context != null)
      handler = new Ignore1stResultSet(handler);

    final List<?> results = getRunner(dataSource, executionContext).execute(query.text(), handler, query.parameters().toArray());
    return results.size() <= 1 ? results.get(0) : results.get(results.size() - 1);
  }

  private static QueryRunner getRunner(DataSource dataSource, ExecutionContext executionContext) {
    StatementConfiguration statementConfig = executionContext.remainingQueryTimeout > 0
        ? new StatementConfiguration.Builder().queryTimeout(executionContext.remainingQueryTimeout).build()
        : null;
    return new QueryRunner(dataSource, statementConfig);
  }

  private int[] executeBatchUpdate(DataSource dataSource, ExecutionContext executionContext) throws SQLException {
    int[] batchResult;
    try (final Connection connection = dataSource.getConnection()) {
      if (getLock() != null)
        advisoryLock(getLock(), connection);

      boolean previousCommitState = connection.getAutoCommit();
      try {
        if (previousCommitState)
          connection.setAutoCommit(false);

        List<String> queryTexts = batchTexts();
        List<List<Object>> params = batchParameters();
        try (Statement stmt = connection.createStatement()) {
          for (String queryText : queryTexts)
            stmt.addBatch(queryText); //TODO: Use parameters

          if (executionContext.remainingQueryTimeout > 0)
            stmt.setQueryTimeout(executionContext.remainingQueryTimeout);

          batchResult = stmt.executeBatch();
          connection.commit();
        }
      }
      catch (SQLException e) {
        connection.rollback();
        throw e;
      }
      finally {
        if (getLock() != null)
          advisoryUnlock(getLock(), connection);

        if (previousCommitState)
          connection.setAutoCommit(true);
      }
    }
    return batchResult;
  }

  protected class ExecutionContext {
    private int queryTimeout;
    private int remainingQueryTimeout;
    private int executionAttempts;
    private int maximumRetries;
    private long startTime;
    private long lastAttemptTime;
    private List<Exception> retriedExceptions;
    private DataSourceProvider dataSourceProvider;
    private boolean useReplica;

    public ExecutionContext(int queryTimeout, int maximumRetries,
        DataSourceProvider dataSourceProvider, boolean useReplica) {
      this.queryTimeout = remainingQueryTimeout = queryTimeout;
      this.maximumRetries = maximumRetries;
      this.dataSourceProvider = dataSourceProvider;
      this.useReplica = useReplica;
      executions.add(this);
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
          If a timeout occurs right after the invocation, it could be caused
          by a serverless aurora scaling. Then we should retry again.
          57014 - query_canceled
          57P01 - admin_shutdown
           */
          sqlEx.getSQLState().equalsIgnoreCase("57014")
              /*
              NOTE: "admin_shutdown" (57P01) also is used when some admin user kills the backend
              using pg_terminate_backend. So this SQL state is not treated as recoverable,
              since anyway in serverless v2 it's not applicable during scaling anymore.
               */
              //|| sqlEx.getSQLState().equalsIgnoreCase("57P01")
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

      @Override
      public synchronized Throwable getCause() {
        Throwable cause = super.getCause();
        if (cause != null)
          return cause;
        else {
          List<Exception> retriedExceptions = getExecutionContext().retriedExceptions;
          return retriedExceptions.isEmpty() ? null : retriedExceptions.get(retriedExceptions.size() - 1);
        }
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

  private static class Ignore1stResultSet implements ResultSetHandler<Object> {
    private final ResultSetHandler<?> originalHandler;
    private boolean calledBefore;

    public Ignore1stResultSet(ResultSetHandler<?> originalHandler) {
      this.originalHandler = originalHandler;
    }

    @Override
    public Object handle(ResultSet rs) throws SQLException {
      if (!calledBefore) {
        calledBefore = true;
          //TODO: using runWriteQueryAsync together with using query context throws NPE due to returned null value
        return null;
      }
      return originalHandler.handle(rs);
    }
  }
}
