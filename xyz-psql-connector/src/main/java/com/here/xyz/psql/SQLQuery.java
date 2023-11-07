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

package com.here.xyz.psql;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.psql.datasource.StaticDataSources;
import com.here.xyz.psql.query.InlineQueryRunner;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.apache.commons.dbutils.ResultSetHandler;

/**
 * A struct like object that contains the string for a prepared statement and the respective parameters for replacement.
 */
public class SQLQuery {
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

  private HashMap<String, List<Integer>> namedParams2Positions = new HashMap<>();

  private PreparedStatement preparedStatement;

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
  public List<Object> parameters() {
    return parameters;
  }

  public SQLQuery substitute() {
    replaceVars();
    replaceFragments();
    replaceNamedParameters(!async);
    if (async) {
      SQLQuery asyncQuery = new SQLQuery("SELECT asyncify(#{query}, #{password})")
          .withNamedParameter("query", text())
          .withNamedParameter("password", PSQLXyzConnector.getInstance().getConfig().getDatabaseSettings().getPassword());
      statement = asyncQuery.substitute().text();
      return asyncQuery;
    }
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

  public static String getOperation(PropertyQuery.QueryOperation op) {
    if (op == null)
      throw new NullPointerException("op is required");

    switch (op) {
      case EQUALS:
        return "=";
      case NOT_EQUALS:
        return "<>";
      case LESS_THAN:
        return "<";
      case GREATER_THAN:
        return ">";
      case LESS_THAN_OR_EQUALS:
        return "<=";
      case GREATER_THAN_OR_EQUALS:
        return ">=";
      case CONTAINS:
        return "@>";
    }

    return "";
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

  private static String getClashing(Map<String, ?> map1, Map<String, ?> map2) {
    if (map1 == null || map2 == null)
      return null;
    for (String key : map1.keySet())
      if (map2.containsKey(key) && !Objects.deepEquals(map1.get(key), map2.get(key)))
        return key;
    return null;
  }

  public void run(DataSource dataSource) throws SQLException, ErrorResponseException {
    run(dataSource, rs -> null);
  }

  public <R> R run(DataSource dataSource, ResultSetHandler<R> handler) throws SQLException, ErrorResponseException {
    return new InlineQueryRunner<>(() -> this, handler).run(new StaticDataSources(dataSource));
  }

  public int write(DataSource dataSource) throws SQLException, ErrorResponseException {
    return new InlineQueryRunner<Void>(() -> this).write(new StaticDataSources(dataSource));
  }

  /**
   * @deprecated Can be removed, once the db client interface has been streamlined.
   */
  /** from ? to $1-$N */
  @Deprecated
  public static SQLQuery substituteAndUseDollarSyntax(SQLQuery q) {
    q.substitute();

    String translatedQuery = "";
    int i = 1;
    for (char c: q.text().toCharArray()) {
      if(c == '?') {
        translatedQuery += "$"+i++;
      }else
        translatedQuery += c;
    }

    q.statement = translatedQuery;
    return q;
  }
}
