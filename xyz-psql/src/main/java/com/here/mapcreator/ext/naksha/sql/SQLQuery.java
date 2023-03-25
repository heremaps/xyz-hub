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

package com.here.mapcreator.ext.naksha.sql;

import com.here.xyz.events.PropertyQuery;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A struct like object that contains the string for a prepared statement and the respective parameters for replacement.
 */
public class SQLQuery {
  private String statement;
  private List<Object> parameters;
  private Map<String, Object> namedParameters;
  private Map<String, String> variables;
  private Map<String, SQLQuery> queryFragments;
  private static final String VAR_PREFIX = "\\$\\{";
  private static final String VAR_SUFFIX = "\\}";
  private static final String FRAGMENT_PREFIX = "${{";
  private static final String FRAGMENT_SUFFIX = "}}";
  private static final String VAR_SCHEMA = "schema";
  private static final String VAR_TABLE = "table";
  private static final String VAR_HST_TABLE = "hsttable";
  private static final String VAR_TABLE_SEQ = "table_seq";
  private static final String VAR_HST_TABLE_SEQ = "hsttable_seq";

  @Deprecated
  public SQLQuery() {
    this.statement = "";
    this.parameters = new ArrayList<>();
  }

  public SQLQuery(String text) {
    this();
    append(text);
  }

  @Deprecated
  public SQLQuery(String text, Object... parameters) {
    this();
    append(text, parameters);
  }

  @Deprecated
  public static SQLQuery join(List<SQLQuery> queries, String delimiter, boolean encloseInBrackets ) {
    if (queries == null) throw new NullPointerException("queries parameter is required");
    if (queries.size() == 0) throw new IllegalArgumentException("queries parameter is required");


    int counter = 0;
    final SQLQuery result = new SQLQuery();
    if( queries.size() > 1 && encloseInBrackets ){
      result.append("(");
    }
    for (SQLQuery q : queries) {
      if (q == null) continue;

      if (counter++ > 0) result.append(delimiter);
      result.append(q);
    }

    if( queries.size() > 1 && encloseInBrackets ){
      result.append(")");
    }

    if (counter == 0) return null;

    return result;
  }

  @Deprecated
  public void append(String text, Object... parameters) {
    addText(text);
    addParameters(parameters);
  }

  @Deprecated
  public void append(SQLQuery other) {
    if (other.parameters() != null)
      append(other.text(), other.parameters().toArray());
    else
      append(other.text());
  }

  @Deprecated
  private void addText(CharSequence text) {
    if (text == null || text.length() == 0)
      return;
    if (text.charAt(0) == ' ' || statement.length() == 0 || statement.charAt(statement.length() - 1) == ' ')
      statement += text;
    else
      statement += " " + text;
  }

  @Deprecated
  public void addParameter(Object value) {
    parameters.add(value);
  }

  private void addParameters(Object... values) {
    if (values != null) {
      Collections.addAll(parameters, values);
    }
  }

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

  //TODO: Make private when refactoring is complete
  public void setText(String queryText) {
    statement = queryText;
  }

  @Deprecated
  public List<Object> parameters() {
    return parameters;
  }

  public void substitute() {
    replaceVars();
    replaceFragments();
    replaceNamedParameters();
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

  @Deprecated
  public static String replaceVars(String query, String schema, String table) {
    SQLQuery q = new SQLQuery(query)
        .withVariable(VAR_SCHEMA, schema)
        .withVariable(VAR_TABLE, table)
        .withVariable(VAR_HST_TABLE, table + "_hst")
        .withVariable(VAR_TABLE_SEQ, table != null ? table.replaceAll("-", "_") + "_i_seq\";" : "")
        .withVariable(VAR_HST_TABLE_SEQ, table != null ? (table + "_hst_seq").replaceAll("-", "_") : " ");
    q.substitute();
    return q.text();
  }

  /**
   * Replaces #{namedVar} in the queryText with ? and appends the corresponding parameter from the specified map.
   */
  private static SQLQuery replaceNamedParameters(String query, Map<String, Object> namedParameters) {
    Pattern p = Pattern.compile("#\\{\\s*([^\\s\\}]+)\\s*\\}");
    SQLQuery qry = new SQLQuery();
    Matcher m = p.matcher( query );

    while( m.find() )
    { String nParam = m.group(1);
      if( !namedParameters.containsKey(nParam) )
        throw new IllegalArgumentException("sql: named Parameter ["+ nParam +"] missing");
      qry.addParameter( namedParameters.get(nParam) );
    }

    qry.append( m.replaceAll("?") );

    return qry;
  }

  //TODO: Replace usages by calls to #substitute()
  @Deprecated
  public static String replaceVars(String query, Map<String, String> replacements, String schema, String table) {
    return replaceVars(replaceVars(query, schema, table), replacements);
  }

  private static String replaceVars(String queryText, Map<String, String> replacements) {
    for (String key : replacements.keySet())
      queryText = queryText.replaceAll(VAR_PREFIX + key + VAR_SUFFIX, sqlQuote(replacements.get(key)));
    return queryText;
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
      replaceChildVars(variablesLookup);
    //Clear all variables
    variables = null;
  }

  private void replaceChildVars(Map<String, String> variables) {
    setText(replaceVars(text(), variables));
  }

  //TODO: Make private when refactoring is complete
  public void replaceFragments() {
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
        if (isClashing(namedParameters, fragment.namedParameters))
          throw new RuntimeException("Can not add substitute fragment ${{" + key + "}} into this query. "
              + "This query contains at least one named parameter which clashes with a named parameter of the fragment.");
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
    setText(queryText);
  }

  //TODO: Make private when refactoring is complete
  public void replaceNamedParameters() {
    if (namedParameters == null || namedParameters.size() == 0)
      return;
    if (parameters() != null && parameters().size() != 0)
      throw new RuntimeException("No named parameters can be used inside queries which use un-named parameters. "
          + "Use only named parameters instead!");
    SQLQuery q = replaceNamedParameters(text(), namedParameters);
    setText(q.text());
    addParameters(q.parameters.toArray());
    //Clear all named parameters
    namedParameters = null;
  }

  //TODO: Can be removed after completion of refactoring
  @Deprecated
  public void replaceUnnamedParameters() {
    if (parameters() == null || parameters().size() == 0)
      return;
    List<Object> params = parameters();
    //Clear all un-named parameters
    parameters = new ArrayList<>();
    int i = 0;
    for (Object paramValue : params) {
      String paramName = "param" + ++i;
      setNamedParameter(paramName, paramValue);
      setText(text().replaceFirst(Pattern.quote("?"), "#{" + paramName + "}"));
    }
  }

  public static String getOperation(PropertyQuery.QueryOperation op) {
    if (op == null) {
      throw new NullPointerException("op is required");
    }

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
    if (parameters() != null && parameters().size() > 0)
      throw new RuntimeException("No named parameters can be used inside queries which use parameters. Use only named parameters instead!");
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
    initQueryFragments();
    checkForUnnamedParametersInFragment(fragment); //TODO: Can be removed after completion of refactoring
    queryFragments.put(key, fragment);
  }

  public SQLQuery withQueryFragment(String key, SQLQuery fragment) {
    setQueryFragment(key, fragment);
    return this;
  }

  private static boolean isClashing(Map<String, ?> map1, Map<String, ?> map2) {
    if (map1 == null || map2 == null)
      return false;
    for (String key : map1.keySet())
      if (map2.containsKey(key) && !Objects.equals(map1.get(key), map2.get(key)))
        return true;
    return false;
  }
}
