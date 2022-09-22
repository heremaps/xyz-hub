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

import static com.here.xyz.psql.DatabaseHandler.HISTORY_TABLE_SUFFIX;

import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.QueryEvent;
import com.here.xyz.psql.query.GetFeatures;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
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
  private StringBuilder statement;
  private List<Object> parameters;
  private Map<String, Object> namedParameters;
  private Map<String, String> variables;
  private Map<String, String> queryFragments;
  private static final String PREFIX = "\\$\\{";
  private static final String SUFFIX = "\\}";
  private static final String FRAGMENT_PREFIX = "${{";
  private static final String FRAGMENT_SUFFIX = "}}";
  private static final String VAR_SCHEMA = "${schema}";
  private static final String VAR_TABLE = "${table}";
  private static final String VAR_HST_TABLE = "${hsttable}";
  private static final String VAR_TABLE_SEQ = "${table_seq}";
  private static final String VAR_HST_TABLE_SEQ = "${hsttable_seq}";

  public SQLQuery() {
    this.statement = new StringBuilder();
    this.parameters = new ArrayList<>();
  }

  public SQLQuery(String text) {
    this();
    append(text);
  }

  public SQLQuery(String text, Object... parameters) {
    this();
    append(text, parameters);
  }

  public SQLQuery(String text, Map<String, Object> namedParameters) {
    this();
    append(text);
    setNamedParameters(namedParameters);
  }

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

  public static SQLQuery join(String delimiter, SQLQuery... queries) {
    return join(Arrays.asList(queries), delimiter, false);
  }

  private static SQLQuery replaceNamedParameters( String query, Map<String, Object> namedParameters )
  {  // replace #{namedVar} in query with ? and appends corresponding parameter from map namedParameters
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

  public void replaceNamedParameters() {
    if (namedParameters == null || namedParameters.size() == 0)
      return;
    if (parameters() != null && parameters().size() != 0)
      throw new RuntimeException("No named parameters can be used inside queries which use un-named parameters. "
          + "Use only named parameters instead!");
    SQLQuery q = replaceNamedParameters(text(), namedParameters);
    setText(q.text());
    addParameters(q.parameters.toArray());
  }

  public void append(String text, Object... parameters) {
    addText(text);
    addParameters(parameters);
  }

  public void append(String text, Map<String, Object> namedParameters) {
    addText(text);
    setNamedParameters(namedParameters);
  }

  public void append(SQLQuery other) {
    addText(other.statement);
    if (other.parameters != null) {
      if (parameters == null)
        parameters = new ArrayList<>();
      parameters.addAll(other.parameters);
    }
    setNamedParameters(other.namedParameters);
    setQueryFragments(other.queryFragments);
  }

  private void addText(CharSequence text) {
    if (text == null || text.length() == 0) {
      return;
    }
    if (text.charAt(0) == ' ' || statement.length() == 0 || statement.charAt(statement.length() - 1) == ' ') {
      statement.append(text);
    } else {
      statement.append(' ');
      statement.append(text);
    }
  }

  public void addParameter(Object value) {
    parameters.add(value);
  }

  public void addParameters(Object... values) {
    if (values != null) {
      Collections.addAll(parameters, values);
    }
  }

  public String text() {
    return statement.toString();
  }

  public void setText(CharSequence queryText) {
    statement.setLength(0);
    statement.append(queryText);
  }

  public List<Object> parameters() {
    return parameters;
  }

  public void setParameters(List<Object> parameters) {
    this.parameters = parameters;
  }


  /**
   * Quote the given string so that it can be inserted into an SQL statement.
   *
   * @param text the text to escape.
   * @return the escaped text surrounded with quotes.
   */
  public static String sqlQuote(final String text) {
    return text == null ? "" : '"' + text.replace("\"", "\"\"") + '"';
  }

  public static String replaceVars(String query, String schema, String table) {
    return query
            .replace(VAR_SCHEMA, sqlQuote(schema))
            .replace(VAR_TABLE, sqlQuote(table))
            .replace(VAR_HST_TABLE, sqlQuote(table+HISTORY_TABLE_SUFFIX))
            .replace(VAR_TABLE_SEQ, sqlQuote(table != null ? table.replaceAll("-", "_")+"_i_seq\";" : ""))
            .replace(VAR_HST_TABLE_SEQ, sqlQuote(table != null ? (table+HISTORY_TABLE_SUFFIX+"_seq").replaceAll("-", "_") :" "));
  }

  protected static String replaceVars(String query, Map<String, String> replacements, String schema, String table) {
    return replaceVars(replaceVars(query, schema, table), replacements);
  }

  protected static String replaceVars(String query, Map<String, String> replacements) {
    for (String key : replacements.keySet())
      query = query.replaceAll(PREFIX + key + SUFFIX, sqlQuote(replacements.get(key)));
    return query;
  }

  public void replaceVars() {
    if (variables == null)
      return;
    setText(replaceVars(text(), variables));
  }

  public void replaceFragments() {
    if (queryFragments == null)
      return;
    String text = text();
    for (String key : queryFragments.keySet())
      text = text.replace(FRAGMENT_PREFIX + key + FRAGMENT_SUFFIX, queryFragments.get(key));
    setText(text);
  }

  public static SQLQuery selectJson(QueryEvent event) throws SQLException {
    return GetFeatures.buildSelectionFragmentBWC(event);
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

  public void initNamedParameters() {
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

  public void setNamedParameter(String key, Object value) {
    initNamedParameters();
    namedParameters.put(key, value);
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

  public Map<String, String> getQueryFragments() {
    if (queryFragments == null)
      return Collections.emptyMap();
    return new HashMap<>(queryFragments);
  }

  private void initQueryFragments() {
    if (parameters() != null && parameters().size() > 0)
      throw new RuntimeException("No query fragments can be used inside queries which use parameters. Use named parameters instead!");
    if (queryFragments == null)
      queryFragments = new HashMap<>();
  }

  public void setQueryFragments(Map<String, String> queryFragments) {
    if (queryFragments == null)
      return;
    initQueryFragments();
    this.queryFragments.putAll(queryFragments);
  }

  public String getQueryFragment(String key) {
    if (queryFragments == null)
      return null;
    return queryFragments.get(key);
  }

  public void setQueryFragment(String key, String fragmentText) {
    initQueryFragments();
    queryFragments.put(key, fragmentText);
  }

  public void setQueryFragment(String key, SQLQuery fragment) {
    //The fragment itself could also have sub-fragments. These must be replaced first.
    fragment.replaceFragments();
    setQueryFragment(key, fragment.text());

    if (isClashing(queryFragments, fragment.queryFragments))
      throw new RuntimeException("Can not add sub-fragments of fragment ${{" + key + "}} to this query. "
          + "This query contains at least one fragment which clashes with these sub-fragments.");
    setQueryFragments(fragment.queryFragments);

    if (isClashing(variables, fragment.variables))
      throw new RuntimeException("Can not add variables of fragment ${{" + key + "}} to this query. "
          + "This query contains at least one variable which clashes with these variables.");
    setVariables(fragment.variables);

    if (isClashing(namedParameters, fragment.namedParameters))
      throw new RuntimeException("Can not add named parameters of fragment ${{" + key + "}} to this query. "
          + "This query contains at least one named parameter which clashes with these named parameters.");
    setNamedParameters(fragment.namedParameters);
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
