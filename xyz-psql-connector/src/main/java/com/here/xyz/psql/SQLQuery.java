/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

import com.here.xyz.events.PropertyQuery;

import javax.sql.DataSource;
import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.here.xyz.psql.DatabaseHandler.HISTORY_TABLE_SUFFIX;

/**
 * A struct like object that contains the string for a prepared statement and the respective parameters for replacement.
 */
public class SQLQuery {
  private StringBuilder statement;
  private List<Object> parameters;

  private static final String PREFIX = "\\$\\{";
  private static final String SUFFIX = "\\}";
  private static final String VAR_SCHEMA = "${schema}";
  private static final String VAR_TABLE = "${table}";
  private static final String VAR_HST_TABLE = "${hsttable}";

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

  public static SQLQuery replaceNamedParameters( String query, Map<String, Object> namedParameters )
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

  public void append(String text, Object... parameters) {
    addText(text);
    if (parameters != null) {
      Collections.addAll(this.parameters, parameters);
    }
  }

  public void append(SQLQuery other) {
    addText(other.statement);
    parameters.addAll(other.parameters);
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
            .replace(VAR_HST_TABLE, sqlQuote(table+HISTORY_TABLE_SUFFIX));
  }

  protected static String replaceVars(String query, Map<String, String> replacements, String schema, String table) {
    String replaced = replaceVars(query, schema, table);
    for (String key : replacements.keySet()) {
      replaced = replaced.replaceAll(PREFIX + key + SUFFIX, sqlQuote(replacements.get(key)));
    }
    return replaced;
  }

  protected static SQLQuery selectJson(List<String> selection, DataSource dataSource) throws SQLException {
    if (selection == null) {
      return new SQLQuery("jsondata");
    }
    if (!selection.contains("type")) {
      selection.add("type");
    }

    return new SQLQuery("( select case when prj_build ?? 'properties' then prj_build else jsonb_set(prj_build,'{properties}','null'::jsonb) end from prj_build(?,jsondata) )", createSQLArray(selection.toArray(new String[0]), "text", dataSource));
  }

  /**
   * Creates a SQL Array of the given type.
   */
  protected static Array createSQLArray(final String[] strings, String type, DataSource dataSource) throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      return conn.createArrayOf(type, strings);
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

  protected static SQLQuery createKey(String key) {
    String[] results = key.split("\\.");
    return new SQLQuery(
            "jsondata->" + Collections.nCopies(results.length, "?").stream().collect(Collectors.joining("->")), results);
  }

  protected static String getValue(Object value, PropertyQuery.QueryOperation op) {
    if (value instanceof String) {
      if(op.equals(PropertyQuery.QueryOperation.CONTAINS) && ((String) value).startsWith("{") && ((String) value).endsWith("}"))
        return "(?::jsonb || '[]'::jsonb)";
      return "to_jsonb(?::text)";
    }
    if (value instanceof Number) {
      return "to_jsonb(?::numeric)";
    }
    if (value instanceof Boolean) {
      return "to_jsonb(?::boolean)";
    }
    return "";
  }
}
