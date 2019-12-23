/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A struct like object that contains the string for a prepared statement and the respective parameters for replacement.
 */
public class SQLQuery {

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

  public void append(String text, Object... parameters) {
    addText(text);
    if (parameters != null) {
      for (Object p : parameters) {
        this.parameters.add(p);
      }
    }
  }

  public void append(SQLQuery other) {
    addText(other.statement);
    for (Object p : other.parameters) {
      parameters.add(p);
    }
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
      for (Object value : values) {
        parameters.add(value);
      }
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

  private StringBuilder statement;
  private List<Object> parameters;
}
