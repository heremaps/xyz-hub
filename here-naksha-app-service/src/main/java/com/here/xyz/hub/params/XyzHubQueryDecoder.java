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
package com.here.xyz.hub.params;

import static com.here.xyz.events.QueryDelimiter.COLON;
import static com.here.xyz.events.QueryDelimiter.STAR;
import static com.here.xyz.events.QueryParameterType.ANY;
import static com.here.xyz.events.QueryParameterType.STRING;
import static com.here.xyz.hub.params.XyzHubQueryParameters.CLUSTERING;
import static com.here.xyz.hub.params.XyzHubQueryParameters.CLUSTERING_DOT;
import static com.here.xyz.hub.params.XyzHubQueryParameters.QUERY;
import static com.here.xyz.hub.params.XyzHubQueryParameters.SELECTION;
import static com.here.xyz.hub.params.XyzHubQueryParameters.TWEAKS;
import static com.here.xyz.hub.params.XyzHubQueryParameters.TWEAKS_DOT;

import com.here.naksha.lib.core.exceptions.ParameterError;
import com.here.xyz.events.QueryDelimiter;
import com.here.xyz.events.QueryParameter;
import com.here.xyz.events.QueryParameterDecoder;
import com.here.xyz.events.QueryParameterType;
import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The special XYZ-Hub query decoder with plenty of hacks to stay downward compatible.
 */
class XyzHubQueryDecoder extends QueryParameterDecoder {

  /**
   * All supported expansion macros.
   */
  private static final Map<String, String> EXPANSION_MARCO = new HashMap<String, String>() {
    {
      put("p", "properties.");
      put("xyz", "properties.@ns:com:here:xyz.");
    }
  };

  @Deprecated
  private static final Map<String, String> OLD_QUERY = new HashMap<String, String>() {
    {
      put("f.", "");
      put("f.createdAt", "properties.@ns:com:here:xyz.createdAt");
      put("f.updatedAt", "properties.@ns:com:here:xyz.updatedAt");
      put("p.", "properties.");
      put("xyz.", "properties.@ns:com:here:xyz.");
      put("author", "properties.@ns:com:here:xyz.author");
    }
  };

  @Override
  protected boolean stopOnDelimiter(@NotNull QueryDelimiter delimiter, boolean wasUrlEncoded) throws ParameterError {
    // If the client URL encoded a delimiter, the semantic meaning is gone, we use it as normal character.
    if (wasUrlEncoded) {
      sb.append(delimiter.delimiterChar);
      return false;
    }
    if (delimiter == STAR && parsingKeyOrArgument() && parameter.arguments().size() == 0) {
      // This is a better way to implement the query parameter name expansion (in an RFC compliant way).
      //
      // We use the star in the first argument of all parameters as macro expansion.
      // Examples:
      // "query:p*name=foo" expands to "&query:properties.name=foo".
      // "query:xyz*createdAt=gt=12345678" expands to "&query:properties.@ns:com:here:xyz.createdAt=gt=12345678".
      //
      // You can take away the macro expansion by simply URL encoding the star.
      final String key = sb.toString();
      final String replacement = EXPANSION_MARCO.get(key);
      if (replacement != null) {
        sb.setLength(0);
        sb.append(replacement);
        return false;
      }
      throw new ParameterError(errorMsg("Invalid macro expansion found: ", key));
    }
    // Use all other delimiters to split arguments or values.
    return true;
  }

  private @Nullable String old_query(@NotNull String name) {
    for (final Map.Entry<String, String> entry : OLD_QUERY.entrySet()) {
      final String key = entry.getKey();
      assert key.length() > 0;
      final String replacement = entry.getValue();
      if (key.charAt(key.length() - 1) == '.' && name.startsWith(key)) {
        // For example "f.id" becomes "id", "p.name" becomes "properties.name".
        return name.substring(key.length()) + replacement;
      } else if (name.equals(key)) {
        // For example "&author" becomes "properties.@ns:com:here:xyz.author".
        return replacement;
      }
    }
    return null;
  }

  // TODO: We need to get rid of this, because it is utterly broken by design (as the semantic meaning can't be
  // removed from the dot).
  //       This is just a downward compatibility hack for old "p.{name}" hack and the others.
  @Override
  protected @NotNull QueryParameter newParameter() throws ParameterError {
    final String name = (String) sbToValue(QueryParameterType.STRING);
    assert name != null;
    final QueryParameter p;

    // Previously DataHub supported search parameters things like "&p.name!=foo", "&p.age<=5" or "&p.age>=3".
    // We changed this into a more RFC compliant standard way like: "&query:p*name!=foo", "&query:p*age<=5" or
    // "&query:p*age>=3".
    // This fixes all the ugly hacks like "&p.name=foo" and translate them into "&query:p*name=foo" and alike.
    final String query = old_query(name);
    if (query != null) {
      p = new QueryParameter(parameterList, QUERY, parameterList.size());
      p.argumentsDelimiter().add(COLON);
      p.arguments().add(query);
      return p;
    }
    if (name.startsWith(TWEAKS_DOT)) { // tweaks.param
      p = new QueryParameter(parameterList, TWEAKS, parameterList.size());
      p.argumentsDelimiter().add(COLON);
      p.arguments().add(name.substring(TWEAKS_DOT.length()));
      return p;
    }
    if (name.startsWith(CLUSTERING_DOT)) { // clustering.param
      p = new QueryParameter(parameterList, CLUSTERING, parameterList.size());
      p.argumentsDelimiter().add(COLON);
      p.arguments().add(name.substring(CLUSTERING_DOT.length()));
      return p;
    }
    p = new QueryParameter(parameterList, name, parameterList.size());
    return p;
  }

  // TODO: We need to get rid of this, because it is utterly broken by design (as the semantic meaning can't be
  // removed from the dot).
  //       This is just a downward compatibility hack for old "p.{name}" hack and the others.
  @Override
  protected void addArgumentAndDelimiter(@NotNull QueryDelimiter prefix) throws ParameterError {
    assert parameter != null;
    Object value;
    // Expand the first argument of query to the property name.
    if (QUERY.equals(parameter.key()) && parameter.arguments().size() == 0) {
      value = sbToValue(STRING);
      assert value instanceof String;
      final String property = old_query((String) value);
      if (property != null) {
        // The value as expanded, for example "p.name" to "properties.name".
        // This allows us to do "&query:p.value=gt=10".
        value = property;
      }
    } else {
      value = sbToValue(ANY);
    }
    parameter.arguments().add(value);
    parameter.argumentsDelimiter().add(prefix);
  }

  @Override
  protected void addValueAndDelimiter(@NotNull QueryDelimiter postfix) throws ParameterError {
    assert parameter != null;
    if (SELECTION.equals(parameter.key())) {
      Object value = sbToValue(STRING);
      if (!(value instanceof String) || ((String) value).isEmpty()) {
        return;
      }
      String name = old_query((String) value);
      if (name == null) {
        name = (String) value;
      }
      parameter.values().add(name);
      parameter.valuesDelimiter().add(postfix);
    } else {
      super.addValueAndDelimiter(postfix);
    }
  }
}
