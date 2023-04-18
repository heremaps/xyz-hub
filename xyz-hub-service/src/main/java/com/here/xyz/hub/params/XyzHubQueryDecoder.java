package com.here.xyz.hub.params;

import static com.here.xyz.events.QueryDelimiter.COLON;
import static com.here.xyz.events.QueryDelimiter.EXCLAMATION_MARK;
import static com.here.xyz.events.QueryDelimiter.PLUS;
import static com.here.xyz.events.QueryParameterType.ANY;
import static com.here.xyz.events.QueryParameterType.STRING;
import static com.here.xyz.hub.params.XyzHubQueryParameters.QUERY;
import static com.here.xyz.hub.params.XyzHubQueryParameters.TAGS;

import com.here.xyz.events.QueryDelimiter;
import com.here.xyz.events.QueryParameter;
import com.here.xyz.events.QueryParameterDecoder;
import com.here.xyz.events.QueryParameterType;
import com.here.xyz.exceptions.ParameterError;
import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The special XYZ-Hub query decoder.
 */
class XyzHubQueryDecoder extends QueryParameterDecoder {

  private static final Map<String, String> EXPAND = new HashMap<String, String>() {
    {
      put("f", "");
      put("createdAt", "properties.@ns:com:here:xyz.createdAt");
      put("updatedAt", "properties.@ns:com:here:xyz.updatedAt");
      put("p", "properties.");
      put("xyz", "properties.@ns:com:here:xyz.");
    }
  };

  @Deprecated
  private static final Map<String, String> OLD_EXPAND = new HashMap<String, String>() {
    {
      put("f.", "");
      put("f.createdAt", "properties.@ns:com:here:xyz.createdAt");
      put("f.updatedAt", "properties.@ns:com:here:xyz.updatedAt");
      put("p.", "properties.");
      put("xyz.", "properties.@ns:com:here:xyz.");
    }
  };

  @Override
  protected boolean stopOnDelimiter(@NotNull QueryDelimiter delimiter) {
    // We need the colon for our "query:properties.name" search, but we only split at the first colon of the “query” key.
    if (isKey() && delimiter == COLON && equals(QUERY, sb)) {
      return true;
    }
    if (isKeyOrArgument() && delimiter == EXCLAMATION_MARK) {
      // This is the right way to implement the query (in a performant and RFC compliant way).
      //
      // We use the exclamation mark in the first argument of all “query” parameters as expander!
      // We will support: "query:xyz!version=gt=5" and expand it to "&query:properties.@ns:com:here:xyz.version=gt=5".
      // This can be archived the same way then previously "&p.@ns:com:here:xyz.version=gt=5".
      if (QUERY.equals(parameter.key()) && parameter.arguments().size() == 0) {
        final String key = sb.toString();
        final String replacement = EXPAND.get(key);
        if (replacement != null) {
          sb.setLength(0);
          sb.append(replacement);
          return false;
        }
      }
      // Otherwise, we need the exclamation mark for the not equals (!=) downward compatibility hack.
      return true;
    }
    // We need the plus for downward compatibility with tags query.
    if (delimiter == PLUS && isValue() && TAGS.equals(parameter.key())) {
      return true;
    }
    // Super supports equals, ampersand, semicolon and comma.
    return super.stopOnDelimiter(delimiter);
  }

  // Add downward compatibility: Merge all search query parameter into "query:{json-path}".
  // Basically, for example "&p.value=gt=5,6" will result is a search for "properties.value" greater-than 5 or 6.
  @Deprecated
  private @Nullable String expand(@NotNull String name) {
    for (final Map.Entry<String, String> entry : OLD_EXPAND.entrySet()) {
      final String key = entry.getKey();
      final String replacement = entry.getValue();
      if (name.startsWith(key)) {
        return replacement;
      }
    }
    return null;
  }

  // TODO: We need to get rid of this, because it is utterly broken by design (as the semantic meaning can't be removed from the dot).
  //       This is just a downward compatibility hack for old "p.{name}" hack and the others.
  @Override
  protected @NotNull QueryParameter newParameter() throws ParameterError {
    final String name = (String) sbToValue(QueryParameterType.STRING);
    assert name != null;

    final QueryParameter p;
    final String property = expand(name);
    if (property != null) {
      p = new QueryParameter(parameterList, QUERY, parameterList.size());
      p.argumentsDelimiter().add(COLON);
      p.arguments().add(property);
    } else {
      p = new QueryParameter(parameterList, name, parameterList.size());
    }
    return p;
  }

  // TODO: We need to get rid of this, because it is utterly broken by design (as the semantic meaning can't be removed from the dot).
  //       This is just a downward compatibility hack for old "p.{name}" hack and the others.
  @Override
  protected void addArgumentAndDelimiter(@NotNull QueryDelimiter prefix) throws ParameterError {
    assert parameter != null;
    Object value;
    // Expand the first argument of query to the property name.
    if (QUERY.equals(parameter.key()) && parameter.arguments().size() == 0) {
      value = sbToValue(STRING);
      assert value instanceof String;
      final String property = expand((String) value);
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

}
