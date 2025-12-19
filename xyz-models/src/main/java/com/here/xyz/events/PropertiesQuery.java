/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.events;


import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class PropertiesQuery extends ArrayList<PropertyQueryList> {
  public static final String FEATURE_PREFIX = "f.";
  public static final String PROPERTIES_PREFIX = "p.";
  public static final String ALIAS_PREFIX = "$";

  private static final String PROPERTIES_EVENT_PREFIX = "properties.";

  //Special search keys
  public static final Map<String, String> SEARCH_KEY_REPLACEMENTS = Map.of(
          //default indices are getting used
          FEATURE_PREFIX + "id", "id",
          FEATURE_PREFIX + "geometry.type", "geometry.type",
          //those are only available if space < 10k and has OLD_LAYOUT - otherwise an index is needed
          FEATURE_PREFIX + "createdAt", "properties.@ns:com:here:xyz.createdAt",
          FEATURE_PREFIX + "updatedAt", "properties.@ns:com:here:xyz.updatedAt",
          FEATURE_PREFIX + "tags", "properties.@ns:com:here:xyz.tags"
  );

  public PropertiesQuery() {}

  public PropertiesQuery(Collection<? extends PropertyQueryList> c) {
    super(c);
  }

  public PropertiesQuery filterOutNamedProperty(String... propertyNames) {
    if (propertyNames == null || propertyNames.length == 0) return this;
    final List<String> keys = Arrays.asList(propertyNames);

    for (Iterator<PropertyQueryList> outerIt = this.iterator(); outerIt.hasNext();) {
      final PropertyQueryList list = outerIt.next();
      list.removeIf(prop -> keys.contains(prop.getKey()));
      if (list.isEmpty()) outerIt.remove();
    }

    return this;
  }

  public List<String> getQueryKeys(){
    ArrayList keyList = new ArrayList();
    for (PropertyQueryList queries : this){
      for(PropertyQuery query : queries){
        keyList.add(query.getKey());
      }
    }
    return keyList;
  }

  public static PropertiesQuery fromString(String query) {
    return fromString(query, "", false);
  }

  public static PropertiesQuery fromString(String query, String property, boolean spaceProperties) {
      if (query == null || query.length() == 0)
        return null;

      PropertyQueryList pql = new PropertyQueryList();
      Stream.of(query.split("&"))
              .map(queryParam -> queryParam.startsWith("tags=") ? transformLegacyTags(queryParam) : queryParam)
              .filter(queryParam -> queryParam.startsWith(PROPERTIES_PREFIX) || queryParam.startsWith(FEATURE_PREFIX)
                      || queryParam.startsWith(ALIAS_PREFIX) || spaceProperties)
              .forEach(keyValuePair -> {
                PropertyQuery propertyQuery = new PropertyQuery();

                String operatorComma = "-#:comma:#-";
                try {
                  keyValuePair = keyValuePair.replaceAll(",", operatorComma);
                  keyValuePair = URLDecoder.decode(keyValuePair, "utf-8");
                } catch (UnsupportedEncodingException e) {
                  e.printStackTrace();
                }

                int position=0;
                String op=null;

                //store "main" operator. Needed for such cases foo=bar-->test
                for (String shortOperator : PropertyQuery.QueryOperation.inputRepresentations()) {
                  int currentPositionOfOp = keyValuePair.indexOf(shortOperator);
                  if (currentPositionOfOp != -1) {
                    if(
                      // feature properties query
                            (!spaceProperties && (op == null || currentPositionOfOp < position || ( currentPositionOfOp == position && op.length() < shortOperator.length() ))) ||
                                    // space properties query
                                    (keyValuePair.substring(0,currentPositionOfOp).equals(property) && spaceProperties && (op == null || currentPositionOfOp < position || ( currentPositionOfOp == position && op.length() < shortOperator.length() )))
                    ) {
                      op = shortOperator;
                      position = currentPositionOfOp;
                    }
                  }
                }

                if (op != null) {
                  String[] keyVal = new String[] {
                          keyValuePair.substring(0, position).replaceAll(operatorComma,","),
                          keyValuePair.substring(position + op.length())
                  };
                  //Cut from API-Gateway appended "="
                  if ((">".equals(op) || "<".equals(op)) && keyVal[1].endsWith("="))
                    keyVal[1] = keyVal[1].substring(0, keyVal[1].length() - 1);

                  propertyQuery.setKey(spaceProperties ? keyVal[0] : getConvertedKey(keyVal[0]));
                  propertyQuery.setOperation(PropertyQuery.QueryOperation.fromInputRepresentation(op));
                  String[] rawValues = keyVal[1].split(operatorComma);

                  ArrayList<Object> values = new ArrayList<>();
                  for (String rawValue : rawValues)
                    values.add(getConvertedValue(rawValue));

                  propertyQuery.setValues(values);
                  pql.add(propertyQuery);
                }
              });

      PropertiesQuery pq = new PropertiesQuery();
      pq.add(pql);

      if (pq.stream().flatMap(List::stream).mapToLong(l -> l.getValues().size()).sum() == 0)
        return null;

      return pq;
  }

  public static String getConvertedKey(String rawKey) {
    if (rawKey.startsWith(PROPERTIES_PREFIX))
      return rawKey.replaceFirst(PROPERTIES_PREFIX, PROPERTIES_EVENT_PREFIX);

    //replace special searches
    String replacement = SEARCH_KEY_REPLACEMENTS.get(rawKey);
    if(replacement != null)
      return  replacement;
    //cut "f." for all searches on root-properties which are not special searches
    if (rawKey.startsWith(FEATURE_PREFIX))
      return rawKey.substring(2);
    return rawKey;
  }

  public static Object getConvertedValue(String rawValue) {
    // JSONPath
    if (rawValue != null && rawValue.startsWith(ALIAS_PREFIX)) {
      return rawValue;
    }

    // Boolean
    if (rawValue.equals("true")) {
      return true;
    }
    if (rawValue.equals("false")) {
      return false;
    }
    // Long
    try {
      return Long.parseLong(rawValue);
    } catch (NumberFormatException ignored) {
    }
    // Double
    try {
      return Double.parseDouble(rawValue);
    } catch (NumberFormatException ignored) {
    }

    if (rawValue.length() > 2 && rawValue.charAt(0) == '"' && rawValue.charAt(rawValue.length() - 1) == '"') {
      return rawValue.substring(1, rawValue.length() - 1);
    }

    if (rawValue.length() > 2 && rawValue.charAt(0) == '\'' && rawValue.charAt(rawValue.length() - 1) == '\'') {
      return rawValue.substring(1, rawValue.length() - 1);
    }

    if(rawValue.equalsIgnoreCase(".null"))
      return null;

    // String
    return rawValue;
  }

  private static String transformLegacyTags(String legacyTagsQuery) {
    String[] tagQueryParts = legacyTagsQuery.split("=");
    if (tagQueryParts.length != 2)
      return legacyTagsQuery;
    String tags = tagQueryParts[1];

    return FEATURE_PREFIX + "tags" + "=cs=" + tags;
  }

  public List<String> getJsonPathValues() {
    List<String> jsonPaths = new ArrayList<>();

    for (PropertyQueryList queries : this) {
      for (PropertyQuery query : queries) {
        for (Object value : query.getValues()) {
          if (value instanceof String) {
            String s = (String) value;
            if (s.startsWith(ALIAS_PREFIX)) {
              jsonPaths.add(s);
            }
          }
        }
      }
    }

    return jsonPaths;
  }
}
