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

package com.here.xyz.psql.query;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.TagsQuery;
import com.here.xyz.psql.PSQLXyzConnector;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.query.helpers.GetIndexList;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/*
NOTE: All subclasses of QueryEvent are deprecated except SearchForFeaturesEvent.
Once refactoring is complete, all members of SearchForFeaturesEvent can be pulled up to QueryEvent and QueryEvent
can be renamed to SearchForFeaturesEvent again.
 */
public class SearchForFeatures<E extends SearchForFeaturesEvent, R extends XyzResponse> extends GetFeatures<E, R> {

  protected boolean hasSearch;

  public SearchForFeatures(E event) throws SQLException, ErrorResponseException {
    super(event);
  }

  public static void checkCanSearchFor(SearchForFeaturesEvent event, PSQLXyzConnector dbHandler) throws ErrorResponseException {
    if (!canSearchFor(XyzEventBasedQueryRunner.readTableFromEvent(event), event.getPropertiesQuery(), dbHandler))
      throw new ErrorResponseException(XyzError.ILLEGAL_ARGUMENT,
          "Invalid request parameters. Search for the provided properties is not supported for this space.");
  }

  @Override
  protected SQLQuery buildQuery(E event) throws SQLException, ErrorResponseException {
    SQLQuery query = super.buildQuery(event);

    SQLQuery searchQuery = buildSearchFragment(event);
    if (hasSearch)
      query.setQueryFragment("filterWhereClause", searchQuery);
    else
      query.setQueryFragment("filterWhereClause", "TRUE");

    query.setQueryFragment("limit", buildLimitFragment(event.getLimit()));
    return query;
  }

  protected SQLQuery buildSearchFragment(E event) {
    final SQLQuery searchQuery = SearchForFeatures.generateSearchQuery(event);
    hasSearch = searchQuery != null;
    return searchQuery;
  }

  protected static SQLQuery buildLimitFragment(long limit) {
    return new SQLQuery("LIMIT #{limit}").withNamedParameter("limit", limit);
  }


  /**
   * @deprecated Please use {@link SearchForFeatures#generatePropertiesQuery(SearchForFeaturesEvent)} instead.
   */
  //TODO: Can be removed after completion of refactoring
  @Deprecated
  protected static SQLQuery generatePropertiesQueryBWC(SearchForFeaturesEvent event) {
    SQLQuery query = generatePropertiesQuery(event);
    if (query != null)
      query.replaceNamedParameters();
    return query;
  }

  /**
   * Determines if PropertiesQuery can be executed. Check if required Indices are created.
   */
  private static List<String> sortableCanSearchForIndex( List<String> indices )
  { if( indices == null ) return null;
    List<String> skeys = new ArrayList<String>();
    for( String k : indices)
      if( k.startsWith("o:") )
        skeys.add( k.replaceFirst("^o:([^,]+).*$", "$1") );

    return skeys;
  }

  private static boolean canSearchFor(String tableName, PropertiesQuery query, PSQLXyzConnector dbHandler) {
    if (query == null) {
      return true;
    }

    try {
      List<String> keys = query.stream().flatMap(List::stream)
          .filter(k -> k.getKey() != null && k.getKey().length() > 0).map(PropertyQuery::getKey).collect(Collectors.toList());

      int idx_check = 0;

      for (String key : keys) {

        /** properties.foo vs foo (root)
         * If hub receives "f.foo=bar&p.foo=bar" it will generates a PropertyQuery with properties.foo=bar and foo=bar
         **/
        boolean isPropertyQuery = key.startsWith("properties.");

        /** If property query hits default system index - allow search. [id, properties.@ns:com:here:xyz.createdAt, properties.@ns:com:here:xyz.updatedAt]" */
        if (     key.equals("id")
            ||  key.equals("properties.@ns:com:here:xyz.createdAt")
            ||  key.equals("properties.@ns:com:here:xyz.updatedAt")
        )
          return true;

        /** Check if custom Indices are available. Eg.: properties.foo1&f.foo2*/
        List<String> indices = new GetIndexList(tableName).run(dbHandler.getDataSourceProvider());

        /** The table has not many records - Indices are not required */
        if (indices == null) {
          return true;
        }

        List<String> sindices = sortableCanSearchForIndex( indices );
        /** If it is a root property query "foo=bar" we extend the suffix "f."
         *  If it is a property query "properties.foo=bar" we remove the suffix "properties." */
        String searchKey = isPropertyQuery ? key.substring("properties.".length()) : "f."+key;

        if (indices.contains( searchKey ) || (sindices != null && sindices.contains(searchKey)) ) {
          /** Check if all properties are indexed */
          idx_check++;
        }
      }

      if(idx_check == keys.size())
        return true;

      return new GetIndexList(tableName).run(dbHandler.getDataSourceProvider()) == null;
    } catch (Exception e) {
      // In all cases, when something with the check went wrong, allow the search
      return true;
    }
  }

  private static String getParamNameForValue(Map<String, Integer> countingMap, String key) {
    Integer counter = countingMap.get(key);
    countingMap.put(key, counter == null ? 1 : ++counter);
    return "userValue_" + key + (counter == null ? "" : "" + counter);
  }

  private static SQLQuery generatePropertiesQuery(SearchForFeaturesEvent event) {
    PropertiesQuery properties = event.getPropertiesQuery();
    if (properties == null || properties.size() == 0) {
      return null;
    }
    // TODO: This is only a hot-fix for the connector. The issue is caused in the service and the code below will be removed after the next XYZ Hub deployment
    if (properties.get(0).size() == 0 || properties.get(0).size() == 1 && properties.get(0).get(0) == null) {
      return null;
    }

    HashMap<String, Integer> countingMap = new HashMap<>();
    Map<String, Object> namedParams = new HashMap<>();
    // List with the outer OR combined statements
    List<SQLQuery> disjunctionQueries = new ArrayList<>();
    properties.forEach(conjunctions -> {

      // List with the AND combined statements
      final List<SQLQuery> conjunctionQueries = new ArrayList<>();
      conjunctions.forEach(propertyQuery -> {

        // List with OR combined statements for one property key
        final List<SQLQuery> keyDisjunctionQueries = new ArrayList<>();
        int valuesCount = propertyQuery.getValues().size();
        for (int i = 0; i < valuesCount; i++) {
          Object v = propertyQuery.getValues().get(i);

          final String psqlOperation;
          PropertyQuery.QueryOperation op = propertyQuery.getOperation();

          String  key = propertyQuery.getKey(),
              paramName = getParamNameForValue(countingMap, key),
              value = getValue(v, op, key, paramName);
          SQLQuery q = createKey(key);
          namedParams.putAll(q.getNamedParameters());

          if(v == null){
            //Overrides all operations e.g: p.foo=lte=.null => p.foo=.null
            //>> [not] (jsondata->?->? is not null and jsondata->?->? != 'null'::jsonb )
            SQLQuery q1 = new SQLQuery( op.equals(PropertyQuery.QueryOperation.NOT_EQUALS) ? "((" : "not ((" );
            q1.append(q);
            q1.append("is not null");

            if(! ("id".equals(key) || "geometry.type".equals(key)) )
            { q1.append("and"); q1.append(q); q1.append("!= 'null'::jsonb" ); }

            q1.append("))");
            q = q1;
          }else{
            psqlOperation = SQLQuery.getOperation(op);
            q.append(new SQLQuery(psqlOperation + (value == null ? "" : value)));
            namedParams.put(paramName, v);
          }
          keyDisjunctionQueries.add(q);
        }
        conjunctionQueries.add(SQLQuery.join(keyDisjunctionQueries, " OR ", true));
      });
      disjunctionQueries.add(SQLQuery.join(conjunctionQueries, " AND ", false));
    });
    SQLQuery query = SQLQuery.join(disjunctionQueries, " OR ", false);
    query.setNamedParameters(namedParams);
    return query;
  }

  private static SQLQuery joinQueries(List<SQLQuery> queries, String delimiter, boolean encloseInBrackets) {
    List<String> fragmentPlaceholders = new ArrayList<>();
    Map<String, SQLQuery> queryFragments = new HashMap<>();
    int i = 0;
    for (SQLQuery q : queries) {
      String fragmentName = "f" + i++;
      String fragmentPlaceholder = "${{" + fragmentName + "}}";
      if (encloseInBrackets)
        fragmentPlaceholder = "(" + fragmentPlaceholder + ")";
      fragmentPlaceholders.add(fragmentPlaceholder);
      queryFragments.put(fragmentName, q);
    }

    SQLQuery joinedQuery = new SQLQuery(String.join(delimiter, fragmentPlaceholders));
    for (Entry<String, SQLQuery> queryFragment : queryFragments.entrySet())
      joinedQuery.setQueryFragment(queryFragment.getKey(), queryFragment.getValue());
    return joinedQuery;
  }

  private static SQLQuery generateTagsQuery(TagsQuery tagsQuery) {
    if (tagsQuery == null || tagsQuery.size() == 0)
      return null;

    SQLQuery query = new SQLQuery("(");
    for (int i = 0; i < tagsQuery.size(); i++) {
      query.append((i == 0 ? "" : " OR ") + "jsondata->'properties'->'@ns:com:here:xyz'->'tags' ??& #{tags" + i + "}");
      query.setNamedParameter("tags" + i, tagsQuery.get(i).toArray(new String[0]));
    }
    query.append(")");

    return query;
  }

  /**
   * @deprecated Please use {@link #generateSearchQuery(SearchForFeaturesEvent)} instead.
   */
  //TODO: Can be removed after completion of refactoring
  @Deprecated
  public static SQLQuery generateSearchQueryBWC(SearchForFeaturesEvent event) {
    SQLQuery query = generateSearchQuery(event);
    if (query != null)
      query.replaceNamedParameters();
    return query;
  }

  protected static SQLQuery generateSearchQuery(final SearchForFeaturesEvent event) {
    final SQLQuery propertiesQuery = generatePropertiesQuery(event);
    final SQLQuery tagsQuery = generateTagsQuery(event.getTags());

    SQLQuery query = new SQLQuery("");
    if (propertiesQuery != null) {
      query.append("${{propertiesQuery}}");
      query.setQueryFragment("propertiesQuery", propertiesQuery);
    }
    if (tagsQuery != null) {
      query.append ((propertiesQuery != null ? " AND " : "") + "${{tagsQuery}}");
      query.setQueryFragment("tagsQuery", tagsQuery);
    }

    query.replaceFragments();
    if (query.text().isEmpty())
      return null;
    return query;
  }

  private static SQLQuery createKey(String key) {
    String[] keySegments = key.split("\\.");

    /** ID is indexed as text */
    if(keySegments.length == 1 && keySegments[0].equalsIgnoreCase("id")) {
      return new SQLQuery("id");
    }

    /** special handling on geometry column */
    if(keySegments.length == 2 && keySegments[0].equalsIgnoreCase("geometry") && keySegments[1].equalsIgnoreCase("type")) {
      return new SQLQuery("GeometryType(geo) ");
    }

    Map<String, Object> segmentNames = new HashMap<>();
    String keyPath = "jsondata";
    for (String keySegment : keySegments) {
      String segmentParamName = "keySegment_" + keySegment;
      keyPath += "->#{" + segmentParamName + "}";
      segmentNames.put(segmentParamName, keySegment);
    }

    return new SQLQuery(keyPath).withNamedParameters(segmentNames);
  }

  private static String getValue(Object value, PropertyQuery.QueryOperation op, String key, String paramName) {
    String param = "#{" + paramName + "}";

    if (key.equalsIgnoreCase("geometry.type"))
      return "upper(" + param + "::text)";

    if (value == null)
      return null;

    /** The ID is indexed as text */
    if (key.equalsIgnoreCase("id"))
      return param + "::text";

    if (value instanceof String) {
      if (op.equals(PropertyQuery.QueryOperation.CONTAINS) && ((String) value).startsWith("{") && ((String) value).endsWith("}"))
        return "(" + param + "::jsonb || '[]'::jsonb)";
      return "to_jsonb(" + param + "::text)";
    }
    if (value instanceof Number) {
      return "to_jsonb(" + param + "::numeric)";
    }
    if (value instanceof Boolean) {
      return "to_jsonb(" + param + "::boolean)";
    }
    return "";
  }

}
