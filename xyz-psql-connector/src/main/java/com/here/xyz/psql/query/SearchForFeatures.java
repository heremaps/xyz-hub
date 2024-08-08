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

package com.here.xyz.psql.query;

import static com.here.xyz.events.PropertyQuery.QueryOperation.CONTAINS;
import static com.here.xyz.events.PropertyQuery.QueryOperation.NOT_EQUALS;
import static com.here.xyz.responses.XyzError.ILLEGAL_ARGUMENT;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.psql.query.helpers.GetIndexList;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
NOTE: All subclasses of QueryEvent are deprecated except SearchForFeaturesEvent.
Once refactoring is complete, all members of SearchForFeaturesEvent can be pulled up to QueryEvent and QueryEvent
can be renamed to SearchForFeaturesEvent again.
 */
public class SearchForFeatures<E extends SearchForFeaturesEvent, R extends XyzResponse> extends GetFeatures<E, R> {
  protected boolean hasSearch;
  private SearchForFeaturesEvent tmpEvent; //TODO: Remove after refactoring

  public SearchForFeatures(E event) throws SQLException, ErrorResponseException {
    super(event);
    tmpEvent = event;
    //hasSearch = (event.getPropertiesQuery() == null || event.getPropertiesQuery().size() == 0)
    //    && (event.getTags() == null || event.getTags().size() == 0);
  }

  @Override
  protected R run(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {
    if (tmpEvent.getClass() == SearchForFeaturesEvent.class)
      checkCanSearchFor(tmpEvent);
    return super.run(dataSourceProvider);
  }

  protected void checkCanSearchFor(SearchForFeaturesEvent event) throws ErrorResponseException {
    if (!canSearchFor(event))
      throw new ErrorResponseException(ILLEGAL_ARGUMENT,
          "Invalid request parameters. Search for the provided properties is not supported for this space.");
  }

  @Override
  protected SQLQuery buildFilterWhereClause(E event) {
    SQLQuery searchQuery = buildSearchFragment(event);

    if (searchQuery != null)
      return searchQuery;

    return super.buildFilterWhereClause(event);
  }

  protected SQLQuery buildSearchFragment(E event) {
    final SQLQuery searchQuery = generateSearchQuery(event);
    hasSearch = searchQuery != null;
    return searchQuery;
  }

  @Override
  protected SQLQuery buildLimitFragment(E event) {
    return new SQLQuery("LIMIT #{limit}").withNamedParameter("limit", event.getLimit());
  }

  /**
   * Determines if PropertiesQuery can be executed. Check if required Indices are created.
   */
  private static List<String> sortableCanSearchForIndex( List<String> indices )
  { if( indices == null ) return null;
    List<String> skeys = new ArrayList<>();
    for( String k : indices)
      if( k.startsWith("o:") )
        skeys.add( k.replaceFirst("^o:([^,]+).*$", "$1") );

    return skeys;
  }

  private boolean canSearchFor(SearchForFeaturesEvent event) {
    DataSourceProvider dataSourceProvider = getDataSourceProvider();
    String tableName = XyzEventBasedQueryRunner.readTableFromEvent(event);
    PropertiesQuery query = event.getPropertiesQuery();

    if (query == null)
      return true;

    try {
      List<String> keys = query.stream().flatMap(List::stream)
          .filter(k -> k.getKey() != null && k.getKey().length() > 0).map(PropertyQuery::getKey).collect(Collectors.toList());

      int idx_check = 0;

      //NOTE: All keys are always full qualified property-"paths" (dot-separated)
      for (String key : keys) {
        boolean isPropertyQuery = key.startsWith("properties.");

        //If the property query hits a default existing system index - allow the search
        if (key.equals("id"))
          return true;

        //Check if custom Indices are available. E.g., properties.foo1
        List<String> indices = new GetIndexList(tableName).run(dataSourceProvider);

        //The table does not have too many records - Indices are not required
        if (indices == null)
          return true;

        List<String> sindices = sortableCanSearchForIndex( indices );
        //If it is a root property query "foo=bar" we extend the suffix "f."
        //If it is a property query "properties.foo=bar" we remove the suffix "properties."
        //TODO: That seems to be a weired hack. Check why that is needed and remove if possible
        String searchKey = isPropertyQuery ? key.substring("properties.".length()) : "f." + key;

        if (indices.contains(searchKey) || sindices != null && sindices.contains(searchKey))
          //Check if all properties are indexed
          idx_check++;
      }

      if (idx_check == keys.size())
        return true;

      //TODO: Why is that 2nd extra call needed? Could we remove it?
      return new GetIndexList(tableName).run(dataSourceProvider) == null;
    }
    catch (Exception e) {
      // In all cases, when something with the check went wrong, allow the search
      return true;
    }
  }

  private static String getParamNameForValue(Map<String, Integer> countingMap, String key) {
    Integer counter = countingMap.get(key);
    countingMap.put(key, counter == null ? 1 : ++counter);
    return "userValue_" + key + (counter == null ? "" : "" + counter);
  }

  protected static SQLQuery generatePropertiesQuery(SearchForFeaturesEvent event) {
    PropertiesQuery properties = event.getPropertiesQuery();
    if (properties == null || properties.size() == 0)
      return null;

    // TODO: This is only a hot-fix for the connector. The issue is caused in the service and the code below will be removed after the next XYZ Hub deployment
    if (properties.get(0).size() == 0 || properties.get(0).size() == 1 && properties.get(0).get(0) == null)
      return null;

    HashMap<String, Integer> countingMap = new HashMap<>();
    Map<String, Object> namedParams = new HashMap<>();
    //List with the outer OR combined statements
    List<SQLQuery> disjunctionQueries = new ArrayList<>();
    properties.forEach(conjunctions -> {

      //List with the AND combined statements
      final List<SQLQuery> conjunctionQueries = new ArrayList<>();
      conjunctions.forEach(propertyQuery -> {

        //List with OR combined statements for one property key
        final List<SQLQuery> keyDisjunctionQueries = new ArrayList<>();
        int valuesCount = propertyQuery.getValues().size();
        for (int i = 0; i < valuesCount; i++) {
          Object v = propertyQuery.getValues().get(i);
          QueryOperation op = propertyQuery.getOperation();
          String key = propertyQuery.getKey(),
              paramName = getParamNameForValue(countingMap, key),
              value = getValue(v, op, key, paramName);
          SQLQuery keyPath = createKey(key);
          SQLQuery predicateQuery;
          namedParams.putAll(keyPath.getNamedParameters());

          if (v == null) {
            //Overrides all operations e.g: p.foo=lte=.null => p.foo=.null
            //>> [not] (jsondata->?->? is not null and jsondata->?->? != 'null'::jsonb )
            predicateQuery = new SQLQuery("${{not}} ((${{keyPath}} IS NOT NULL ${{notJsonbComparison}}))")
                .withQueryFragment("not", op == NOT_EQUALS ? "" : "not")
                .withQueryFragment("keyPath", keyPath)
                .withQueryFragment("notJsonbComparison",
                    "id".equals(key) || "geometry.type".equals(key) ? "" : "AND ${{keyPath}} != 'null'::jsonb");
          }
          else {
            predicateQuery = new SQLQuery("${{keyPath}} ${{operation}} ${{value}}")
                .withQueryFragment("keyPath", keyPath)
                .withQueryFragment("operation", QueryOperation.getOutputRepresentation(op))
                .withQueryFragment("value", value == null ? "" : value);
            namedParams.put(paramName, v);
          }
          keyDisjunctionQueries.add(predicateQuery);
        }
        conjunctionQueries.add(SQLQuery.join(keyDisjunctionQueries, " OR ", true));
      });
      disjunctionQueries.add(SQLQuery.join(conjunctionQueries, " AND ", false));
    });
    return SQLQuery.join(disjunctionQueries, " OR ", false)
        .withNamedParameters(namedParams);
  }

  protected static SQLQuery generateSearchQuery(final SearchForFeaturesEvent event) {
    return generatePropertiesQuery(event);
  }

  private static SQLQuery createKey(String key) {
    String[] keySegments = key.split("\\.");

    //ID is indexed as text
    if (keySegments.length == 1 && keySegments[0].equalsIgnoreCase("id"))
      return new SQLQuery("id");

    //Special handling on geometry column
    if (keySegments.length == 2 && keySegments[0].equalsIgnoreCase("geometry") && keySegments[1].equalsIgnoreCase("type"))
      return new SQLQuery("GeometryType(geo) ");

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
      return "upper(" + param + "::TEXT)";

    if (value == null)
      return null;

    //The ID is indexed as text
    if (key.equalsIgnoreCase("id"))
      return param + "::TEXT";

    if (value instanceof String) {
      if (op == CONTAINS && ((String) value).startsWith("{") && ((String) value).endsWith("}"))
        return "(" + param + "::JSONB || '[]'::JSONB)";
      return "to_jsonb(" + param + "::TEXT)";
    }

    if (value instanceof Number)
      return "to_jsonb(" + param + "::NUMERIC)";

    if (value instanceof Boolean)
      return "to_jsonb(" + param + "::BOOLEAN)";

    return "";
  }

}
