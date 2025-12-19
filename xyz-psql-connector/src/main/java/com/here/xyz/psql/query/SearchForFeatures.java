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

package com.here.xyz.psql.query;

import static com.here.xyz.events.PropertyQuery.QueryOperation.CONTAINS;
import static com.here.xyz.events.PropertyQuery.QueryOperation.NOT_EQUALS;
import static com.here.xyz.responses.XyzError.ILLEGAL_ARGUMENT;
import static com.here.xyz.util.db.ConnectorParameters.TableLayout;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.psql.query.helpers.GetIndexList;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.IndexHelper;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.here.xyz.psql.query.helpers.GetIndexList.IndexList;
import static com.here.xyz.psql.query.helpers.GetIndexList.BIG_SPACE_THRESHOLD;
import static com.here.xyz.events.PropertiesQuery.ALIAS_PREFIX;
/*
NOTE: All subclasses of QueryEvent are deprecated except SearchForFeaturesEvent.
Once refactoring is complete, all members of SearchForFeaturesEvent can be pulled up to QueryEvent and QueryEvent
can be renamed to SearchForFeaturesEvent again.
 */
public class SearchForFeatures<E extends SearchForFeaturesEvent, R extends XyzResponse> extends GetFeatures<E, R> {
  private Boolean canSearch;
  private SearchForFeaturesEvent tmpEvent; //TODO: Remove after refactoring

  public SearchForFeatures(E event) throws SQLException, ErrorResponseException {
    super(event);
    tmpEvent = event;
  }

  @Override
  protected R run(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {
    if (tmpEvent.getClass() == SearchForFeaturesEvent.class)
      checkCanSearchFor(tmpEvent);
    return super.run(dataSourceProvider);
  }

  protected void checkCanSearchFor(SearchForFeaturesEvent event) throws ErrorResponseException {
    this.canSearch = canSearchFor(event);
  }

  @Override
  protected SQLQuery buildFilterWhereClause(E event) {
    SQLQuery searchQuery = buildSearchFragment(event);

    if (searchQuery != null)
      return searchQuery;

    return super.buildFilterWhereClause(event);
  }

  protected SQLQuery buildSearchFragment(E event) {
    TableLayout tableLayout = getTableLayout();

    //Support global search if less than 10k features and no explicit searchable properties are defined
    //if we toggle to the old layout we can search directly on the jsondata column
    if(canSearch == null)
      tableLayout = TableLayout.OLD_LAYOUT;
    return generateSearchQuery(event, tableLayout);
  }

  @Override
  protected SQLQuery buildLimitFragment(E event) {
    return new SQLQuery("LIMIT #{limit}").withNamedParameter("limit", event.getLimit());
  }

  /**
   * Checks if search is possible for the given {@link SearchForFeaturesEvent}.
   * Returns true if search is allowed, false if not, or null if table has less than 10k objects.
   *
   * @param event The search event containing query parameters.
   * @return Boolean indicating if search is possible, or null if undetermined.
   */
  private Boolean canSearchFor(SearchForFeaturesEvent event) throws ErrorResponseException {
    DataSourceProvider dataSourceProvider = getDataSourceProvider();
    String tableName = XyzEventBasedQueryRunner.readTableFromEvent(event);
    PropertiesQuery query = event.getPropertiesQuery();

    if (query == null)
      return true;

    try {
      List<String> keys = query.stream().flatMap(List::stream)
          .filter(k -> k.getKey() != null && k.getKey().length() > 0).map(PropertyQuery::getKey).collect(Collectors.toList());

      boolean hasAliasKey = keys.stream().anyMatch(k -> k.startsWith(ALIAS_PREFIX));

      int idx_check = 0;

      //Check if custom Indices are available. E.g., properties.foo1
      IndexList indexList = new GetIndexList(tableName).run(dataSourceProvider);

      List<IndexHelper.OnDemandIndex> indices = indexList.getIndices();

      //The table does not have too many records - Indices are not required. Not allowed for alias searches.
      if (indexList.getCount() <= BIG_SPACE_THRESHOLD && !hasAliasKey) {
        return null;
      }

      for (String key : keys) {
        //If the property query hits a default existing system index - allow the search
        //We moved it below the index check, to be able to allow global searches on small tables without indices
        if (key.equals("id") || key.equals("geometry.type")) {
          idx_check++;
          continue;
        }

        //NewLayout idx-comments
        //  "$root:[$.root]::scalar"
        //  "$geometry.type:[$.geometry.type]::scalar"
        //  "$properties.foo1.nested:[$.properties.foo1.nested]::scalar"
        //  "$alias1:[$.properties.refQuad]::scalar"

        //OldLayout idx-comments
        //  "f.root"
        //  "f.geometry.type"
        //  "foo1.nested"
        if(!getTableLayout().hasSearchableColumn() || !key.startsWith(ALIAS_PREFIX))
          key = getBwcKey(key);

        String requiredIndex = new IndexHelper.OnDemandIndex().withPropertyPath(key).getIndexName(tableName);

        //skip properties checks for small spaces - only alias searches need indices
        if((!key.startsWith("$.") && indexList.getCount() <= BIG_SPACE_THRESHOLD))
          idx_check++;
        else {
          for (IndexHelper.OnDemandIndex idx : indices) {
            //check if alias matches
            try {
              String extractedAliasFromIdx = key.startsWith(ALIAS_PREFIX) ? idx.extractAlias() : idx.extractLogicalPropertyPath();

              //Check if alias matches (we do not have a matching index name) TODO: check!
               if (extractedAliasFromIdx.equals(key)) {
                 idx_check++;
                 continue;
              }

              //Property query uses non-alias notation - but check alias based index is present
              if(extractedAliasFromIdx.startsWith(ALIAS_PREFIX) && ! key.startsWith(ALIAS_PREFIX) && extractedAliasFromIdx.substring(1).equals(key)){
                idx_check++;
                continue;
              }
            }catch (IllegalArgumentException e) { } //ignore to be BWC

            //Check index name and compare base one expected hash
            if (idx.getIndexName().equals(requiredIndex))
              idx_check++;

            //Check for old array index notation. BWC
            if(idx.getIndexName().equals(new IndexHelper.OnDemandIndex().withPropertyPath(key+"::array").getIndexName(tableName))){
              idx_check++;
            }
          }
        }
      }
      if(idx_check != keys.size()){
        throw new ErrorResponseException(ILLEGAL_ARGUMENT,
                "Invalid request parameters. Search for the provided properties is not supported for this space.");
      }
      return true;
    }
    catch (ErrorResponseException e) {
      throw e;
    }
    catch (Exception e) {
      // In all cases, when something with the check went wrong, allow the search
      return true;
    }
  }

  private String getBwcKey(String key) {
    if (key.startsWith(ALIAS_PREFIX))
      return key;
    else if(key.startsWith("properties."))
      return key.substring("properties.".length());
    else
      return "f." + key;
  }

  private static String getParamNameForValue(Map<String, Integer> countingMap, String key) {
    Integer counter = countingMap.get(key);
    countingMap.put(key, counter == null ? 1 : ++counter);
    return "userValue_" + key + (counter == null ? "" : "" + counter);
  }

  protected static SQLQuery generatePropertiesQuery(SearchForFeaturesEvent event, TableLayout layout) {
    PropertiesQuery properties = event.getPropertiesQuery();
    if (properties == null || properties.isEmpty())
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
          SQLQuery keyPath = createKey(key, layout);
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

  protected static SQLQuery generateSearchQuery(final SearchForFeaturesEvent event, TableLayout layout) {
    return generatePropertiesQuery(event, layout);
  }

  private static SQLQuery createKey(String key, TableLayout layout) {
    String[] keySegments = key.split("\\.");

    //ID is indexed as text
    if (keySegments.length == 1 && keySegments[0].equalsIgnoreCase("id"))
      return new SQLQuery("id");

    //Special handling on geometry column
    if (keySegments.length == 2 && keySegments[0].equalsIgnoreCase("geometry") && keySegments[1].equalsIgnoreCase("type"))
      return new SQLQuery("GeometryType(geo) ");

    Map<String, Object> segmentNames = new HashMap<>();
    String keyPath = "jsondata";

    //If we have less than 10k features we allow global search. Hereto we search inside jsondata column even
    //if we have a searchable column. For alias searches we dont know the structure so we need inside the searchable column.
    //We know that the extraction got made.
    boolean searchOnAlias = key.startsWith(ALIAS_PREFIX);

    //layout with searchable column.. Reading extraction from searchable column
    if(layout.hasSearchableColumn()) {
      keyPath =  "searchable";
      key = key.startsWith(ALIAS_PREFIX) ? key.substring(1) : key;
      String segmentParamName = "keySegment_" + key;
      keyPath += "->#{" + segmentParamName + "}";
      segmentNames.put(segmentParamName, key);
      return new SQLQuery(keyPath).withNamedParameters(segmentNames);
    }

    //layout without searchable column.. Reading from jsondata column
    for (String keySegment : keySegments) {
      String segmentParamName = "keySegment_" + keySegment;
      keyPath += "->#{" + segmentParamName + "}";
      segmentNames.put(segmentParamName, keySegment);
    }

    return new SQLQuery(keyPath).withNamedParameters(segmentNames);
  }

  private static String getValue(Object value, PropertyQuery.QueryOperation op, String key, String paramName) {
    String param = "#{" + paramName + "}";

    //Geometry type needs to get compared as upper case text
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
