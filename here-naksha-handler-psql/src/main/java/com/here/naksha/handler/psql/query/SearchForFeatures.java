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

package com.here.naksha.handler.psql.query;

import com.here.naksha.handler.psql.PsqlHandler;
import com.here.naksha.lib.core.models.payload.events.PropertyQueryOr;
import com.here.naksha.lib.core.models.payload.events.QueryOperation;
import com.here.naksha.lib.core.models.payload.events.TagsQuery;
import com.here.naksha.lib.core.models.payload.events.feature.QueryEvent;
import com.here.naksha.lib.core.models.payload.events.feature.SearchForFeaturesEvent;
import com.here.naksha.lib.psql.sql.SQLQuery;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

/*
NOTE: All subclasses of QueryEvent are deprecated except SearchForFeaturesEvent.
Once refactoring is complete, all members of SearchForFeaturesEvent can be pulled up to QueryEvent and QueryEvent
can be renamed to SearchForFeaturesEvent again.
 */
public class SearchForFeatures<E extends SearchForFeaturesEvent> extends GetFeatures<E> {

    protected boolean hasSearch;

    public SearchForFeatures(@NotNull E event, @NotNull PsqlHandler psqlConnector) throws SQLException {
        super(event, psqlConnector);
    }

    @Override
    protected @NotNull SQLQuery buildQuery(@NotNull E event) throws SQLException {
        SQLQuery query = super.buildQuery(event);

        SQLQuery searchQuery = buildSearchFragment(event);
        if (hasSearch) query.setQueryFragment("filterWhereClause", searchQuery);
        else query.setQueryFragment("filterWhereClause", "TRUE");

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

    // TODO: Can be removed after completion of refactoring
    @Deprecated
    public static SQLQuery generatePropertiesQueryBWC(PropertyQueryOr properties) {
        SQLQuery query = generatePropertiesQuery(properties);
        if (query != null) query.replaceNamedParameters();
        return query;
    }

    private static String getParamNameForValue(Map<String, Integer> countingMap, String key) {
        Integer counter = countingMap.get(key);
        countingMap.put(key, counter == null ? 1 : ++counter);
        return "userValue_" + key + (counter == null ? "" : "" + counter);
    }

    private static SQLQuery generatePropertiesQuery(PropertyQueryOr properties) {
        if (properties == null || properties.size() == 0) {
            return null;
        }
        // TODO: This is only a hot-fix for the connector. The issue is caused in the service and the
        // code below will be removed after the next XYZ Hub deployment
        if (properties.get(0).size() == 0
                || properties.get(0).size() == 1 && properties.get(0).get(0) == null) {
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
                    QueryOperation op = propertyQuery.getOperation();

                    String key = propertyQuery.getKey(),
                            paramName = getParamNameForValue(countingMap, key),
                            value = getValue(v, op, key, paramName);
                    SQLQuery q = createKey(key);
                    namedParams.putAll(q.getNamedParameters());

                    if (v == null) {
                        // Overrides all operations e.g: p.foo=lte=.null => p.foo=.null
                        // >> [not] (jsondata->?->? is not null and jsondata->?->? != 'null'::jsonb )
                        SQLQuery q1 = new SQLQuery(op.equals(QueryOperation.NOT_EQUALS) ? "((" : "not ((");
                        q1.append(q);
                        q1.append("is not null");

                        if (!("id".equals(key) || "geometry.type".equals(key))) {
                            q1.append("and");
                            q1.append(q);
                            q1.append("!= 'null'::jsonb");
                        }

                        q1.append("))");
                        q = q1;
                    } else {
                        psqlOperation = SQLQuery.getOperation(op);
                        q.append(new SQLQuery(psqlOperation + (value == null ? "" : value)));
                        namedParams.put(paramName, v);
                    }
                    keyDisjunctionQueries.add(q);
                }
                conjunctionQueries.add(SQLQuery.join(keyDisjunctionQueries, "OR", true));
            });
            disjunctionQueries.add(SQLQuery.join(conjunctionQueries, "AND", false));
        });
        SQLQuery query = SQLQuery.join(disjunctionQueries, "OR", false);
        query.setNamedParameters(namedParams);
        return query;
    }

    private static SQLQuery generateTagsQuery(TagsQuery tagsQuery) {
        if (tagsQuery == null || tagsQuery.size() == 0) return null;

        SQLQuery query = new SQLQuery("(");
        for (int i = 0; i < tagsQuery.size(); i++) {
            query.append(
                    (i == 0 ? "" : " OR ") + "jsondata->'properties'->'@ns:com:here:xyz'->'tags' ??& #{tags" + i + "}");
            query.setNamedParameter("tags" + i, tagsQuery.get(i).toArray(new String[0]));
        }
        query.append(")");

        return query;
    }

    // TODO: Can be removed after completion of refactoring
    @Deprecated
    public static SQLQuery generateSearchQueryBWC(QueryEvent event) {
        SQLQuery query = generateSearchQuery(event);
        if (query != null) query.replaceNamedParameters();
        return query;
    }

    protected static SQLQuery generateSearchQuery(final QueryEvent event) { // TODO: Make private again
        final SQLQuery propertiesQuery = generatePropertiesQuery(event.getPropertiesQuery());
        final SQLQuery tagsQuery = generateTagsQuery(event.getTags());

        SQLQuery query = new SQLQuery("");
        if (propertiesQuery != null) {
            query.append("${{propertiesQuery}}");
            query.setQueryFragment("propertiesQuery", propertiesQuery);
        }
        if (tagsQuery != null) {
            query.append((propertiesQuery != null ? " AND " : "") + "${{tagsQuery}}");
            query.setQueryFragment("tagsQuery", tagsQuery);
        }

        query.replaceFragments();
        if (query.text().isEmpty()) return null;
        return query;
    }

    private static SQLQuery createKey(String key) {
        String[] keySegments = key.split("\\.");

        /** ID is indexed as text */
        if (keySegments.length == 1 && keySegments[0].equalsIgnoreCase("id")) {
            return new SQLQuery("jsondata->>'id'");
        }

        /** special handling on geometry column */
        if (keySegments.length == 2
                && keySegments[0].equalsIgnoreCase("geometry")
                && keySegments[1].equalsIgnoreCase("type")) {
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

    private static String getValue(Object value, QueryOperation op, String key, String paramName) {
        String param = "#{" + paramName + "}";

        if (key.equalsIgnoreCase("geometry.type")) return "upper(" + param + "::text)";

        if (value == null) return null;

        /** The ID is indexed as text */
        if (key.equalsIgnoreCase("id")) return param + "::text";

        if (value instanceof String) {
            if (op.equals(QueryOperation.CONTAINS)
                    && ((String) value).startsWith("{")
                    && ((String) value).endsWith("}")) return "(" + param + "::jsonb || '[]'::jsonb)";
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
