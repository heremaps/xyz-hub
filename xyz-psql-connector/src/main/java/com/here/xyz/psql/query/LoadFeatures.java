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

package com.here.xyz.psql.query;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class LoadFeatures extends GetFeatures<LoadFeaturesEvent> {

  public LoadFeatures(LoadFeaturesEvent event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);
  }

  @Override
  protected SQLQuery buildQuery(LoadFeaturesEvent event) throws SQLException {
    final Map<String, String> idMap = event.getIdsMap();

    SQLQuery filterWhereClause = new SQLQuery("${{idColumn}} = ANY(#{ids})")
        .withNamedParameter("ids", idMap.keySet().toArray(new String[0]))
        .withQueryFragment("idColumn", buildIdFragment(event));

    if (event.getVersionsToKeep() > 1) {
      filterWhereClause = new SQLQuery("(id, version) IN (${{loadFeaturesInput}})")
          .withQueryFragment("loadFeaturesInput", buildLoadFeaturesInputFragment())
          .withNamedParameter("ids", idMap.keySet().toArray(new String[0]))
          .withNamedParameter("versions", idMap.values().stream().map(v -> v == null ? Long.MAX_VALUE : Long.parseLong(v)).collect(
              Collectors.toList()).toArray(new Long[0]));
    }

    SQLQuery headQuery = super.buildQuery(event)
        .withQueryFragment("filterWhereClause", filterWhereClause);

    if (event.getVersionsToKeep() <= 1) {
      SQLQuery query = headQuery;

      if (event.getEnableHistory() && (!isExtendedSpace(event) || event.getContext() != DEFAULT)) {
        final boolean compactHistory = !event.getEnableGlobalVersioning() && dbHandler.getConfig().getConnectorParams().isCompactHistory();
        if (compactHistory)
          //History does not contain Inserts
          query = new SQLQuery("${{headQuery}} UNION ${{historyQuery}}");
        else
          //History does contain Inserts
          query = new SQLQuery("SELECT DISTINCT ON(jsondata->'properties'->'@ns:com:here:xyz'->'uuid') * FROM("
              + "    ${{headQuery}} UNION ${{historyQuery}}"
              + ")A");

        return query
            .withQueryFragment("headQuery", headQuery)
            .withQueryFragment("historyQuery", buildHistoryQuery(event, idMap.values()));
      }
    }

    return headQuery;
  }

  private static String buildLoadFeaturesInputFragment() {
    return "WITH "
        + "  recs AS ( "
        + "    SELECT unnest(transform_load_features_input(#{ids}, #{versions})) as rec) "
        + "SELECT "
        + "  (rec).id, "
        + "  (rec).version "
        + "FROM recs";
  }

  private SQLQuery buildHistoryQuery(LoadFeaturesEvent event, Collection<String> uuids) {
    SQLQuery historyQuery = new SQLQuery("SELECT jsondata, ${{geo}} "
        + "FROM ${schema}.${hsttable} h "
        + "WHERE uuid = ANY(#{uuids}) AND EXISTS("
        + "    SELECT 1"
        + "    FROM ${schema}.${table} t"
        + "    WHERE t.${{idColumn}} =  h.jsondata->>'id'"
        + ")");
    historyQuery.setQueryFragment("geo", buildGeoFragment(event));
    historyQuery.withQueryFragment("idColumn", buildIdFragment(event));
    historyQuery.setNamedParameter("uuids", uuids.toArray(new String[0]));
    historyQuery.setVariable("hsttable", getDefaultTable(event) + DatabaseHandler.HISTORY_TABLE_SUFFIX);
    return historyQuery;
  }
}
