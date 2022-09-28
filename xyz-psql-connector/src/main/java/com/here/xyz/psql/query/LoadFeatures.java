/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

public class LoadFeatures extends GetFeatures<LoadFeaturesEvent> {

  public LoadFeatures(LoadFeaturesEvent event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);
  }

  @Override
  protected SQLQuery buildQuery(LoadFeaturesEvent event) {
    final Map<String, String> idMap = event.getIdsMap();

    String filterWhereClause = "jsondata->>'id' = ANY(#{ids})";
    SQLQuery query;

    if (isExtendedSpace(event) && event.getContext() == DEFAULT || !event.getEnableHistory() || idMap.size() == 0)
      query = buildQuery(event, filterWhereClause);
    else {
      final boolean compactHistory = !event.getEnableGlobalVersioning() && dbHandler.getConfig().getConnectorParams().isCompactHistory();
      if (compactHistory)
        //History does not contain Inserts
        query = new SQLQuery("${{headQuery}} UNION ${{historyQuery}}");
      else
        //History does contain Inserts
        query = new SQLQuery("SELECT DISTINCT ON(jsondata->'properties'->'@ns:com:here:xyz'->'uuid') * FROM("
            + "    ${{headQuery}} UNION ${{historyQuery}}"
            + ")A");

      query.setQueryFragment("headQuery", buildQuery(event, filterWhereClause));
      query.setQueryFragment("historyQuery", buildHistoryQuery(event, idMap.values()));
    }
    //query.setQueryFragment("filterWhereClause", filterWhereClause);
    query.setNamedParameter("ids", idMap.keySet().toArray(new String[0]));
    return query;
  }

  private SQLQuery buildHistoryQuery(LoadFeaturesEvent event, Collection<String> uuids) {
    SQLQuery historyQuery = new SQLQuery("SELECT jsondata, ${{geo}} "
        + "FROM ${schema}.${hsttable} h "
        + "WHERE uuid = ANY(#{uuids}) AND EXISTS("
        + "    SELECT 1"
        + "    FROM ${schema}.${table} t"
        + "    WHERE t.jsondata->>'id' =  h.jsondata->>'id'"
        + ")");
    historyQuery.setQueryFragment("geo", buildGeoFragment(event));
    historyQuery.setNamedParameter("uuids", uuids.toArray(new String[0]));
    historyQuery.setVariable("hsttable", dbHandler.getConfig().readTableFromEvent(event) + DatabaseHandler.HISTORY_TABLE_SUFFIX);
    return historyQuery;
  }
}
