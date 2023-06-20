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


import com.here.mapcreator.ext.naksha.sql.SQLQuery;
import com.here.xyz.models.payload.events.feature.LoadFeaturesEvent;
import com.here.xyz.psql.PsqlHandler;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

public class LoadFeatures extends GetFeatures<LoadFeaturesEvent> {

  public LoadFeatures(@NotNull LoadFeaturesEvent event, @NotNull PsqlHandler psqlConnector)
      throws SQLException {
    super(event, psqlConnector);
  }

  @Override
  protected @NotNull SQLQuery buildQuery(@Nonnull LoadFeaturesEvent event) throws SQLException {
    final Map<String, String> idMap = event.getIdsMap();

    SQLQuery filterWhereClause =
        new SQLQuery("jsondata->>'id' = ANY(#{ids})")
            .withNamedParameter("ids", idMap.keySet().toArray(new String[0]));

    SQLQuery headQuery =
        super.buildQuery(event).withQueryFragment("filterWhereClause", filterWhereClause);

    SQLQuery query = headQuery;
    if (
    /*event.getEnableHistory() &&*/ (!isExtendedSpace(event))) {
      final boolean compactHistory = /*!event.getEnableGlobalVersioning() &&*/
          processor.connectorParams().isCompactHistory();
      if (compactHistory)
        // History does not contain Inserts
        query = new SQLQuery("${{headQuery}} UNION ${{historyQuery}}");
      else
        // History does contain Inserts
        query =
            new SQLQuery(
                "SELECT DISTINCT ON(jsondata->'properties'->'@ns:com:here:xyz'->'uuid') * FROM("
                    + "    ${{headQuery}} UNION ${{historyQuery}}"
                    + ")A");

      query
          .withQueryFragment("headQuery", headQuery)
          .withQueryFragment("historyQuery", buildHistoryQuery(event, idMap.values()));
    }

    return query;
  }

  private SQLQuery buildHistoryQuery(LoadFeaturesEvent event, Collection<String> uuids) {
    SQLQuery historyQuery =
        new SQLQuery(
            "SELECT jsondata, ${{geo}} "
                + "FROM ${schema}.${hsttable} h "
                + "WHERE uuid = ANY(#{uuids}) AND EXISTS("
                + "    SELECT 1"
                + "    FROM ${schema}.${table} t"
                + "    WHERE t.jsondata->>'id' =  h.jsondata->>'id'"
                + ")");
    historyQuery.setQueryFragment("geo", buildGeoFragment(event));
    historyQuery.setNamedParameter("uuids", uuids.toArray(new String[0]));
    historyQuery.setVariable("hsttable", processor.spaceHistoryTable());
    return historyQuery;
  }
}
