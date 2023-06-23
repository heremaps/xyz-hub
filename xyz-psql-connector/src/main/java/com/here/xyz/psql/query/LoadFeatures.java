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
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LoadFeatures extends GetFeatures<LoadFeaturesEvent, FeatureCollection> {

  public LoadFeatures(LoadFeaturesEvent event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);
    setUseReadReplica(false);
  }

  @Override
  protected SQLQuery buildQuery(LoadFeaturesEvent event) throws SQLException, ErrorResponseException {
    return event.getVersionsToKeep() > 1 ? buildQueryForV2K2(event) : buildQueryForV2K1(event);
  }

  private SQLQuery buildQueryForV2K1(LoadFeaturesEvent event) throws SQLException, ErrorResponseException {
    final Map<String, String> idMap = event.getIdsMap();

    SQLQuery filterWhereClause = new SQLQuery("id = ANY(#{ids})")
        .withNamedParameter("ids", idMap.keySet().toArray(new String[0]));

    SQLQuery headQuery = super.buildQuery(event)
        .withQueryFragment("filterWhereClause", filterWhereClause);

    if (event.getEnableHistory() && (!isExtendedSpace(event) || event.getContext() != DEFAULT)) {
      final boolean compactHistory = !event.getEnableGlobalVersioning() && dbHandler.getConfig().getConnectorParams().isCompactHistory();
      SQLQuery query;
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

    return headQuery;
  }

  private SQLQuery buildQueryForV2K2(LoadFeaturesEvent event) throws SQLException, ErrorResponseException {
    final Map<String, String> idMap = event.getIdsMap();
    Set<String> idsOnly = new HashSet<>();
    Map<String, String> idsWithVersions = new HashMap<>();

    idMap.forEach((k,v) -> {
      idsOnly.add(k);
      if (v != null)
        idsWithVersions.put(k, v);
    });

    SQLQuery filterWhereClauseHead = new SQLQuery("(id, next_version) IN (${{loadFeaturesInputIdsOnly}})")
        .withQueryFragment("loadFeaturesInputIdsOnly", buildLoadFeaturesInputFragment(idsOnly.toArray(new String[0])));

    SQLQuery filterWhereClauseHistory = new SQLQuery("(id, version) IN (${{loadFeaturesInput}})")
        .withQueryFragment("loadFeaturesInput", buildLoadFeaturesInputFragment(
            idsWithVersions.keySet().toArray(new String[0]),
            idsWithVersions.values().stream().map(Long::parseLong).toArray(Long[]::new)
        ));

    return new SQLQuery("${{versioningHead}} UNION ${{versioningHistory}}")
        .withQueryFragment("versioningHead", super.buildQuery(event).withQueryFragment("filterWhereClause", filterWhereClauseHead))
        .withQueryFragment("versioningHistory", super.buildQuery(event).withQueryFragment("filterWhereClause", filterWhereClauseHistory));

  }

  private static SQLQuery buildLoadFeaturesInputFragment(String[] ids) {
    return buildLoadFeaturesInputFragment(ids, null);
  }

  private static SQLQuery buildLoadFeaturesInputFragment(String[] ids, Long[] versions) {
    String idsParamName = "ids" + (versions != null ? "WithVersions" : ""); //TODO: That's a workaround for a minor bug in SQLQuery
    SQLQuery inputFragment = new SQLQuery("WITH "
        + "  recs AS ( "
        + "    SELECT unnest(transform_load_features_input(#{" + idsParamName + "}${{versions}})) as rec) "
        + "SELECT "
        + "  (rec).id, "
        + "  (rec).version "
        + "FROM recs")
        .withNamedParameter(idsParamName, ids)
        .withQueryFragment("versions", versions == null ? "" : ", #{versions}");
     if (versions != null)
        inputFragment.setNamedParameter("versions", versions);
     return inputFragment;
  }

  @Deprecated
  private SQLQuery buildHistoryQuery(LoadFeaturesEvent event, Collection<String> uuids) {
    return new SQLQuery("SELECT jsondata, ${{geo}}, jsondata->>'id' as id "
        + "FROM ${schema}.${hsttable} h "
        + "WHERE uuid = ANY(#{uuids}) AND EXISTS("
        + "    SELECT 1"
        + "    FROM ${schema}.${table} t"
        + "    WHERE t.id =  h.jsondata->>'id'"
        + ")")
        .withQueryFragment("geo", buildGeoFragment(event))
        .withNamedParameter("uuids", uuids.toArray(new String[0]))
        .withVariable("hsttable", getDefaultTable(event) + DatabaseHandler.HISTORY_TABLE_SUFFIX);
  }
}
