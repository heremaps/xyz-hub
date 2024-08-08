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

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LoadFeatures extends GetFeatures<LoadFeaturesEvent, FeatureCollection> {
  private boolean isForHistoryQuery;
  private boolean emptyRequest;

  public LoadFeatures(LoadFeaturesEvent event) throws SQLException, ErrorResponseException {
    super(event);
    setUseReadReplica(false);
    emptyRequest = event.getIdsMap() == null || event.getIdsMap().size() == 0;
  }

  @Override
  protected FeatureCollection run(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {
    return emptyRequest ? new FeatureCollection() : super.run(dataSourceProvider);
  }

  @Override
  protected SQLQuery buildQuery(LoadFeaturesEvent event) throws SQLException, ErrorResponseException {
    if (event.getVersionsToKeep() > 1) {
      SQLQuery headQuery = super.buildQuery(event);
      isForHistoryQuery = true;
      SQLQuery historyQuery = super.buildQuery(event);
      return new SQLQuery("(${{head}}) UNION (${{history}})")
          .withQueryFragment("head", headQuery)
          .withQueryFragment("history", historyQuery);
    }
    return super.buildQuery(event);
  }

  @Override
  protected SQLQuery buildFilterWhereClause(LoadFeaturesEvent event) {
    final Map<String, String> idMap = event.getIdsMap();

    if (event.getVersionsToKeep() > 1) {
      Set<String> idsOnly = new HashSet<>();
      Map<String, String> idsWithVersions = new HashMap<>();

      idMap.forEach((k, v) -> {
        idsOnly.add(k);
        if (v != null)
          idsWithVersions.put(k, v);
      });

      if (isForHistoryQuery)
        return new SQLQuery("(id, version) IN (${{loadFeaturesInput}})")
            .withQueryFragment("loadFeaturesInput", buildLoadFeaturesInputFragment(
                idsWithVersions.entrySet().stream().map(e -> LoadFeatureVersionInput.of(e.getKey(), Long.parseLong(e.getValue())))
                    .collect(Collectors.toList()), false));

      return new SQLQuery("(id, next_version) IN (${{loadFeaturesInputIdsOnly}})")
          .withQueryFragment("loadFeaturesInputIdsOnly", buildLoadFeaturesInputFragment(idsOnly
              .stream()
              .map(id -> LoadFeatureVersionInput.of(id))
              .collect(Collectors.toList()), true));
    }
    else
      return new SQLQuery("id = ANY(#{ids})")
          .withNamedParameter("ids", idMap.keySet().toArray(new String[0]));
  }

  private static SQLQuery buildLoadFeaturesInputFragment(List<LoadFeatureVersionInput> input, boolean head) {
    String inputParamName = head ? "headIds" : "historyIds";
    //NOTE: If the version of the input object = -1 (default / unset) this will be translated into max_bigint() by the following query
    return new SQLQuery("SELECT * FROM "
        + "json_populate_recordset((null, max_bigint())::LOAD_FEATURE_VERSION_INPUT, #{" + inputParamName + "}::JSON)")
        .withNamedParameter(inputParamName, XyzSerializable.serialize(input));
  }

  /**
   * The java type representation of the Postgres types LOAD_FEATURE_VERSION_INPUT.
   * This class is used for serialization to enable passing an array of complex type as a parameter to the LFE query.
   */
  @JsonInclude(NON_DEFAULT)
  private static class LoadFeatureVersionInput implements XyzSerializable {
    public String id;
    public long version = -1;

    public static LoadFeatureVersionInput of(String id) {
      LoadFeatureVersionInput input = new LoadFeatureVersionInput();
      input.id = id;
      return input;
    }
    public static LoadFeatureVersionInput of(String id, long version) {
      LoadFeatureVersionInput input = of(id);
      input.version = version;
      return input;
    }
  }
}
