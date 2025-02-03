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

package com.here.xyz.test.featurewriter.sql;

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildCreateSpaceTableQueries;

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.UpdateStrategy.OnExists;
import com.here.xyz.events.UpdateStrategy.OnMergeConflict;
import com.here.xyz.events.UpdateStrategy.OnNotExists;
import com.here.xyz.events.UpdateStrategy.OnVersionConflict;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.test.SQLITBase;
import com.here.xyz.test.featurewriter.SpaceWriter;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.SQLError;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SQLSpaceWriter extends SpaceWriter {

  protected static String VERSION_SEQUENCE_SUFFIX = "_version_seq";

  public SQLSpaceWriter(boolean composite, String testSuiteName) {
    super(composite, testSuiteName);
  }

  @Override
  public void createSpaceResources() throws Exception {
    try (DataSourceProvider dsp = SQLITBase.getDataSourceProvider()) {
      List<SQLQuery> queries = new ArrayList<>();
      if (composite)
        queries.addAll(buildCreateSpaceTableQueries(dsp.getDatabaseSettings().getSchema(), superSpaceId()));
      queries.addAll(buildCreateSpaceTableQueries(dsp.getDatabaseSettings().getSchema(), spaceId()));
      SQLQuery.batchOf(queries).writeBatch(dsp);
    }
  }

  @Override
  public void cleanSpaceResources() throws Exception {
    try (DataSourceProvider dsp = SQLITBase.getDataSourceProvider()) {
      buildDropSpaceQuery(dsp.getDatabaseSettings().getSchema(), spaceId()).write(dsp);

      if (composite)
        buildDropSpaceQuery(dsp.getDatabaseSettings().getSchema(), superSpaceId()).write(dsp);
    }
  }

  private SQLQuery buildDropSpaceQuery(String schema, String table) {
    return new SQLQuery("${{dropTable}} ${{dropISequence}} ${{dropVersionSequence}}").withQueryFragment("dropTable",
            "DROP TABLE IF EXISTS ${schema}.${table};").withQueryFragment("dropISequence", "DROP SEQUENCE IF EXISTS ${schema}.${iSequence};")
        .withQueryFragment("dropVersionSequence", "DROP SEQUENCE IF EXISTS ${schema}.${versionSequence};").withVariable(SCHEMA, schema)
        .withVariable(TABLE, table).withVariable("iSequence", table + VERSION_SEQUENCE_SUFFIX)
        .withVariable("versionSequence", table + VERSION_SEQUENCE_SUFFIX);
  }

  @Override
  protected void writeFeatures(List<Feature> featureList, String author, OnExists onExists,
      OnNotExists onNotExists, OnVersionConflict onVersionConflict,
      OnMergeConflict onMergeConflict, boolean isPartial, SpaceContext spaceContext, boolean historyEnabled,
      SQLError expectedErrorCode) throws Exception {
    runWriteFeatureQuery(featureList, author, onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial, spaceContext,
        historyEnabled);
  }

  private int[] runWriteFeatureQuery(List<Feature> featureList, String author, OnExists onExists, OnNotExists onNotExists,
      OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial, SpaceContext spaceContext,
      boolean historyEnabled) throws Exception {
    try (DataSourceProvider dsp = SQLITBase.getDataSourceProvider()) {
      SQLQuery q = generateWriteFeatureQuery(featureList, author, onExists, onNotExists, onVersionConflict, onMergeConflict,
          isPartial, spaceContext, historyEnabled);

      return SQLQuery.batchOf(q).writeBatch(dsp);
    }
  }

  private SQLQuery generateWriteFeatureQuery(List<Feature> featureList, String author, OnExists onExists, OnNotExists onNotExists,
      OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial, SpaceContext spaceContext,
      boolean historyEnabled) {
    final Map<String, Object> queryContext = new HashMap<>(Map.of(
        "schema", getDataSourceProvider().getDatabaseSettings().getSchema(),
        "table", spaceId(),
        "context", spaceContext,
        "historyEnabled", historyEnabled
    ));
    if (composite)
      queryContext.put("extendedTable", superSpaceId());

    WriteFeaturesEvent.Modification modification = new
            WriteFeaturesEvent.Modification()
            .withFeatureData(new FeatureCollection().withFeatures(featureList))
            .withUpdateStrategy(new UpdateStrategy(onExists, onNotExists, onVersionConflict, onMergeConflict))
            .withPartialUpdates(isPartial);

    return new SQLQuery("SELECT write_features(#{featureModificationList}, 'Modifications', #{author}, #{responseDataExpected});")
        .withNamedParameter("featureModificationList", XyzSerializable.serialize(Set.of(modification)))
        .withNamedParameter("author", author)
        .withNamedParameter("responseDataExpected", true)
        .withContext(queryContext);
  }
}
