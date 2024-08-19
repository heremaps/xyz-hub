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

package com.here.xyz.test;

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildCreateSpaceTableQueries;

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.json.JSONObject;

public class SQLBasedSpaceTest extends SpaceWritingTest {

  protected static String VERSION_SEQUENCE_SUFFIX = "_version_seq";

  public SQLBasedSpaceTest(boolean composite) {
    super(composite);
  }

  @Override
  public void createSpaceResources() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      List<SQLQuery> queries = new ArrayList<>();
      if (this.composite)
        queries.addAll(buildCreateSpaceTableQueries(dsp.getDatabaseSettings().getSchema(), superSpaceId()));
      queries.addAll(buildCreateSpaceTableQueries(dsp.getDatabaseSettings().getSchema(), spaceId()));
      SQLQuery.batchOf(queries).writeBatch(dsp);
    }
  }

  @Override
  public void cleanSpaceResources() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      buildDropSpaceQuery(dsp.getDatabaseSettings().getSchema(), spaceId()).write(dsp);

      if (this.composite)
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
  protected void writeFeatures(List<Feature> featureList, String author, SpaceWritingTest.OnExists onExists,
      SpaceWritingTest.OnNotExists onNotExists, SpaceWritingTest.OnVersionConflict onVersionConflict,
      SpaceWritingTest.OnMergeConflict onMergeConflict, boolean isPartial, SpaceContext spaceContext, boolean historyEnabled,
      SpaceWritingTest.SQLError expectedErrorCode) throws Exception {
    runWriteFeatureQuery(featureList, author, onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial, spaceContext,
        historyEnabled);
  }

  private int[] runWriteFeatureQuery(List<Feature> featureList, String author, OnExists onExists, OnNotExists onNotExists,
      OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial, SpaceContext spaceContext,
      boolean historyEnabled) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      List<SQLQuery> q = generateWriteFeatureQuery(featureList, author, onExists, onNotExists, onVersionConflict, onMergeConflict,
          isPartial, spaceContext, historyEnabled);

      return SQLQuery.batchOf(q).writeBatch(dsp);
    }
  }

  private List<SQLQuery> generateWriteFeatureQuery(List<Feature> featureList, String author, OnExists onExists, OnNotExists onNotExists,
      OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial, SpaceContext spaceContext,
      boolean historyEnabled) {
    SQLQuery contextQuery = createContextQuery(spaceContext, historyEnabled);

    SQLQuery writeFeaturesQuery = new SQLQuery("SELECT write_features(#{featureList}::TEXT, #{author}::TEXT, #{onExists},"
        + "#{onNotExists}, #{onVersionConflict}, #{onMergeConflict}, #{isPartial}::BOOLEAN);").withNamedParameter("featureList",
            XyzSerializable.serialize(featureList)).withNamedParameter("author", author)
        .withNamedParameter("onExists", onExists != null ? onExists.toString() : null)
        .withNamedParameter("onNotExists", onNotExists != null ? onNotExists.toString() : null)
        .withNamedParameter("onVersionConflict", onVersionConflict != null ? onVersionConflict.toString() : null)
        .withNamedParameter("onMergeConflict", onMergeConflict != null ? onMergeConflict.toString() : null)
        .withNamedParameter("isPartial", isPartial);
    return Arrays.asList(contextQuery, writeFeaturesQuery);
  }

  //TODO: Use query context directly instead
  private SQLQuery createContextQuery(SpaceContext spaceContext, boolean historyEnabled) {
    return createContextQuery(getDataSourceProvider().getDatabaseSettings().getSchema(), spaceContext, historyEnabled);
  }

  private SQLQuery createContextQuery(String schema, SpaceContext spaceContext, boolean historyEnabled) {
    JSONObject context = new JSONObject()
        .put("schema", schema)
        .put("table", spaceId())
        .put("context", spaceContext)
        .put("historyEnabled", historyEnabled);

    if (composite)
      context.put("extendedTable", superSpaceId());

    return new SQLQuery("select context(#{context}::JSONB);").withNamedParameter("context", context.toString());
  }
}
