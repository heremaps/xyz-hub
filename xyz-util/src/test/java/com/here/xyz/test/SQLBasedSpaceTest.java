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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.json.JSONObject;

public class SQLBasedSpaceTest extends GenericSpaceBased {

  protected static String VERSION_SEQUENCE_SUFFIX = "_version_seq";
  private boolean composite;

  public SQLBasedSpaceTest(boolean composite) {
    this.composite = composite;
  }

  @Override
  public void createSpaceResources() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      List<SQLQuery> queries = new ArrayList<>();
      queries.addAll(buildCreateSpaceTableQueries(dsp.getDatabaseSettings().getSchema(), resource));
      if (this.composite)
        queries.addAll(buildCreateSpaceTableQueries(dsp.getDatabaseSettings().getSchema(), resource + "_ext"));
      SQLQuery.batchOf(queries).writeBatch(dsp);
    }
  }

  @Override
  public void cleanSpaceResources() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {

      SQLQuery q = buildDropSpaceQuery(dsp.getDatabaseSettings().getSchema(), resource);
      q.write(dsp);

      if (this.composite) {
        q = buildDropSpaceQuery(dsp.getDatabaseSettings().getSchema(), resource + "_ext");
        q.write(dsp);
      }
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
  protected void writeFeaturesWithAssertion(List<Feature> featureList, String author, GenericSpaceBased.OnExists onExists,
      GenericSpaceBased.OnNotExists onNotExists, GenericSpaceBased.OnVersionConflict onVersionConflict,
      GenericSpaceBased.OnMergeConflict onMergeConflict, boolean isPartial, SpaceContext spaceContext, boolean historyEnabled,
      GenericSpaceBased.SQLError expectedErrorCode) throws Exception {
    boolean exceptionThrown = false;
    try {
      runWriteFeatureQuery(featureList, author, onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial, spaceContext,
          historyEnabled);
    }
    catch (SQLException e) {
      exceptionThrown = true;
      if (expectedErrorCode != null)
        assertEquals(expectedErrorCode.getErrorCode(), e.getSQLState());
      else
        fail("Unexpected Error " + e);
    }
    if (expectedErrorCode != null && !exceptionThrown)
      fail("Expected SQLException got not thrown");
  }

  protected int[] runWriteFeatureQuery(List<Feature> featureList, String author, OnExists onExists, OnNotExists onNotExists,
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

  private SQLQuery createContextQuery(SpaceContext spaceContext, boolean historyEnabled) {
    return createContextQuery(getDataSourceProvider().getDatabaseSettings().getSchema(), resource, spaceContext, historyEnabled);
  }

  private SQLQuery createContextQuery(String schema, String table, SpaceContext spaceContext, boolean historyEnabled) {
    JSONObject context = new JSONObject().put("schema", schema).put("table", table).put("extTable", table + "_ext")
        .put("context", spaceContext).put("historyEnabled", historyEnabled);

    return new SQLQuery("select context(#{context}::JSONB);").withNamedParameter("context", context.toString());
  }

  protected void runWriteFeatureQueryWithSQLAssertion(List<Feature> featureList, String author, OnExists onExists, OnNotExists onNotExists,
      OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial, SpaceContext spaceContext,
      boolean historyEnabled, SQLError expectedErrorCode) throws Exception {
    boolean exceptionThrown = false;
    try {
      runWriteFeatureQuery(featureList, author, onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial, spaceContext,
          historyEnabled);
    }
    catch (SQLException e) {
      exceptionThrown = true;
      if (expectedErrorCode != null)
        assertEquals(expectedErrorCode.getErrorCode(), e.getSQLState());
      else
        fail("Unexpected Error " + e);
    }
    if (expectedErrorCode != null && !exceptionThrown)
      fail("Expected SQLException got not thrown");
  }
}
