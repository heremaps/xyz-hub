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

package com.here.xyz.test.featurewriter;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static com.here.xyz.test.featurewriter.TestSuite.TEST_FEATURE_GEOMETRY;
import static com.here.xyz.test.featurewriter.TestSuite.TEST_FEATURE_ID;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.UpdateStrategy.OnExists;
import com.here.xyz.events.UpdateStrategy.OnNotExists;
import com.here.xyz.events.UpdateStrategy.OnVersionConflict;
import com.here.xyz.events.UpdateStrategy.OnMergeConflict;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.test.SQLITBase;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.SQLError;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class SpaceWriter extends SQLITBase {
  public static String DEFAULT_AUTHOR = "ANONYMOUS";
  public static String OTHER_AUTHOR = "Other Author";
  public static String UPDATE_AUTHOR = "ANONYMOUS_UPDATE";
  public static String DEFAULT_FEATURE_ID = TEST_FEATURE_ID;
  //Used for Table or SpaceName
  private String spaceId;
  protected boolean composite;

  protected String superSpaceId() {
    if (!composite)
      throw new IllegalArgumentException(spaceId + " is not a composite space");
    return spaceId + "_super";
  }

  protected String spaceId() {
    return spaceId;
  }

  protected SpaceWriter(boolean composite, String testSuiteName) {
    spaceId = testSuiteName != null ? testSuiteName : getClass().getSimpleName();
    this.composite = composite;
  }

  public abstract void createSpaceResources() throws Exception;

  public abstract void cleanSpaceResources() throws Exception;

  public void writeFeature(Feature modifiedFeature, String author, OnExists onExists, OnNotExists onNotExists,
                           OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial, SpaceContext spaceContext,
                           boolean isHistoryActive, SQLError expectedError) throws Exception {
    try {
      writeFeature(modifiedFeature, author, onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial,
          spaceContext, isHistoryActive);
    }
    catch (SQLException e) {
      assertEquals(expectedError, SQLError.fromErrorCode(e.getSQLState()));
    }
  }

  public void writeFeature(Feature modifiedFeature, String author, OnExists onExists, OnNotExists onNotExists,
      OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial, SpaceContext spaceContext,
      boolean isHistoryActive) throws Exception {
    writeFeatures(Arrays.asList(modifiedFeature), author, onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial,
        spaceContext, isHistoryActive, null);
  }

  protected abstract void writeFeatures(List<Feature> featureList, String author, OnExists onExists, OnNotExists onNotExists,
      OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial, SpaceContext spaceContext,
      boolean historyEnabled, SQLError expectedErrorCode) throws Exception;

  public Feature getFeature(SpaceContext context) throws Exception {
    return toFeature(getFeatureRow(context));
  }

  public Feature toFeature(SpaceTableRow row) {
    if (row == null)
      return null;

    Feature feature = XyzSerializable.fromMap(row.jsondata);
    feature.getProperties().getXyzNamespace()
        .withVersion(row.version)
        .withAuthor(row.author);
    return feature
        .withGeometry(row.geo);
  }

  public SpaceTableRow getFeatureRow(SpaceContext context) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      return new SQLQuery("SELECT id, version, next_version, operation, author, jsondata, geo " + " FROM ${schema}.${table} "
          + "WHERE id = #{id} AND next_version = #{MAX_BIGINT}")
          .withVariable(SCHEMA, dsp.getDatabaseSettings().getSchema())
          .withVariable(TABLE, context == SUPER ? superSpaceId() : spaceId())
          .withNamedParameter("id", TEST_FEATURE_ID)
          .withNamedParameter("MAX_BIGINT", Long.MAX_VALUE)
          .run(dsp, rs -> {
            try {
              return rs.next()
                  ? new SpaceTableRow(
                      rs.getString("id"),
                      rs.getLong("version"),
                      rs.getLong("next_version"),
                      Operation.valueOf(rs.getString("operation")),
                      rs.getString("author"),
                      XyzSerializable.deserialize(rs.getString("jsondata"), Map.class),
                      TEST_FEATURE_GEOMETRY //TODO: Read & transform geo from row, when it becomes relevant
                    )
                  : null;
            }
            catch (JsonProcessingException e) {
              throw new SQLException(e);
            }
          });
    }
  }

  public record SpaceTableRow(String id, long version, long next_version, Operation operation, String author, Map<String, Object> jsondata,
      Geometry geo) {}

  public int getRowCount(SpaceContext context) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      return new SQLQuery("SELECT count(1) FROM ${schema}.${table} ")
          .withVariable(SCHEMA, dsp.getDatabaseSettings().getSchema())
          .withVariable(TABLE, context == SUPER ? superSpaceId() : spaceId())
          .run(dsp, rs -> rs.next() ? rs.getInt(1) : 0);
    }
  }

  public Operation getLastUsedFeatureOperation(SpaceContext context) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      return new SQLQuery("SELECT operation FROM ${schema}.${table} WHERE id = #{id} ORDER BY version DESC LIMIT 1")
          .withVariable(SCHEMA, dsp.getDatabaseSettings().getSchema())
          .withVariable(TABLE, context == SUPER ? superSpaceId() : spaceId())
          .withNamedParameter("id", DEFAULT_FEATURE_ID)
          .run(dsp, rs -> rs.next() ? Operation.valueOf(rs.getString(1)) : null);
    }
  }

  public SQLQuery checkNotExistingFeature(String id) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery check = new SQLQuery("SELECT id FROM ${schema}.${table} WHERE id = #{id};").withVariable(SCHEMA,
          dsp.getDatabaseSettings().getSchema()).withVariable(TABLE, spaceId()).withNamedParameter("id", id);

      check.run(dsp, rs -> {
        assertFalse("Feature exists but was expected to not exist!", rs.next());
        return null;
      });
    }
    return null;
  }

  public enum Operation {
    I, //Insert
    U, //Update
    D, //Delete
    H, //InsertHide
    J  //UpdateHide
  }
}
