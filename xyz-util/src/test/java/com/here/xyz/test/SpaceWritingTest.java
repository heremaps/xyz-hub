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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static com.here.xyz.test.featurewriter.TestSuite.TEST_FEATURE_GEOMETRY;
import static com.here.xyz.test.featurewriter.TestSuite.TEST_FEATURE_ID;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class SpaceWritingTest extends SQLITBase {
  public static String DEFAULT_AUTHOR = "ANONYMOUS";
  public static String OTHER_AUTHOR = "Other Author";
  public static String UPDATE_AUTHOR = "ANONYMOUS_UPDATE";
  public static String DEFAULT_FEATURE_ID = TEST_FEATURE_ID;
  //Used for Table or SpaceName
  private String spaceId = this.getClass().getSimpleName();
  protected boolean composite;

  protected String superSpaceId() {
    if (!composite)
      throw new IllegalArgumentException(spaceId + " is not a composite space");
    return spaceId + "_super";
  }

  protected String spaceId() {
    return spaceId;
  }

  protected SpaceWritingTest(boolean composite) {
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

  public void checkFeatureCount(int expectedCount) throws Exception {
    assertEquals(expectedCount, getRowCount(EXTENSION));
  }

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

  public SQLQuery checkExistingFeature(Feature feature, Long version, Long next_version, Operation operation, String author)
      throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery checkQuery = new SQLQuery(
          "SELECT id, version, next_version, operation, author, jsondata, ST_AsGeojson(geo) AS geo " + " FROM ${schema}.${table} "
              + "WHERE id = #{id} AND version = #{version};").withVariable(SCHEMA, dsp.getDatabaseSettings().getSchema())
          .withVariable(TABLE, spaceId()).withNamedParameter("id", feature.getId()).withNamedParameter("version", version);

      checkQuery.run(dsp, rs -> {
        try {
          assertTrue("Feature does not exist but was expected to exist", rs.next());

          Long db_version = rs.getLong("version");
          Long db_next_version = rs.getLong("next_version");
          String db_operation = rs.getString("operation");
          String db_author = rs.getString("author");
          String db_jsondata = rs.getString("jsondata");
          String db_geo = rs.getString("geo");

          if (version != null)
            assertEquals(version, db_version);
          if (next_version != null)
            assertEquals(next_version, db_next_version);
          if (operation != null)
            assertEquals(operation.toString(), db_operation);
          if (author != null)
            assertEquals(author, db_author);
          if (feature.getGeometry() != null)
            checkGeometry(db_geo, feature.getGeometry());
          if (feature.getProperties() != null)
            checkProperties(db_jsondata, feature.getProperties());

          checkNamespace(db_jsondata, author, operation, version);
          return null;
        }
        catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      });
    }
    return null;
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

  public SQLQuery checkDeletedFeatureOnHistory(String id, boolean shouldExist) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery check = new SQLQuery(
          "SELECT id FROM ${schema}.${table} WHERE id = #{id} AND operation = 'D' AND next_version = max_bigint();").withVariable(SCHEMA,
          dsp.getDatabaseSettings().getSchema()).withVariable(TABLE, spaceId()).withNamedParameter("id", id);

      check.run(dsp, rs -> {
        if (shouldExist)
          assertTrue("History entry for deletion does not exist!", rs.next());
        return null;
      });
    }
    return null;
  }

  protected void checkNamespace(String dbFeature, String author, Operation operation, long version) throws JsonProcessingException {
    Feature f = XyzSerializable.deserialize(dbFeature);

    long createdAt = f.getProperties().getXyzNamespace().getCreatedAt();
    long updatedAt = f.getProperties().getXyzNamespace().getUpdatedAt();

    assertNotNull(createdAt);
    assertNotNull(updatedAt);
    //TODO: Hub does not write version & author - it gets injected. Align FeatureWriter!
    //assertEquals(version, f.getProperties().getXyzNamespace().getVersion() );
    //assertEquals(author, f.getProperties().getXyzNamespace().getAuthor());

    if (operation.equals(Operation.I))
      assertEquals(createdAt, updatedAt);
  }

  protected void checkGeometry(String dbGeometry, Geometry featureGeo) throws JsonProcessingException {
    Geometry dbGeo = XyzSerializable.deserialize(dbGeometry);
    dbGeo.getJTSGeometry().equalsExact(featureGeo.getJTSGeometry());
  }

  protected void checkProperties(String dbFeature, Properties expectedProperties) throws JsonProcessingException {
    Feature f = XyzSerializable.deserialize(dbFeature);
    checkProperties((HashMap) f.getProperties().toMap(), (HashMap) expectedProperties.toMap());
  }

  protected void checkProperties(HashMap dbProperties, HashMap expectedProperties) {
    for (Object key : dbProperties.keySet()) {
      Object dbValue = dbProperties.get(key);
      Object expectedValue = expectedProperties.get(key);

      //gets checked in checkNamespace
      if (key.equals("@ns:com:here:xyz"))
        continue;

      if (dbValue instanceof HashMap<?, ?>)
        checkProperties((HashMap) dbValue, (HashMap) expectedValue);
      if (dbValue instanceof ArrayList<?>) {
        if (((ArrayList<?>) dbValue).size() != ((ArrayList<?>) expectedValue).size())
          fail("Array sizes are not equal " + dbValue + " != " + expectedValue);
        for (Object item : (ArrayList<?>) dbValue) {
          if (((ArrayList<?>) expectedValue).indexOf(item) == -1)
            fail("Array item is missing " + item + " not in " + expectedValue);
        }
        continue;
      }

      if (!dbValue.equals(expectedValue))
        fail("Properties not equal: " + dbValue + " != " + expectedValue);
    }
  }

  private static final Map<String, SQLError> SQLErrorLookup = new HashMap<>();

  public enum SQLError {
    ILLEGAL_ARGUMENT("XYZ40"),
    FEATURE_EXISTS("XYZ20"),
    FEATURE_NOT_EXISTS("XYZ44"),
    MERGE_CONFLICT_ERROR("XYZ48"),
    VERSION_CONFLICT_ERROR("XYZ49"),
    XYZ_EXCEPTION("XYZ50"),
    IMPORT_FORMAT_NOT_SUPPORTED("XYZ51"),
    IMPORT_FAILED_NON_RETRYABLE("XYZ52");


    public final String errorCode;

    SQLError(String errorCode) {
      this.errorCode = errorCode;
      SQLErrorLookup.put(errorCode, this);
    }

    public String getErrorCode() {
      return errorCode;
    }

    public static SQLError fromErrorCode(String errorCode) {
      return SQLErrorLookup.get(errorCode);
    }
  }

  public enum OnExists {
    DELETE,
    REPLACE, //Default
    RETAIN,
    ERROR
  }

  public enum OnNotExists {
    CREATE, //Default
    RETAIN,
    ERROR
  }

  public enum OnVersionConflict {
    MERGE, //Default for WRITE
    REPLACE, //Default for DELETE
    RETAIN,
    ERROR
  }

  public enum OnMergeConflict {
    REPLACE,
    RETAIN,
    ERROR //Default
  }

  public enum Operation {
    I, //Insert
    U, //Update
    D, //Delete
    H, //InsertHide
    J  //UpdateHide
  }
}
