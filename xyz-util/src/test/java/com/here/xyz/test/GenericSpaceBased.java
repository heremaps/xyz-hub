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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public abstract class GenericSpaceBased extends SQLITBase{
  public static String DEFAULT_AUTHOR = "ANONYMOUS";
  public static String UPDATE_AUTHOR = "ANONYMOUS_UPDATE";
  public static String DEFAULT_FEATURE_ID = "id1";

  public enum SQLError {
    ILLEGAL_ARGUMENT("XYZ40"),
    FEATURE_EXISTS("XYZ44"),
    FEATURE_NOT_EXISTS("XYZ45"),
    MERGE_CONFLICT_ERROR("XYZ48"),
    VERSION_CONFLICT_ERROR("XYZ49"),
    XYZ_EXCEPTION("XYZ50"),
    IMPORT_FORMAT_NOT_SUPPORTED("XYZ51"),
    IMPORT_FAILED_NON_RETRYABLE("XYZ52");

    public final String errorCode;
    SQLError(String errorCode) {this.errorCode = errorCode;}

    public String getErrorCode(){return errorCode;}
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

  //Used for Table or SpaceName
  String resource = this.getClass().getSimpleName();

  public abstract void createSpaceResources() throws Exception;

  public abstract void cleanSpaceResources() throws Exception;

  protected abstract void writeFeaturesWithAssertion(List<Feature> featureList, String author, OnExists onExists,
             OnNotExists onNotExists, OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict,
             boolean isPartial, SpaceContext spaceContext, boolean historyEnabled, SQLError expectedErrorCode) throws Exception;

  public void writeFeature(Feature modifiedFeature, String author,
                              OnExists onExists, OnNotExists onNotExists,
                              OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial,
                              SpaceContext spaceContext, boolean isHistoryActive, SQLError expectedError)
          throws Exception {
    writeFeaturesWithAssertion(Arrays.asList(modifiedFeature), author, onExists , onNotExists,
            onVersionConflict, onMergeConflict, isPartial, spaceContext, isHistoryActive, expectedError);
  }

  protected SQLQuery getFeaturesByIds(List<String> featureIds) throws Exception {
    //WIP
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery check = new SQLQuery("Select id, version, next_version, operation, author, jsondata, geo " +
              " from ${schema}.${table} " +
              "WHERE id = ANY(#{ids});")
              .withVariable(SCHEMA, dsp.getDatabaseSettings().getSchema())
              .withVariable(TABLE, resource)
              .withNamedParameter("ids", featureIds.toArray(new String[0]));

      check.run(dsp, rs -> {
        while (rs.next()) {
          rs.getLong("version");
          rs.getLong("next_version");
          rs.getString("operation");
          rs.getString("author");
          rs.getString("jsondata");
          rs.getString("geo");
        }
        return null;
      });
    }
    return null;
  }

  public void checkFeatureCount(int expectedCnt) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery cntQuery = new SQLQuery("Select count(1) from ${schema}.${table} ")
              .withVariable(SCHEMA, dsp.getDatabaseSettings().getSchema())
              .withVariable(TABLE, resource);

      cntQuery.run(dsp, rs -> {
        int count = 0;
        if(rs.next()){
          count = rs.getInt(1);
        }
        assertEquals(expectedCnt, count);
        return null;
      });
    }
  }

  public SQLQuery checkExistingFeature(Feature feature, Long version, Long next_version, Operation operation, String author) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery checkQuery = new SQLQuery("Select id, version, next_version, operation, author, jsondata, ST_AsGeojson(geo) as geo " +
              " from ${schema}.${table} " +
              "WHERE id = #{id} AND version = #{version};")
              .withVariable(SCHEMA, dsp.getDatabaseSettings().getSchema())
              .withVariable(TABLE, resource)
              .withNamedParameter("id", feature.getId())
              .withNamedParameter("version", version);

      checkQuery.run(dsp, rs -> {
        if(!rs.next())
          throw new RuntimeException("Feature does not exists");

        Long db_version = rs.getLong("version");
        Long db_next_version = rs.getLong("next_version");
        String db_operation = rs.getString("operation");
        String db_author = rs.getString("author");
        String db_jsondata = rs.getString("jsondata");
        String db_geo = rs.getString("geo");

        if(version != null)
          assertEquals(version, db_version);
        if(next_version != null)
          assertEquals(next_version, db_next_version);
        if(operation != null)
          assertEquals(operation.toString(), db_operation);
        if(author != null)
          assertEquals(author, db_author);
        if(feature.getGeometry() != null)
          checkGeometry(db_geo, feature.getGeometry());
        if(feature.getProperties() != null)
          checkProperties(db_jsondata, feature.getProperties());

        checkNamespace(db_jsondata, author, operation, version);
        return null;
      });
    }
    return null;
  }

  public SQLQuery checkNotExistingFeature(String id) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery check = new SQLQuery("Select id from ${schema}.${table} WHERE id = #{id};")
              .withVariable(SCHEMA, dsp.getDatabaseSettings().getSchema())
              .withVariable(TABLE, resource)
              .withNamedParameter("id", id);

      check.run(dsp, rs -> {
        if(rs.next())
          throw new RuntimeException("Feature exists!");
        return null;
      });
    }
    return null;
  }

  public SQLQuery checkDeletedFeatureOnHistory(String id, boolean shouldExist) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery check = new SQLQuery("Select id from ${schema}.${table} WHERE id = #{id} AND operation='D' AND next_version=max_bigint();")
              .withVariable(SCHEMA, dsp.getDatabaseSettings().getSchema())
              .withVariable(TABLE, resource)
              .withNamedParameter("id", id);

      check.run(dsp, rs -> {
        if(rs.next()) {
          return null;
        }
        if(shouldExist)
          throw new RuntimeException("History entry for deletion does not exist!");
        return null;
      });
    }
    return null;
  }

  protected void checkNamespace(String dbFeature, String author, Operation operation, long version){
    try {
      Feature f = XyzSerializable.deserialize(dbFeature);

      long createdAt = f.getProperties().getXyzNamespace().getCreatedAt();
      long updatedAt = f.getProperties().getXyzNamespace().getUpdatedAt();

      assertNotNull(createdAt);
      assertNotNull(updatedAt);
      //TODO: Hub does not write version & author - it gets injected. Align FeatureWriter!
      //assertEquals(version, f.getProperties().getXyzNamespace().getVersion() );
      //assertEquals(author, f.getProperties().getXyzNamespace().getAuthor());

      if(operation.equals(Operation.I)){
        assertEquals(createdAt, updatedAt);
      }

    }catch (JsonProcessingException e){
      throw new RuntimeException(e);
    }
  }

  protected void checkGeometry(String dbGeometry, Geometry featureGeo){
    try {
      Geometry dbGeo = XyzSerializable.deserialize(dbGeometry);
      dbGeo.getJTSGeometry().equalsExact(featureGeo.getJTSGeometry());
    }catch (JsonProcessingException e){
      throw new RuntimeException(e);
    }
  }

  protected void checkProperties(String dbFeature, Properties expectedProperties){
    try {
      Feature f = XyzSerializable.deserialize(dbFeature);

      checkProperties((HashMap) f.getProperties().toMap(), (HashMap) expectedProperties.toMap());
    }catch (JsonProcessingException e){
      throw new RuntimeException(e);
    }
  }

  protected void checkProperties(HashMap dbProperties, HashMap expectedProperties){
    for (Object key : dbProperties.keySet()){
      Object dbValue = dbProperties.get(key);
      Object expectedValue = expectedProperties.get(key);

      //gets checked in checkNamespace
      if(key.equals("@ns:com:here:xyz"))
        continue;

      if(dbValue instanceof HashMap<?,?>)
        checkProperties((HashMap) dbValue, (HashMap) expectedValue);
      if(dbValue instanceof ArrayList<?>){
        if(((ArrayList<?>) dbValue).size() != ((ArrayList<?>) expectedValue).size())
          fail("Array sizes are not equal "+dbValue+" != " + expectedValue);
        for (Object item : (ArrayList<?>) dbValue){
          if(((ArrayList<?>) expectedValue).indexOf(item) == -1)
            fail("Array item is missing "+item+" not in " + expectedValue);
        }
        continue;
      }

      if(!dbValue.equals(expectedValue))
        fail("Properties not equal: "+dbValue+" != " + expectedValue);
    }
  }
}
