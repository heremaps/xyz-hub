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
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildCreateSpaceTableQueries;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class SQLITSpaceBase extends SQLITBase{
  private static String VERSION_SEQUENCE_SUFFIX = "_version_seq";
  protected static String DEFAULT_AUTHOR = "testAuthor";
  protected static String DEFAULT_FEATURE_ID = "id1";

  protected enum SQLErrorCodes {
    XYZ40, //Illegal Argument
    XYZ44, //Feature exists
    XYZ45, //Feature not exists
    XYZ48, //MergeConflictError
    XYZ49, //VersionConflictError
    XYZ50, //XyzException
    XYZ51, //Import Format not supported
    XYZ52  //Import Failed non retryable
  }

  protected enum OnExists {
    DELETE,
    REPLACE, //Default
    RETAIN,
    ERROR
  }

  protected enum OnNotExists {
    CREATE, //Default
    RETAIN,
    ERROR
  }

  protected enum OnVersionConflict {
    MERGE, //Default for WRITE
    REPLACE,
    RETAIN,
    ERROR //Default
  }

  protected enum OnMergeConflict {
    REPLACE, //Default for DELETE
    RETAIN,
    ERROR
  }

  protected enum Operation {
    I, //Insert
    U, //Update
    D, //Delete
    H, //InsertHide
    J  //UpdateHide
  }
  
  String table = this.getClass().getSimpleName();
  
  @Before
  public void prepare() throws Exception {
    createSpaceTable();
  }

  @After
  public void clean() throws Exception {
    dropSpaceTable();
  }

  private void createSpaceTable() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      List<SQLQuery> queries = new ArrayList<>();
      queries.addAll(buildCreateSpaceTableQueries(dsp.getDatabaseSettings().getSchema(), table));
      SQLQuery.batchOf(queries).writeBatch(dsp);
    }
  }

  private void dropSpaceTable() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {

      SQLQuery q = new SQLQuery("${{dropTable}} ${{dropISequence}} ${{dropVersionSequence}}")
              .withQueryFragment("dropTable", "DROP TABLE IF EXISTS ${schema}.${table};")
              .withQueryFragment("dropISequence", "DROP SEQUENCE IF EXISTS ${schema}.${iSequence};")
              .withQueryFragment("dropVersionSequence", "DROP SEQUENCE IF EXISTS ${schema}.${versionSequence};")
              .withVariable(SCHEMA, dsp.getDatabaseSettings().getSchema())
              .withVariable(TABLE, table)
              .withVariable("iSequence", table + VERSION_SEQUENCE_SUFFIX)
              .withVariable("versionSequence", table + VERSION_SEQUENCE_SUFFIX);

      q.write(dsp);
    }
  }

  protected int[] runWriteFeatureQuery(List<Feature> featureList, String author, OnExists onExists, OnNotExists onNotExists,
                                    OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial,
                                    SpaceContext spaceContext, boolean historyEnabled) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      List<SQLQuery> q = generateWriteFeatureQuery(featureList, author,
              onExists, onNotExists, onVersionConflict, onMergeConflict,
              isPartial, spaceContext, historyEnabled);

      return SQLQuery.batchOf(q).writeBatch(dsp);
    }
  }

  protected void runWriteFeatureQueryWithSQLAssertion(List<Feature> featureList, String author, OnExists onExists,
             OnNotExists onNotExists, OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict,
             boolean isPartial, SpaceContext spaceContext, boolean historyEnabled, SQLErrorCodes expectedErrorCode) throws Exception {

    try{
      runWriteFeatureQuery(featureList, author, onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial, spaceContext, historyEnabled);
    }catch (SQLException e){
        if(expectedErrorCode != null)
          assertEquals(expectedErrorCode.toString(), e.getSQLState());
    }
  }

  private List<SQLQuery> generateWriteFeatureQuery(List<Feature> featureList, String author, OnExists onExists, OnNotExists onNotExists,
                           OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial,
                           SpaceContext spaceContext, boolean historyEnabled){
      SQLQuery contextQuery = createContextQuery(spaceContext, historyEnabled);

      SQLQuery writeFeaturesQuery = new SQLQuery("SELECT write_features(#{featureList}::TEXT, #{author}::TEXT, #{onExists}," +
              "#{onNotExists}, #{onVersionConflict}, #{onMergeConflict}, #{isPartial}::BOOLEAN);")
          .withNamedParameter("featureList", XyzSerializable.serialize(featureList))
          .withNamedParameter("author", author)
          .withNamedParameter("onExists", onExists != null ? onExists.toString() : null)
          .withNamedParameter("onNotExists", onNotExists != null ? onNotExists.toString() : null)
          .withNamedParameter("onVersionConflict", onVersionConflict != null ? onVersionConflict.toString() : null)
          .withNamedParameter("onMergeConflict", onMergeConflict != null ? onMergeConflict.toString() : null)
          .withNamedParameter("isPartial", isPartial);
      return Arrays.asList(contextQuery, writeFeaturesQuery);
  }

  private SQLQuery createContextQuery( SpaceContext spaceContext, boolean historyEnabled) {
    return createContextQuery(getDataSourceProvider().getDatabaseSettings().getSchema(), table,
            spaceContext, historyEnabled);
  }
  private SQLQuery createContextQuery(String schema, String table, SpaceContext spaceContext,
                                      boolean historyEnabled) {
    JSONObject context = new JSONObject()
            .put("schema", schema)
            .put("table", table)
            .put("context", spaceContext)
            .put("historyEnabled", historyEnabled);

     return new SQLQuery("select context(#{context}::JSONB);")
            .withNamedParameter("context", context.toString());
  }

  protected SQLQuery getFeaturesByIds(List<String> featureIds) throws Exception {
    //WIP
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery check = new SQLQuery("Select id, version, next_version, operation, author, jsondata, geo " +
              " from ${schema}.${table} " +
              "WHERE id = ANY(#{ids});")
              .withVariable(SCHEMA, dsp.getDatabaseSettings().getSchema())
              .withVariable(TABLE, table)
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

  protected SQLQuery checkExistingFeature(Feature feature, Long version, Long next_version, Operation operation, String author) throws Exception {
    //WIP
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery check = new SQLQuery("Select id, version, next_version, operation, author, jsondata, ST_AsGeojson(geo) as geo " +
              " from ${schema}.${table} " +
              "WHERE id = #{id};")
              .withVariable(SCHEMA, dsp.getDatabaseSettings().getSchema())
              .withVariable(TABLE, table)
              .withNamedParameter("id", feature.getId());

      check.run(dsp, rs -> {
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

  protected SQLQuery checkNotExistingFeature(String id) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery check = new SQLQuery("Select id from ${schema}.${table} WHERE id = #{id};")
              .withVariable(SCHEMA, dsp.getDatabaseSettings().getSchema())
              .withVariable(TABLE, table)
              .withNamedParameter("id", id);

      check.run(dsp, rs -> {
        if(rs.next())
          throw new RuntimeException("Feature exists!");
        return null;
      });
    }
    return null;
  }

  protected void checkNamespace(String jsondata, String author, Operation operation, long version){
    try {
      Feature f = XyzSerializable.deserialize(jsondata);

      long createdAt = f.getProperties().getXyzNamespace().getCreatedAt();
      long updatedAt = f.getProperties().getXyzNamespace().getUpdatedAt();

      assertEquals(author, f.getProperties().getXyzNamespace().getAuthor());
      assertNotNull(createdAt);
      assertNotNull(updatedAt);
      assertEquals(version, f.getProperties().getXyzNamespace().getVersion() );

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

  protected void checkProperties(String jsondata, Properties featureProperties){
    try {
      Feature f = XyzSerializable.deserialize(jsondata);
      Properties dbProperties = f.getProperties();

      if(!featureProperties.keySet().equals(dbProperties.keySet()))
        fail("Properties not equal."+featureProperties.keySet()+" != " + dbProperties.keySet());
    }catch (JsonProcessingException e){
      throw new RuntimeException(e);
    }
  }
}
