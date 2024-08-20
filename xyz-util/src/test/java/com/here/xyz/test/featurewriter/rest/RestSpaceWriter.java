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

package com.here.xyz.test.featurewriter.rest;

import static com.here.xyz.test.featurewriter.SpaceWriter.OnVersionConflict.REPLACE;
import static com.here.xyz.test.featurewriter.SpaceWriter.SQLError.FEATURE_EXISTS;
import static com.here.xyz.test.featurewriter.SpaceWriter.SQLError.FEATURE_NOT_EXISTS;
import static com.here.xyz.test.featurewriter.SpaceWriter.SQLError.MERGE_CONFLICT_ERROR;
import static com.here.xyz.test.featurewriter.TestSuite.TEST_FEATURE_ID;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.test.featurewriter.SpaceWriter;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.ErrorResponseException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RestSpaceWriter extends SpaceWriter {
  private boolean history;

  public RestSpaceWriter(boolean composite, boolean history) {
    super(composite);
    this.history = history;
  }

  private HubWebClient webClient(String author) {
    return HubWebClient.getInstance("http://localhost:8080/hub", Map.of("Author", author));
  }

  @Override
  public void createSpaceResources() throws Exception {
    Space space = new Space()
        .withId(spaceId())
        .withTitle(spaceId() + " Titel")
        .withVersionsToKeep(this.history ? 100 : 1);

    if (this.composite) {
      Space superSpace = new Space()
          .withId(superSpaceId())
          .withTitle(superSpaceId() + " Titel");
      webClient(DEFAULT_AUTHOR).createSpace(superSpace);

      space.setExtension(new Space.Extension()
              .withSpaceId(superSpaceId()));
    }

    webClient(DEFAULT_AUTHOR).createSpace(space);
  }

  @Override
  public void cleanSpaceResources() throws Exception {
    webClient(DEFAULT_AUTHOR).deleteSpace(spaceId());
    if (this.composite)
      webClient(DEFAULT_AUTHOR).deleteSpace(superSpaceId());
  }

  @Override
  public void writeFeatures(List<Feature> featureList, String author, OnExists onExists, OnNotExists onNotExists,
      OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial,
      SpaceContext spaceContext, boolean historyEnabled, SQLError expectedErrorCode)
      throws WebClientException, JsonProcessingException, SQLException {
    FeatureCollection featureCollection = new FeatureCollection().withFeatures(featureList);

    try {
      XyzResponse xyzResponse = webClient(author).postFeatures(spaceId(), featureCollection,
          generateQueryParams(onExists, onNotExists, onVersionConflict, onMergeConflict, spaceContext));
      System.out.println(xyzResponse);
    }
    catch (ErrorResponseException e) {
      switch (e.getErrorResponse().statusCode()) {
        case 409: {
          Map<String, Object> responseBody = XyzSerializable.deserialize(e.getErrorResponse().body(), Map.class);
          //FIXME: respond with correct status codes in hub (e.g. not exists => 404)
          String errorMessage = (String) responseBody.get("errorMessage");
          switch (errorMessage) {
            case "The record does not exist.":
              throw new SQLException(errorMessage, FEATURE_NOT_EXISTS.errorCode, e);
            case "The record {" + TEST_FEATURE_ID + "} exists.":
              throw new SQLException(errorMessage, FEATURE_EXISTS.errorCode, e);
            case "Conflict while merging someConflictingValue with someValue":
              throw new SQLException(errorMessage, MERGE_CONFLICT_ERROR.errorCode, e);
            default:
              throw e;
          }
        }
        default:
          throw e;
      }
    }
  }

  public Map<String, String> generateQueryParams(OnExists onExists, OnNotExists onNotExists, OnVersionConflict onVersionConflict,
      OnMergeConflict onMergeConflict, SpaceContext spaceContext) {
    Map<String, String> queryParams = new HashMap<>();

    if (onNotExists != null)
      queryParams.put("ne", onNotExists.toString());
    if (onExists != null && onVersionConflict == null)
      queryParams.put("e", onExists.toString());
    if (onVersionConflict != null) {
      if (onExists != null && onVersionConflict == REPLACE)
        //onExists has priority
        queryParams.put("e", onExists.toString());
      else
        queryParams.put("e", onVersionConflict.toString());
    }
    if (onMergeConflict != null)
      ;//not implemented

    if (spaceContext != null)
      queryParams.put("context", spaceContext.toString());

    return queryParams;
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
}
