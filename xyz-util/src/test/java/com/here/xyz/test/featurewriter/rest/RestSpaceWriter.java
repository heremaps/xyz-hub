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

import static com.here.xyz.test.featurewriter.TestSuite.TEST_FEATURE_ID;
import static com.here.xyz.util.db.pg.SQLError.FEATURE_EXISTS;
import static com.here.xyz.util.db.pg.SQLError.FEATURE_NOT_EXISTS;
import static com.here.xyz.util.db.pg.SQLError.MERGE_CONFLICT_ERROR;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space;
import com.here.xyz.test.featurewriter.SpaceWriter;
import com.here.xyz.util.db.pg.SQLError;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.ErrorResponseException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import com.here.xyz.events.UpdateStrategy.OnExists;
import com.here.xyz.events.UpdateStrategy.OnNotExists;
import com.here.xyz.events.UpdateStrategy.OnVersionConflict;
import com.here.xyz.events.UpdateStrategy.OnMergeConflict;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RestSpaceWriter extends SpaceWriter {
  private boolean history;

  public RestSpaceWriter(boolean composite, boolean history, String testSuiteName) {
    super(composite, testSuiteName);
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
        .withVersionsToKeep(history ? 100 : 1);

    if (composite) {
      Space superSpace = new Space()
          .withId(superSpaceId())
          .withTitle(superSpaceId() + " Titel")
          .withVersionsToKeep(history ? 100 : 1);
      webClient(DEFAULT_AUTHOR).createSpace(superSpace);

      space.setExtension(new Space.Extension()
              .withSpaceId(superSpaceId()));
    }

    webClient(DEFAULT_AUTHOR).createSpace(space);
  }

  @Override
  public void cleanSpaceResources() throws Exception {
    webClient(DEFAULT_AUTHOR).deleteSpace(spaceId());
    if (composite)
      webClient(DEFAULT_AUTHOR).deleteSpace(superSpaceId());
  }

  @Override
  public void writeFeatures(List<Feature> featureList, String author, OnExists onExists, OnNotExists onNotExists,
      OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial,
      SpaceContext spaceContext, boolean historyEnabled, SQLError expectedErrorCode)
      throws WebClientException, JsonProcessingException, SQLException {
    FeatureCollection featureCollection = new FeatureCollection().withFeatures(featureList);

    try {
      String spaceId = spaceId();
      if (spaceContext == SpaceContext.SUPER) {
        spaceId = superSpaceId();
        spaceContext = null;
      }

      webClient(author).postFeatures(spaceId, featureCollection,
          generateQueryParams(onExists, onNotExists, onVersionConflict, onMergeConflict, spaceContext));
    }
    catch (ErrorResponseException e) {
      Map<String, Object> responseBody = XyzSerializable.deserialize(e.getErrorResponse().body(), Map.class);
      String errorMessage = (String) responseBody.get("errorMessage");
      switch (e.getErrorResponse().statusCode()) {
        case 404:
          throw new SQLException(errorMessage, FEATURE_NOT_EXISTS.errorCode, e);
        case 409: {
          switch (errorMessage) {
            case "The record does not exist.", "ERROR: Feature with ID " + TEST_FEATURE_ID + " not exists!":
              throw new SQLException(errorMessage, FEATURE_NOT_EXISTS.errorCode, e);
            case "The record {" + TEST_FEATURE_ID + "} exists.", "ERROR: Feature with ID " + TEST_FEATURE_ID + " exists!":
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
      if (onExists != null && onVersionConflict == OnVersionConflict.REPLACE)
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
}
