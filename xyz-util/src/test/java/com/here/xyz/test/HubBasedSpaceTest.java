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

import static com.here.xyz.test.GenericSpaceBased.OnVersionConflict.REPLACE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HubBasedSpaceTest extends GenericSpaceBased {
  private HubWebClient webClient;
  private boolean composite;
  private boolean history;

  public HubBasedSpaceTest(boolean composite, boolean history) {
    this.composite = composite;
    this.history = history;

    webClient = HubWebClient.getInstance("http://localhost:8080/hub");
  }

  @Override
  public void createSpaceResources() throws Exception {
    Space space = new Space()
        .withId(resource)
        .withTitle(resource + " Titel")
        .withVersionsToKeep(this.history ? 100 : 1);
    webClient.createSpace(space);

    if (this.composite) {
      space = new Space()
          .withId(resource + "_ext")
          .withTitle(resource + "_ext Titel")
          .withExtension(new Space.Extension()
              .withSpaceId(resource));
      webClient.createSpace(space);
    }
  }

  @Override
  public void cleanSpaceResources() throws Exception {
    webClient.deleteSpace(resource);
    if (this.composite)
      webClient.deleteSpace(resource + "_ext");
  }

  @Override
  public void writeFeaturesWithAssertion(List<Feature> featureList, String author, OnExists onExists, OnNotExists onNotExists,
      OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial,
      SpaceContext spaceContext, boolean historyEnabled, SQLError expectedErrorCode) {
    FeatureCollection featureCollection = new FeatureCollection().withFeatures(featureList);

    try {
      XyzResponse xyzResponse = webClient.postFeatures(resource, featureCollection,
          generateQueryParams(onExists, onNotExists, onVersionConflict, onMergeConflict, spaceContext));
      System.out.println(xyzResponse);
    }
    catch (XyzWebClient.ErrorResponseException e) {
      //ToDo impl assert
      if (e.getErrorResponse() != null) {
        switch (e.getErrorResponse().statusCode()) {
          case 409: {
            if (onNotExists != null)
              assertEquals(OnNotExists.ERROR, onNotExists);
            if (onExists != null)
              assertEquals(onExists.ERROR, onExists);
            if (onExists == null && onVersionConflict != null) {
              if (onMergeConflict != null)
                //only on retain and error we will not find an object
                assertNotEquals(onMergeConflict.REPLACE, onMergeConflict);
              else
                assertEquals(onVersionConflict.ERROR, onVersionConflict);
            }
            break;
          }
          default:
            fail(onNotExists + " " + onExists + " " + onMergeConflict + " " + onMergeConflict + " => " + e.getErrorResponse().statusCode());
        }
      }
    }
    catch (XyzWebClient.WebClientException e) {
      //TODO
      fail();
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
}
