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

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient;

import java.util.List;

public class HubBasedSpaceTest extends GenericSpaceBased {
  private HubWebClient webClient;
  private boolean composite;
  private boolean history;
  private OnNotExists onNotExists;
  private OnExists onExists;
  private OnVersionConflict onVersionConflict;
  private OnMergeConflict onMergeConflict;

  public HubBasedSpaceTest(boolean composite, boolean history, OnNotExists onNotExists, OnExists onExists,
                           OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict) {
    this.composite = composite;
    this.history = history;
    this.onNotExists = onNotExists;
    this.onExists = onExists;
    this.onVersionConflict = onVersionConflict;
    this.onMergeConflict = onMergeConflict;

    webClient = HubWebClient.getInstance("http://localhost:8080/hub");
  }

  public static void main(String[] args) throws Exception {
    HubBasedSpaceTest hubBasedSpaceTest = new HubBasedSpaceTest(true, true, null, null, null,null);
    hubBasedSpaceTest.createSpaceResources();
    hubBasedSpaceTest.cleanSpaceResources();
  }

  @Override
  public void createSpaceResources() throws Exception {
    webClient.createSpace(resource, resource+" Title ");

    if(this.composite){
      Space space = new Space()
              .withId(resource+"_ext")
              .withTitle(resource+"_ext Titel")
              .withExtension(new Space.Extension()
              .withSpaceId(resource));
      webClient.createSpace(space);
    }
  }

  @Override
  public void cleanSpaceResources() throws Exception {
    webClient.deleteSpace(resource);
    if(this.composite)
      webClient.deleteSpace(resource+"_ext");
  }

  @Override
  public void writeFeaturesWithAssertion(List<Feature> featureList, String author, OnExists onExists, OnNotExists onNotExists, OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial, SpaceContext spaceContext, boolean historyEnabled, SQLError expectedErrorCode) throws Exception {
    FeatureCollection featureCollection = new FeatureCollection().withFeatures(featureList);

    try {
      XyzResponse xyzResponse = webClient.postFeatures(resource, featureCollection, generateQueryString());
    }catch (XyzWebClient.ErrorResponseException e){
      //ToDo impl assert
      if(e.getErrorResponse() != null){
        switch (e.getErrorResponse().statusCode()){
          case 409  : {
            assert(onNotExists.equals(OnNotExists.ERROR));
          }
        }
      }
    }catch (XyzWebClient.WebClientException e){
      //TODO
    }
  }

  public String generateQueryString(){
    String qs = "";

    if(onNotExists != null)
      qs +="&ne="+onNotExists.toString().toLowerCase();
    if(onExists != null)
      qs +="&e="+onExists.toString().toLowerCase();
    if(onVersionConflict != null)
      ;//qs +="&e="+onVersionConflict.toString();
    if(onMergeConflict != null)
      ;//not implemented

    return qs;
  }
}
