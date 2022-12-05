/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.xyz.hub.rest;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_VND_HERE_CHANGESET_COLLECTION;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_VND_HERE_COMPACT_CHANGESET;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.changesets.ChangesetCollection;
import com.here.xyz.responses.changesets.CompactChangeset;
import com.jayway.restassured.response.ValidatableResponse;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(RestTests.class)
public class ReadHistoryApiIT extends TestSpaceWithFeature {

  @BeforeClass
  public static void setupClass() {
    remove();
  }

  @Before
  public void setup() {
    remove();
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(content("/xyz/hub/createSpaceWithGlobalVersioning.json"))
        .when().post(getCreateSpacePath()).then();

    response.statusCode(OK.code())
        .body("id", equalTo(getSpaceId()))
        .body("title", equalTo("My Demo Space"))
        .body("enableHistory", equalTo(true))
        .body("enableUUID", equalTo(true))
        .body("enableGlobalVersioning", equalTo(true))
        .body("storage.id", equalTo("psql"));

    /** Check Empty History */
    given().
            accept(APPLICATION_VND_HERE_CHANGESET_COLLECTION).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            when().
            get(getSpacesPath() + "/x-psql-test/history").
            then().
            statusCode(OK.code()).
            body("startVersion", equalTo(0)).
            body("endVersion", equalTo(0)).
            body("versions", equalTo(new HashMap()));

    given().
            accept(APPLICATION_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            when().
            get(getSpacesPath() + "/x-psql-test/history/statistics").
            then().
            statusCode(OK.code()).
            body("count.value", equalTo(0)).
            body("maxVersion.value", equalTo(0));

    /**
     * Perform:
     * Insert id: 1-500 (v1) isFree=true
     * Insert id: 501-100 (v2) isFree=true
     * Insert id: 1001-1500 (v3) isFree=true
     * ...
     * Insert id: 4501-5000 (v10) free=true
     *
     * */
    writeFeatures(5000, 500, 1, true);


    /**
     * Perform:
     * Update id: 100-150 (v.11) isFree=false
     * Update id: 150-200 (v.12) isFree=false
     * Update id: 100-200 (v.13) isFree=tue
     * Delete ids: 100,150,200,300 (v.14)
     * */
    modifyFeatures();
  }

  @After
  public void tearDown() {
    remove();
  }

  public void modifyFeatures(){
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get(getSpacesPath() + "/x-psql-test/features/5000").
            then().
            statusCode(OK.code()).
            body("properties.@ns:com:here:xyz.version", equalTo(10));

    /** Write two new versions (11,12) each with 50 objects*/
    writeFeatures(100,50,100,false);

    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get(getSpacesPath() + "/x-psql-test/features/100").
            then().
            statusCode(OK.code()).
            body("properties.@ns:com:here:xyz.version", equalTo(11));

    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get(getSpacesPath() + "/x-psql-test/features/150").
            then().
            statusCode(OK.code()).
            body("properties.@ns:com:here:xyz.version", equalTo(12));

    /** Write to new versions (13) with 100 objects*/
    writeFeatures(100,100,100,true);

    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get(getSpacesPath() + "/x-psql-test/features/150").
            then().
            statusCode(OK.code()).
            body("properties.@ns:com:here:xyz.version", equalTo(13));

    /** Delete some Features (14) */
    String idList = "id=100&id=150&id=200&id=300";
    deleteFeatures(idList);

    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get(getSpacesPath() + "/x-psql-test/features?"+idList).
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(0));
  }

  @Test
  public void readChangesetCollectionWithVersionRange() throws JsonProcessingException {
    /**
     * GET Version 1-2:
     * 500 inserted objects v1
     * 500 inserted objects v2
     * */
    String body =
        given().
            accept(APPLICATION_VND_HERE_CHANGESET_COLLECTION).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get(getSpacesPath() + "/x-psql-test/history?startVersion=1&endVersion=2").
            getBody().asString();

    ChangesetCollection ccol = XyzSerializable.deserialize(body);
    assertEquals(1, ccol.getStartVersion());
    assertEquals(2, ccol.getEndVersion());
    assertEquals(2, ccol.getVersions().size());
    assertEquals(500, ccol.getVersions().get(1).getInserted().getFeatures().size());
    assertEquals(500, ccol.getVersions().get(2).getInserted().getFeatures().size());
    assertEquals(true, ccol.getVersions().get(2).getInserted().getFeatures().get(0).getProperties().get("free"));

    /** Check if v1 includes 500 inserts */
    List<Feature> insertedFeaturesV1 = ccol.getVersions().get(1).getInserted().getFeatures();
    HashSet<Integer> ids = new HashSet<>();
    for(int i=0; i < insertedFeaturesV1.size(); i++){
      int id = Integer.parseInt(insertedFeaturesV1.get(i).getId());
      assertEquals(true, id> 0 && id <=500);
      ids.add(id);
    }
    assertEquals(500, ids.size());

    /**
     * GET Version 14:
     * 4 deleted objects v14
     * */
    body =
            given().
                    accept(APPLICATION_VND_HERE_CHANGESET_COLLECTION).
                    headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
                    when().
                    get(getSpacesPath() + "/x-psql-test/history?startVersion=13&endVersion=14").
                    getBody().asString();

    ccol = XyzSerializable.deserialize(body);
    assertEquals(13, ccol.getStartVersion());
    assertEquals(14, ccol.getEndVersion());
    assertEquals(2, ccol.getVersions().size());
    assertEquals(0, ccol.getVersions().get(14).getInserted().getFeatures().size());
    assertEquals(0, ccol.getVersions().get(14).getUpdated().getFeatures().size());
    assertEquals(4, ccol.getVersions().get(14).getDeleted().getFeatures().size());

    /** Check if v1 includes the 4 deletes */
    List<Feature> deletedFeaturesV1 = ccol.getVersions().get(14).getDeleted().getFeatures();
    ids = new HashSet<>();
    for(int i=0; i < deletedFeaturesV1.size(); i++){
      int id = Integer.parseInt(deletedFeaturesV1.get(i).getId());
      assertEquals(true, id == 100 || id == 150 || id == 200 || id ==300);
      ids.add(id);
    }
    assertEquals(4, ids.size());
  }

  @Test
  public void readChangesetCollectionVersionWise() throws JsonProcessingException {
    /**
     * One Request to get it all
     * GET Version 1-14 (IDs are NOT Distinct; Deletes ARE included):
     *
     * 5000 inserted objects
     * 200 updated objects
     * 4 deleted objects
     * */

    int updatedCount = 0;
    int insertedCount = 0;
    int deletedCount = 0;

    for (int i=1; i <= 14; i++){
      String body =
              given().
                      accept(APPLICATION_VND_HERE_CHANGESET_COLLECTION).
                      headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
                      when().
                      get(getSpacesPath() + "/x-psql-test/history?startVersion="+i+"&endVersion="+i).
                      getBody().asString();

      ChangesetCollection ccol = XyzSerializable.deserialize(body);
      assertEquals(ccol.getEndVersion(), ccol.getStartVersion());

      if(i < 11){
        checkChangesetCollection(ccol, i, 500, 0, 0, true);
      }else if(i == 11 || i == 12){
        checkChangesetCollection(ccol, i, 0, 50, 0, false);
      }else if(i == 13){
        checkChangesetCollection(ccol, i, 0, 100, 0, true);
      }else if(i == 14){
        checkChangesetCollection(ccol, i, 0, 0, 4, null);
      }

      insertedCount += ccol.getVersions().get(i).getInserted().getFeatures().size();
      deletedCount += ccol.getVersions().get(i).getDeleted().getFeatures().size();
      updatedCount += ccol.getVersions().get(i).getUpdated().getFeatures().size();
    }
    assertEquals(5000, insertedCount);
    assertEquals(200, updatedCount);
    assertEquals(4, deletedCount);
  }

  @Test
  public void readChangeSetCollectionIterateThrough() throws JsonProcessingException {
    /**
     * Iterate in chunks with 1000 Features (1000,2000,3000,4000,5000,264)
     * GET Version 1-14 (IDs are NOT Distinct; Deletes ARE included ):
     *
     * 5000 inserted objects
     * 200 updated objects
     * 4 deleted objects
     *
     * 5204 objects overall (each operation)
     * */

    String npt = "";
    int updatedCount = 0;
    int insertedCount = 0;
    int deletedCount = 0;

    do{
      String body =
              given().
                      accept(APPLICATION_VND_HERE_CHANGESET_COLLECTION).
                      headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
                      when().
                      get(getSpacesPath() + "/x-psql-test/history?endVersion=25&limit=1000&pageToken="+npt).
                      getBody().asString();

      ChangesetCollection ccol = XyzSerializable.deserialize(body);
      npt = ccol.getNextPageToken();

      for (int n=ccol.getStartVersion(); n <= ccol.getEndVersion(); n++){
        insertedCount += ccol.getVersions().get(n).getInserted().getFeatures().size();
        deletedCount += ccol.getVersions().get(n).getDeleted().getFeatures().size();
        updatedCount += ccol.getVersions().get(n).getUpdated().getFeatures().size();
      }
    }while(npt != null);

    assertEquals(5000, insertedCount);
    assertEquals(200, updatedCount);
    assertEquals(4, deletedCount);
  }

  @Test
  public void readCompactChangesetWithVersionRange() throws JsonProcessingException {
    /**
     *
     * GET Version 11-14 (IDs ARE Distinct; Deletes ARE included):
     *
     * 98 updated objects
     * 4 deleted objects
     * */

    int updatedCount = 0;
    int insertedCount = 0;
    int deletedCount = 0;

    String body =
            given().
                    accept(APPLICATION_VND_HERE_COMPACT_CHANGESET).
                    headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
                    when().
                    get(getSpacesPath() + "/x-psql-test/history?startVersion=11&endVersion=14").
                    getBody().asString();

    CompactChangeset compc = XyzSerializable.deserialize(body);

    insertedCount += compc.getInserted().getFeatures().size();
    deletedCount += compc.getDeleted().getFeatures().size();
    updatedCount += compc.getUpdated().getFeatures().size();

    HashSet<Integer> ids = new HashSet<>();
    List<Integer> idList = Stream.of(compc.getInserted().getFeatures(), compc.getDeleted().getFeatures(), compc.getUpdated().getFeatures())
            .flatMap(Collection::stream)
            .map(e -> Integer.parseInt(e.getId()))
            .collect(Collectors.toList());
    ids.addAll(idList);

    /** Check distinct Ids */
    assertEquals(102, ids.size());

    assertEquals(0, insertedCount);
    assertEquals(98, updatedCount);
    assertEquals(4, deletedCount);

    /**
     *
     * GET Version 1-12 (IDs ARE Distinct; Deletes ARE included):
     *
     * 4900 inserted objects
     * 100 updated objects
     * */

    body =
            given().
                    accept(APPLICATION_VND_HERE_COMPACT_CHANGESET).
                    headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
                    when().
                    get(getSpacesPath() + "/x-psql-test/history?startVersion=1&endVersion=12").
                    getBody().asString();

    Typed resp = XyzSerializable.deserialize(body);
    if (!(resp instanceof CompactChangeset))
      throw new RuntimeException("Expected response to be a CompactChangeset, but got:" + resp.serialize());
    compc = (CompactChangeset) resp;
    assertEquals(4900,compc.getInserted().getFeatures().size());
    assertEquals(100,compc.getUpdated().getFeatures().size());

    for (Feature feature : compc.getInserted().getFeatures()) {
      int id = Integer.parseInt(feature.getId());
      assertTrue(feature.getProperties().get("free"));
      assertTrue((id < 100 || id >= 200));
    }
    for (Feature feature : compc.getUpdated().getFeatures()) {
      int id = Integer.parseInt(feature.getId());
      assertFalse(feature.getProperties().get("free"));
      assertTrue((id >= 100 && id < 200));
    }
  }

  @Test
  public void readCompactChangesetIterateThrough() throws JsonProcessingException {
    /**
     * Iterate in chunks with 1000 Features (1000,2000,3000,4000,5000)
     * GET Version 1-14 (IDs ARE Distinct; Deletes ARE included):
     *
     * 4798 inserted objects
     * 198 updated objects
     * 4 deleted objects
     *
     * 5000 objects overall (id = distinct + deletes)
     * */

    String npt = "";
    int updatedCount = 0;
    int insertedCount = 0;
    int deletedCount = 0;
    HashSet<Integer> ids = new HashSet<>();

    do{
      String body =
              given().
                      accept(APPLICATION_VND_HERE_COMPACT_CHANGESET).
                      headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
                      when().
                      get(getSpacesPath() + "/x-psql-test/history?endVersion=14&limit=1000&pageToken="+npt).
                      getBody().asString();

      CompactChangeset compc = XyzSerializable.deserialize(body);
      npt = compc.getNextPageToken();

      insertedCount += compc.getInserted().getFeatures().size();
      deletedCount += compc.getDeleted().getFeatures().size();
      updatedCount += compc.getUpdated().getFeatures().size();
      List<Integer> idList = Stream.of(compc.getInserted().getFeatures(), compc.getDeleted().getFeatures(), compc.getUpdated().getFeatures())
              .flatMap(Collection::stream)
              .map(e -> Integer.parseInt(e.getId()))
              .collect(Collectors.toList());
      ids.addAll(idList);

      checkCompactChangesetVersionIntegrity(compc.getInserted().getFeatures());
      checkCompactChangesetVersionIntegrity(compc.getUpdated().getFeatures());

      checkFeatureListContent(compc.getInserted().getFeatures(), true);
      checkFeatureListContent(compc.getUpdated().getFeatures(), true);
      checkFeatureListContent(compc.getDeleted().getFeatures(), true);
    }while(npt != null);

    /** Check distinct ids*/
    assertEquals(5000, ids.size());
    for (int i=1; i<=ids.size(); i++)
      assertTrue(ids.contains(i));

    assertEquals(4898, insertedCount);
    assertEquals(98, updatedCount);
    assertEquals(4, deletedCount);
  }

  @Test
  public void iterateOverVersion() throws JsonProcessingException {
    /**
     * Iterate in chunks with 1000 Features (1000,2000,3000,4000,5000)
     * GET Version <=14 (IDs ARE Distinct + Deletes are NOT included):
     *
     * 4798 inserted objects
     * 198 updated objects
     * 4 deleted objects
     * */
    String handle = "";
    HashSet<Integer> ids = new HashSet<>();

    do{
      String body =
              given().
                      accept(APPLICATION_GEO_JSON).
                      headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
                      when().
                      get(getSpacesPath() + "/x-psql-test/iterate?version=25&limit=1000&handle="+handle).
                      getBody().asString();

      FeatureCollection fc = XyzSerializable.deserialize(body);
      checkFeatureListContent(fc.getFeatures(), false);

      List<Integer> idList = Stream.of(fc.getFeatures())
              .flatMap(Collection::stream)
              .map(e -> Integer.parseInt(e.getId()))
              .collect(Collectors.toList());
      ids.addAll(idList);

      handle = fc.getHandle();

    }while(handle != null);

    /** Check distinct ids*/
    assertEquals(4996, ids.size());
    assertFalse(ids.contains(100));
    /** Check if deletes are not included */
    assertFalse(ids.contains(150));
    assertFalse(ids.contains(200));
    assertFalse(ids.contains(300));

    String body =
            given().
                    accept(APPLICATION_GEO_JSON).
                    headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
                    when().
                    get(getSpacesPath() + "/x-psql-test/iterate?version=3&limit=5000").
                    getBody().asString();

    FeatureCollection fc = XyzSerializable.deserialize(body);
    checkFeatureListContent(fc.getFeatures(), false);
    assertEquals(1500, fc.getFeatures().size());
  }

  @Test
  public void checkRootPropertyAndNullGeometry() throws JsonProcessingException {
    FeatureCollection fe = XyzSerializable.deserialize("{\"type\": \"FeatureCollection\", \"features\" : [{\"id\": \"ID1234\", \"type\": \"Feature\", \"f_root\" : \"bar\", \"properties\":{\"f\":1}}]}");
    /** got written in v15 */

    ValidatableResponse response = given().
            accept(APPLICATION_GEO_JSON).
            contentType(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            body(fe.serialize()).
            when().
            post(getSpacesPath() + "/x-psql-test/features").
            then().
            statusCode(OK.code());

    /** Check CompactChangeset */
    String body =
            given().
                    accept(APPLICATION_VND_HERE_COMPACT_CHANGESET).
                    headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
                    when().
                    get(getSpacesPath() + "/x-psql-test/history?startVersion=15").
                    getBody().asString();

    CompactChangeset compactC = XyzSerializable.deserialize(body);
    assertNotNull(compactC.getInserted());
    assertEquals(1,compactC.getInserted().getFeatures().size());
    assertNull(compactC.getInserted().getFeatures().get(0).getGeometry());
    assertNotEquals(-1,XyzSerializable.serialize(compactC.getInserted().getFeatures().get(0)).indexOf("f_root"));

    /** Check ChangesetCollection */
    body =
            given().
                    accept(APPLICATION_VND_HERE_CHANGESET_COLLECTION).
                    headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
                    when().
                    get(getSpacesPath() + "/x-psql-test/history?startVersion=15").
                    getBody().asString();

    ChangesetCollection changeCol = XyzSerializable.deserialize(body);
    assertNotNull(changeCol.getVersions().get(15));
    assertNotNull(changeCol.getVersions().get(15).getInserted());
    assertEquals(1,changeCol.getVersions().get(15).getInserted().getFeatures().size());
    assertNull(changeCol.getVersions().get(15).getInserted().getFeatures().get(0).getGeometry());
    assertNotEquals(-1,XyzSerializable.serialize(changeCol.getVersions().get(15).getInserted().getFeatures().get(0)).indexOf("f_root"));

    /** Check Iteration over version */
    body =
            given().
                    accept(APPLICATION_GEO_JSON).
                    headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
                    when().
                    get(getSpacesPath() + "/x-psql-test/iterate?version=15").
                    getBody().asString();

    Typed resp = XyzSerializable.deserialize(body);
    if (!(resp instanceof FeatureCollection))
      throw new RuntimeException("Expected response to be a FeatureCollection, but got:" + resp.serialize());
    FeatureCollection fc = (FeatureCollection) resp;

    assertNotNull(fc.getFeatures());
    boolean foundFeature=false;
    for (Feature f : fc.getFeatures()){
      if(f.getId().equalsIgnoreCase("ID1234")){
        assertNull(f.getGeometry());
        assertNotEquals(-1, XyzSerializable.serialize(f).indexOf("f_root"));
        foundFeature=true;
      }
    }
    assertTrue(foundFeature);
  }

  public void writeFeatures(int totalCount, int chunksize, int startId, boolean free){
    for (int i=startId; i<totalCount+startId; i+=chunksize){
      FeatureCollection featureCollection = generateEVFeatures(i, chunksize, free);
      uploadData(featureCollection);
    }
  }

  private void uploadData(FeatureCollection featureCollection) {
    given().
            accept(APPLICATION_GEO_JSON).
            contentType(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            body(featureCollection.serialize()).
            when().
            post(getSpacesPath() + "/x-psql-test/features").
            then().
            statusCode(OK.code());
  }

  private void deleteFeatures(String idList) {
    given()
            .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
            .when()
            .delete(getSpacesPath() + "/x-psql-test/features?"+idList)
            .then()
            .statusCode(NO_CONTENT.code());
  }

  private void checkChangesetCollection(ChangesetCollection ccol, int version, int expectedInserted, int expectedUpdated, int expectedDeleted, Boolean isFree)
          throws JsonProcessingException {
    assertEquals(expectedInserted,ccol.getVersions().get(version).getInserted().getFeatures().size());
    assertEquals(expectedUpdated,ccol.getVersions().get(version).getUpdated().getFeatures().size());
    assertEquals(expectedDeleted,ccol.getVersions().get(version).getDeleted().getFeatures().size());

    if(isFree != null) {
      List<Feature> features = ccol.getVersions().get(version).getInserted().getFeatures();
      if(expectedUpdated != 0)
        features = ccol.getVersions().get(version).getUpdated().getFeatures();
      for (Feature f : features) {
        assertEquals(isFree, f.getProperties().get("free"));
      }
    }
  }

  private void checkCompactChangesetVersionIntegrity(List<Feature> fList) {
    for (Feature f : fList) {
      int v = f.getProperties().getXyzNamespace().getVersion();
      if(v < 11) {
        assertEquals(true, f.getProperties().get("free"));
      }if(v == 11 || v == 12){
        assertEquals(false, f.getProperties().get("free"));
      }else if(v == 13){
        assertEquals(true, f.getProperties().get("free"));
      }
    }
  }

  private void checkFeatureListContent(List<Feature> fList, boolean checkDeleteFlag) {
    for (Feature f : fList) {
      int id = Integer.parseInt(f.getId());
      boolean isFree = f.getProperties().get("free");
      assertTrue(isFree);

      if(checkDeleteFlag) {
        boolean isDeleted = f.getProperties().getXyzNamespace().isDeleted();

        if (id == 100 || id == 150 || id == 200 || id == 300)
          assertTrue(isDeleted);
      }
    }
  }
}
