/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.changesets.ChangesetCollection;
import com.here.xyz.responses.changesets.CompactChangeset;
import com.jayway.restassured.response.ValidatableResponse;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.here.xyz.hub.rest.Api.HeaderValues.*;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
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
        .when().post("/spaces").then();

    response.statusCode(OK.code())
        .body("id", equalTo("x-psql-test"))
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
            get("/spaces/x-psql-test/history").
            then().
            statusCode(OK.code()).
            body("startVersion", equalTo(0)).
            body("endVersion", equalTo(0)).
            body("versions", equalTo(new HashMap()));

    given().
            accept(APPLICATION_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            when().
            get("/spaces/x-psql-test/history/statistics").
            then().
            statusCode(OK.code()).
            body("count.value", equalTo(0)).
            body("maxVersion.value", equalTo(0));

    /**
     * Perform:
     * Insert id: 1-500 (v1)
     * Insert id: 501-100 (v2)
     * Insert id: 1001-1500 (v3)
     * ...
     * Insert id: 4501-5000 (v10)
     *
     * */
    writeFeatures(5000, 500, 1);


    /**
     * Perform:
     * Update id: 100-150 (v.11)
     * Update id: 150-200 (v.12)
     * Update id: 100-200 (v.13)
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
            get("/spaces/x-psql-test/features/5000").
            then().
            statusCode(OK.code()).
            body("properties.@ns:com:here:xyz.version", equalTo(10));

    /** Write two new versions (11,12) each with 50 objects*/
    writeFeatures(100,50,100);

    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/features/100").
            then().
            statusCode(OK.code()).
            body("properties.@ns:com:here:xyz.version", equalTo(11));

    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/features/150").
            then().
            statusCode(OK.code()).
            body("properties.@ns:com:here:xyz.version", equalTo(12));

    /** Write to new versions (13) with 100 objects*/
    writeFeatures(100,100,100);

    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/features/150").
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
            get("/spaces/x-psql-test/features?"+idList).
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(0));
  }

  @Test
  public void readChangeSetCollectionWithVersionRange() throws JsonProcessingException {
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
            get("/spaces/x-psql-test/history?vStart=1&vEnd=2").
            getBody().asString();

    ChangesetCollection cc = XyzSerializable.deserialize(body);
    assertEquals(1, cc.getStartVersion());
    assertEquals(2, cc.getEndVersion());
    assertEquals(2, cc.getVersions().size());
    assertEquals(500, cc.getVersions().get(1).getInserted().getFeatures().size());
    assertEquals(500, cc.getVersions().get(2).getInserted().getFeatures().size());

    /** Check if v1 includes 500 inserts */
    List<Feature> insertedFeaturesV1 = cc.getVersions().get(1).getInserted().getFeatures();
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
                    get("/spaces/x-psql-test/history?vStart=13&vEnd=14").
                    getBody().asString();

    cc = XyzSerializable.deserialize(body);
    assertEquals(13, cc.getStartVersion());
    assertEquals(14, cc.getEndVersion());
    assertEquals(2, cc.getVersions().size());
    assertEquals(0, cc.getVersions().get(14).getInserted().getFeatures().size());
    assertEquals(0, cc.getVersions().get(14).getUpdated().getFeatures().size());
    assertEquals(4, cc.getVersions().get(14).getDeleted().getFeatures().size());

    /** Check if v1 includes the 4 deletes */
    List<Feature> deletedFeaturesV1 = cc.getVersions().get(14).getDeleted().getFeatures();
    ids = new HashSet<>();
    for(int i=0; i < deletedFeaturesV1.size(); i++){
      int id = Integer.parseInt(deletedFeaturesV1.get(i).getId());
      assertEquals(true, id == 100 || id == 150 || id == 200 || id ==300);
      ids.add(id);
    }
    assertEquals(4, ids.size());
  }

  @Test
  public void readChangeSetCollectionVersionWise() throws JsonProcessingException {
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
      Integer curVersion = (i+1);
      String body =
              given().
                      accept(APPLICATION_VND_HERE_CHANGESET_COLLECTION).
                      headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
                      when().
                      get("/spaces/x-psql-test/history?vStart="+i+"&vEnd="+i).
                      getBody().asString();

      ChangesetCollection cc = XyzSerializable.deserialize(body);
      assertEquals(cc.getEndVersion(), cc.getStartVersion());
//      assertTrue(cc.getVersions().containsKey(curVersion));

      insertedCount += cc.getVersions().get(i).getInserted().getFeatures().size();
      deletedCount += cc.getVersions().get(i).getDeleted().getFeatures().size();
      updatedCount += cc.getVersions().get(i).getUpdated().getFeatures().size();
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
                      get("/spaces/x-psql-test/history?vEnd=25&limit=1000&nextPageToken="+npt).
                      getBody().asString();

      ChangesetCollection cc = XyzSerializable.deserialize(body);
      npt = cc.getNextPageToken();

      for (int n=cc.getStartVersion(); n <= cc.getEndVersion(); n++){
        insertedCount += cc.getVersions().get(n).getInserted().getFeatures().size();
        deletedCount += cc.getVersions().get(n).getDeleted().getFeatures().size();
        updatedCount += cc.getVersions().get(n).getUpdated().getFeatures().size();
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
                    get("/spaces/x-psql-test/history?vStart=11&vEnd=14").
                    getBody().asString();

    CompactChangeset cc = XyzSerializable.deserialize(body);

    insertedCount += cc.getInserted().getFeatures().size();
    deletedCount += cc.getDeleted().getFeatures().size();
    updatedCount += cc.getUpdated().getFeatures().size();

    HashSet<Integer> ids = new HashSet<>();
    List<Integer> idList = Stream.of(cc.getInserted().getFeatures(), cc.getDeleted().getFeatures(), cc.getUpdated().getFeatures())
            .flatMap(Collection::stream)
            .map(e -> Integer.parseInt(e.getId()))
            .collect(Collectors.toList());
    ids.addAll(idList);

    /** Check distinct Ids */
    assertEquals(102, ids.size());

    assertEquals(0, insertedCount);
    assertEquals(98, updatedCount);
    assertEquals(4, deletedCount);
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
                      get("/spaces/x-psql-test/history?vEnd=25&limit=1000&nextPageToken="+npt).
                      getBody().asString();

      CompactChangeset cc = XyzSerializable.deserialize(body);
      npt = cc.getNextPageToken();

      insertedCount += cc.getInserted().getFeatures().size();
      deletedCount += cc.getDeleted().getFeatures().size();
      updatedCount += cc.getUpdated().getFeatures().size();
      List<Integer> idList = Stream.of(cc.getInserted().getFeatures(), cc.getDeleted().getFeatures(), cc.getUpdated().getFeatures())
              .flatMap(Collection::stream)
              .map(e -> Integer.parseInt(e.getId()))
              .collect(Collectors.toList());
      ids.addAll(idList);
    }while(npt != null);

    /** Check distinct ids*/
    assertEquals(5000, ids.size());

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
                      get("/spaces/x-psql-test/iterate?v=25&limit=1000&handle="+handle).
                      getBody().asString();

      FeatureCollection fc = XyzSerializable.deserialize(body);
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
                    get("/spaces/x-psql-test/iterate?v=3&limit=5000").
                    getBody().asString();

    FeatureCollection fc = XyzSerializable.deserialize(body);
    assertEquals(1500, fc.getFeatures().size());
  }

  public void writeFeatures(int totalCount, int chunksize, int startId){
    for (int i=startId; i<totalCount+startId; i+=chunksize){
      FeatureCollection featureCollection = generateEVFeatures(i, chunksize);
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
            post("/spaces/x-psql-test/features").
            then().
            statusCode(OK.code());
  }

  private void deleteFeatures(String idList) {
    given()
            .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
            .when()
            .delete("/spaces/x-psql-test/features?"+idList)
            .then()
            .statusCode(NO_CONTENT.code());
  }
}
