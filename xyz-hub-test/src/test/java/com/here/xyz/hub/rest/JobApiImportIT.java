/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.Job;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URL;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;

public class JobApiImportIT extends JobApiIT {
    String testSpace = testSpaceId1;
    protected String testImportJobId = "x-test-import-job";

    @BeforeClass
    public static void setup() { }

    @Before
    public void clean(){
        createSpaceWithCustomStorage(testSpaceId1, "psql", null);
        deleteAllJobsOnSpace(testSpaceId1);
    }

    @After
    public void test(){
        deleteAllJobsOnSpace(testSpaceId1);
        removeSpace(testSpaceId1);
    }

    @Test
    public void validWKBImport() throws Exception {

        /** Create job */
        createTestJobWithId(testSpace, testImportJobId, Job.Type.Import, Job.CSVFormat.JSON_WKB);

        /** Create Upload URL */
        String resp = given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + testSpace + "/job/"+testImportJobId+"/execute?command=createUploadUrl")
                .getBody().asString();
        String url = new JsonObject(resp).getString("url");

        /** Upload File */
        uploadDummyFile(new URL(url), 0);

        /** start import */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + testSpace + "/job/"+testImportJobId+"/execute?command=start")
                .then()
                .statusCode(NO_CONTENT.code());

        /** Poll status */
        pollStatus(testSpace, testImportJobId, Job.Status.finalized, Job.Status.failed);

        /** Check Feature in Space */
        given()
                .accept(APPLICATION_GEO_JSON)
                .contentType(APPLICATION_GEO_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
                .when()
                .get("/spaces/" + testSpace +"/search")
                .then()
                .body("features.size()", equalTo(1));

        /** Delete Job */
        deleteJob(testImportJobId, testSpace);
    }

    @Test
    public void validGEOJSONImport() throws Exception {
        /** Create job */
        createTestJobWithId(testSpace, testImportJobId, Job.Type.Import, Job.CSVFormat.GEOJSON);

        /** Create Upload URL */
        String resp = given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + testSpace + "/job/"+testImportJobId+"/execute?command=createUploadUrl")
                .getBody().asString();
        String url = new JsonObject(resp).getString("url");

        /** Upload File */
        uploadDummyFile(new URL(url), 10);

        /** start import */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + testSpace + "/job/"+testImportJobId+"/execute?command=start")
                .then()
                .statusCode(NO_CONTENT.code());

        /** Poll status */
        pollStatus(testSpace, testImportJobId, Job.Status.finalized, Job.Status.failed);

        /** Check Feature in Space */
        given()
                .accept(APPLICATION_GEO_JSON)
                .contentType(APPLICATION_GEO_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
                .when()
                .get("/spaces/" + testSpace +"/search")
                .then()
                .body("features.size()", equalTo(1));

        /** Delete Job */
        deleteJob(testImportJobId, testSpace);
    }

    @Test
    public void validMultiUrlImport() throws Exception {

        /** Create job */
        createTestJobWithId(testSpace, testImportJobId, Job.Type.Import, Job.CSVFormat.JSON_WKB);

        /** Create single Upload URL */
        String resp1 = given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + testSpace + "/job/"+testImportJobId+"/execute?command=createUploadUrl")
                .getBody().asString();
        String url = new JsonObject(resp1).getString("url");
        uploadDummyFile(new URL(url), 0);

        /** Create multiple upload URLs */
        int urlCount = 2;
        String resp2 = given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + testSpace + "/job/"+testImportJobId+"/execute?command=createUploadUrl&urlCount="+urlCount)
                .getBody().asString();
        JsonArray urls = new JsonObject(resp2).getJsonArray("urls");
        assertEquals(urlCount, urls.size());

        for(int i=0; i<urls.size(); i++)
            uploadDummyFile(new URL(urls.getString(i)), 0);

        /** Create single Upload URL */
        String resp3 = given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + testSpace + "/job/"+testImportJobId+"/execute?command=createUploadUrl")
                .getBody().asString();
        urls = new JsonObject(resp3).getJsonArray("urls");
        assertEquals(1, urls.size());
        uploadDummyFile(new URL(urls.getString(0)), 0);

        /** Check import objects size */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .get("/spaces/" + testSpace + "/job/"+testImportJobId)
                .then()
                .statusCode(OK.code())
                .body("importObjects.size()", equalTo(4));


        /** start import */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + testSpace + "/job/"+testImportJobId+"/execute?command=start")
                .then()
                .statusCode(NO_CONTENT.code());

        /** Poll status */
        pollStatus(testSpace, testImportJobId, Job.Status.finalized, Job.Status.failed);

        /** Check Feature in Space */
        given()
                .accept(APPLICATION_GEO_JSON)
                .contentType(APPLICATION_GEO_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
                .when()
                .get("/spaces/" + testSpace +"/search")
                .then()
                .body("features.size()", equalTo(4));

        /** Delete Job */
        deleteJob(testImportJobId, testSpace);
    }

    @Test
    public void validationErrorImport() throws Exception {
        /** Create job */
        createTestJobWithId(testSpace, testImportJobId, Job.Type.Import, Job.CSVFormat.JSON_WKB);

        /** Create Upload URL */
        String resp = given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + testSpace + "/job/"+testImportJobId+"/execute?command=createUploadUrl")
                .getBody().asString();
        String url = new JsonObject(resp).getString("url");

        /** Upload File */
        uploadDummyFile(new URL(url), 1);

        /** retry import */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + testSpace + "/job/"+testImportJobId+"/execute?command=start")
                .then()
                .statusCode(NO_CONTENT.code());

        /** Poll status */
        pollStatus(testSpace, testImportJobId, Job.Status.failed, Job.Status.finalized);

        /** Upload File */
        uploadDummyFile(new URL(url), 2);
        Thread.sleep(500);

        /** retry import */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + testSpace + "/job/"+testImportJobId+"/execute?command=retry")
                .then()
                .statusCode(NO_CONTENT.code());

        /** Poll status */
        pollStatus(testSpace, testImportJobId, Job.Status.failed, Job.Status.finalized);

        /** Upload File */
        uploadDummyFile(new URL(url), 3);

        /** retry import */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + testSpace + "/job/"+testImportJobId+"/execute?command=retry")
                .then()
                .statusCode(NO_CONTENT.code());

        /** Poll status */
        pollStatus(testSpace, testImportJobId, Job.Status.failed, Job.Status.finalized);

        /** Delete Job */
        deleteJob(testImportJobId, testSpace);
    }

    @Test
    public void startIncompleteImportJob() throws InterruptedException {
        /** Create job */
        createTestJobWithId(testSpace, testImportJobId, Job.Type.Import, Job.CSVFormat.JSON_WKB);

        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + testSpace + "/job/"+testImportJobId+"/execute?command=start")
                .then()
                .statusCode(NO_CONTENT.code());

        Job job = pollStatus(testSpace, testImportJobId, Job.Status.failed, Job.Status.finalized);

        assertEquals(job.getErrorDescription(), Import.ERROR_DESCRIPTION_UPLOAD_MISSING );
        assertEquals(job.getErrorType(), Import.ERROR_TYPE_VALIDATION_FAILED);

        /** Delete Job */
        deleteJob(testImportJobId, testSpace);
    }
}
