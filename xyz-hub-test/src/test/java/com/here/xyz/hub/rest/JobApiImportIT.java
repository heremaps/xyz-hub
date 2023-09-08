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

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.ImportObject;
import com.here.xyz.httpconnector.util.jobs.Job;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.net.URL;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class JobApiImportIT extends JobApiIT {
    protected String testImportJobId = "x-test-import-job";
    protected static String scope = "import";

    @BeforeClass
    public static void setup() {
        cleanUpEnv(scope);
    }

    @AfterClass
    public static void cleanAfterClass(){
        cleanUpEnv(scope);
    }

    @Before
    public void setupAndCleanBefore(){
        deleteAllJobsOnSpace(getScopedSpaceId(testSpaceId1, scope));
        removeSpace(getScopedSpaceId(testSpaceId1, scope));

        /** Create empty test space */
        createSpaceWithCustomStorage(getScopedSpaceId(testSpaceId1, scope), "psql", null);
    }

    @After
    public void cleanAfter(){
        deleteAllJobsOnSpace(getScopedSpaceId(testSpaceId1, scope));
        removeSpace(getScopedSpaceId(testSpaceId1, scope));
    }

    @Test
    public void validWKBImport() throws Exception {
        /** Create job */
        Import job = (Import) createTestJobWithId(getScopedSpaceId(testSpaceId1, scope), testImportJobId, JobApiIT.Type.Import, Job.CSVFormat.JSON_WKB);

        /** Create Upload URL */
        String resp = given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/"+job.getId()+"/execute?command=createUploadUrl")
                .getBody().asString();
        String url = new JsonObject(resp).getString("url");

        /** Upload File */
        uploadDummyFile(new URL(url), 0);

        /** start import */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/"+job.getId()+"/execute?command=start")
                .then()
                .statusCode(NO_CONTENT.code());

        /** Poll status */
        pollStatus(getScopedSpaceId(testSpaceId1, scope), job.getId(), Job.Status.finalized, Job.Status.failed);

        /** Check Feature in Space */
        given()
                .accept(APPLICATION_GEO_JSON)
                .contentType(APPLICATION_GEO_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
                .when()
                .get("/spaces/" + getScopedSpaceId(testSpaceId1, scope) +"/search")
                .then()
                .body("features.size()", equalTo(1));
    }

    @Test
    public void duplicateImport() throws Exception {

        /** Create job */
        Import job = (Import) createTestJobWithId(getScopedSpaceId(testSpaceId1, scope), testImportJobId, JobApiIT.Type.Import, Job.CSVFormat.JSON_WKB);

        for(int i=0; i<3 ; i++) {
            /** Create Upload URL */
            String resp = given()
                    .accept(APPLICATION_JSON)
                    .contentType(APPLICATION_JSON)
                    .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                    .post("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/" + job.getId() + "/execute?command=createUploadUrl")
                    .getBody().asString();
            String url = new JsonObject(resp).getString("url");

            /** Upload File */
            uploadDummyFile(new URL(url), 11);
        }

        /** start import */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/"+job.getId()+"/execute?command=start")
                .then()
                .statusCode(NO_CONTENT.code());

        /** Poll status */
        pollStatus(getScopedSpaceId(testSpaceId1, scope), job.getId(), Job.Status.failed, Job.Status.finalized);

        /** Check Feature in Space */
        given()
                .accept(APPLICATION_GEO_JSON)
                .contentType(APPLICATION_GEO_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
                .when()
                .get("/spaces/" + getScopedSpaceId(testSpaceId1, scope) +"/search")
                .then()
                .body("features.size()", equalTo(1));

        job = (Import)getJob(getScopedSpaceId(testSpaceId1, scope), job.getId());

        assertTrue(job.getErrorDescription().equalsIgnoreCase(Import.ERROR_DESCRIPTION_IDS_NOT_UNIQUE));

        int foundFailed = 0, foundImported = 0;
        boolean foundDetails = false;

        for(String key : job.getImportObjects().keySet()){
            if(job.getImportObjects().get(key).getStatus().equals(ImportObject.Status.failed))
                foundFailed++;
            if(job.getImportObjects().get(key).getStatus().equals(ImportObject.Status.imported))
                foundImported++;
            if(job.getImportObjects().get(key).getDetails() != null
                    && job.getImportObjects().get(key).getDetails().equalsIgnoreCase("1 rows imported"))
                foundDetails = true;
        }

        assertEquals(2,foundFailed);
        assertEquals(1,foundImported);
        assertEquals(true, foundDetails);
    }

    @Test
    public void validGEOJSONImport() throws Exception {
        /** Create job */
        Import job = (Import) createTestJobWithId(getScopedSpaceId(testSpaceId1, scope), testImportJobId, JobApiIT.Type.Import, Job.CSVFormat.GEOJSON);

        /** Create Upload URL */
        String resp = given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/"+job.getId()+"/execute?command=createUploadUrl")
                .getBody().asString();
        String url = new JsonObject(resp).getString("url");

        /** Upload File */
        uploadDummyFile(new URL(url), 10);

        /** start import */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/"+job.getId()+"/execute?command=start")
                .then()
                .statusCode(NO_CONTENT.code());

        /** Poll status */
        pollStatus(getScopedSpaceId(testSpaceId1, scope), job.getId(), Job.Status.finalized, Job.Status.failed);

        /** Check Feature in Space */
        given()
                .accept(APPLICATION_GEO_JSON)
                .contentType(APPLICATION_GEO_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
                .when()
                .get("/spaces/" + getScopedSpaceId(testSpaceId1, scope) +"/search")
                .then()
                .body("features.size()", equalTo(1));
    }

    @Test
    public void validMultiUrlImport() throws Exception {

        /** Create job */
        Import job = (Import) createTestJobWithId(getScopedSpaceId(testSpaceId1, scope), testImportJobId, JobApiIT.Type.Import, Job.CSVFormat.JSON_WKB);

        /** Create single Upload URL */
        String resp1 = given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/"+job.getId()+"/execute?command=createUploadUrl")
                .getBody().asString();
        String url = new JsonObject(resp1).getString("url");
        uploadDummyFile(new URL(url), 0);

        /** Create multiple upload URLs */
        int urlCount = 2;
        String resp2 = given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/"+job.getId()+"/execute?command=createUploadUrl&urlCount="+urlCount)
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
                .post("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/"+job.getId()+"/execute?command=createUploadUrl")
                .getBody().asString();
        urls = new JsonObject(resp3).getJsonArray("urls");
        assertEquals(1, urls.size());
        uploadDummyFile(new URL(urls.getString(0)), 0);

        /** Check import objects size */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .get("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/"+job.getId())
                .then()
                .statusCode(OK.code())
                .body("importObjects.size()", equalTo(4));


        /** start import */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/"+job.getId()+"/execute?command=start")
                .then()
                .statusCode(NO_CONTENT.code());

        /** Poll status */
        pollStatus(getScopedSpaceId(testSpaceId1, scope), job.getId(), Job.Status.finalized, Job.Status.failed);

        /** Check Feature in Space */
        given()
                .accept(APPLICATION_GEO_JSON)
                .contentType(APPLICATION_GEO_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
                .when()
                .get("/spaces/" + getScopedSpaceId(testSpaceId1, scope) +"/search")
                .then()
                .body("features.size()", equalTo(4));
    }

    @Test
    public void validationErrorImport() throws Exception {
        /** Create job */
        Import job = (Import) createTestJobWithId(getScopedSpaceId(testSpaceId1, scope), testImportJobId, JobApiIT.Type.Import, Job.CSVFormat.JSON_WKB);

        /** Create Upload URL */
        String resp = given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/"+job.getId()+"/execute?command=createUploadUrl")
                .getBody().asString();
        String url = new JsonObject(resp).getString("url");

        /** Upload File */
        uploadDummyFile(new URL(url), 1);

        /** retry import */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/"+job.getId()+"/execute?command=start")
                .then()
                .statusCode(NO_CONTENT.code());

        /** Poll status */
        pollStatus(getScopedSpaceId(testSpaceId1, scope), job.getId(), Job.Status.failed, Job.Status.finalized);

        /** Upload File */
        uploadDummyFile(new URL(url), 2);

        /** retry import */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/"+job.getId()+"/execute?command=retry")
                .then()
                .statusCode(NO_CONTENT.code());

        /** Poll status */
        pollStatus(getScopedSpaceId(testSpaceId1, scope), job.getId(), Job.Status.failed, Job.Status.finalized);

        /** Upload File */
        uploadDummyFile(new URL(url), 3);

        /** retry import */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/"+job.getId()+"/execute?command=retry")
                .then()
                .statusCode(NO_CONTENT.code());

        /** Poll status */
        pollStatus(getScopedSpaceId(testSpaceId1, scope), job.getId(), Job.Status.failed, Job.Status.finalized);
    }

    @Test
    public void startIncompleteImportJob() throws InterruptedException {
        /** Create job */
        Import job = (Import) createTestJobWithId(getScopedSpaceId(testSpaceId1, scope), testImportJobId, JobApiIT.Type.Import, Job.CSVFormat.JSON_WKB);

        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/"+job.getId()+"/execute?command=start")
                .then()
                .statusCode(NO_CONTENT.code());

        pollStatus(getScopedSpaceId(testSpaceId1, scope), job.getId(), Job.Status.failed, Job.Status.finalized);
        job = (Import) getJob(getScopedSpaceId(testSpaceId1, scope), job.getId());

        assertEquals(job.getErrorDescription(), Import.ERROR_DESCRIPTION_UPLOAD_MISSING );
        assertEquals(job.getErrorType(), Import.ERROR_TYPE_VALIDATION_FAILED);
    }

    /** For local testing */
    //@Test
    public void validBigWKBImport() throws Exception {
        /** Defines how many upload chunks we want to have */
        int chunkSize = 100;
        /** Create job */
        Import job = (Import) createTestJobWithId(getScopedSpaceId(testSpaceId1, scope), testImportJobId, JobApiIT.Type.Import, Job.CSVFormat.JSON_WKB);

        for (int i=0 ;  i< chunkSize; i++){
            /** Create Upload URL */
            String resp = given()
                    .accept(APPLICATION_JSON)
                    .contentType(APPLICATION_JSON)
                    .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                    .post("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/"+job.getId()+"/execute?command=createUploadUrl")
                    .getBody().asString();
            String url = new JsonObject(resp).getString("url");

            /** Upload File */
            uploadDummyFile(new URL(url), 0);
        }

        /** start import */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/"+job.getId()+"/execute?command=start")
                .then()
                .statusCode(NO_CONTENT.code());

        /** Poll status */
        pollStatus(getScopedSpaceId(testSpaceId1, scope), job.getId(), Job.Status.finalized, Job.Status.failed);

        /** Check Feature in Space */
        given()
                .accept(APPLICATION_GEO_JSON)
                .contentType(APPLICATION_GEO_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
                .when()
                .get("/spaces/" + getScopedSpaceId(testSpaceId1, scope) +"/search")
                .then()
                .body("features.size()", equalTo(chunkSize));
    }

    //    @Test
    public void validBigWKBImportFromLocal() throws Exception {
        String newSpace = "new_empty_space";
        String importId = "manual_big_import";

        removeSpace(newSpace);
        deleteAllJobsOnSpace(newSpace);
        createSpaceWithCustomStorage(newSpace, "psql", null);
        /** Create job */
        Job job = createTestJobWithId(newSpace, importId, JobApiIT.Type.Import, Job.CSVFormat.JSON_WKB);

        String resp = given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + newSpace + "/job/"+job.getId()+"/execute?command=createUploadUrl")
                .getBody().asString();
        String url = new JsonObject(resp).getString("url");

        /** Upload File */
        uploadLocalFile(new URL(url), "/home/mchrza/Downloads/ev-export/other_export.csv");

        /** Create Upload URL */
        resp = given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + newSpace + "/job/"+job.getId()+"/execute?command=createUploadUrl")
                .getBody().asString();
        url = new JsonObject(resp).getString("url");

        /** Upload File */
        uploadLocalFile(new URL(url), "file_path");

        /** start import */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + newSpace + "/job/"+job.getId()+"/execute?command=start")
                .then()
                .statusCode(NO_CONTENT.code());

        /** Poll status */
        pollStatus(newSpace, job.getId(), Job.Status.finalized, Job.Status.failed);

        deleteAllJobsOnSpace(newSpace);
        removeSpace(newSpace);
    }
}
