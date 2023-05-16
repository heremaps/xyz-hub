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

import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.jayway.restassured.response.Response;
import com.jayway.restassured.response.ValidatableResponse;
import io.vertx.core.json.jackson.DatabindCodec;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class JobApiIT extends TestSpaceWithFeature {

    protected static String testSpaceId1 = "x-psql-job-test1";
    protected static String testSpaceId2 = "x-psql-job-test2";
    protected static String testSpaceId2Ext = "x-psql-job-test2-ext";
    protected static String testSpaceId2ExtExt = "x-psql-job-test2-ext-ext";
    protected static String testJobId = "x-test-job";

    @BeforeClass
    public static void setup() {
        /** Create empty test space */
        createSpaceWithCustomStorage(testSpaceId1, "psql", null);

        /** Create test space with content */
        createSpaceWithCustomStorage(testSpaceId2, "psql", null);
        addFeatures(testSpaceId2, "/xyz/hub/mixedGeometryTypes.json", 11);
        /** Create L1 composite space with content*/
        createSpaceWithExtension(testSpaceId2);
        addFeatures(testSpaceId2Ext,"/xyz/hub/processedData.json",252);
        /** Create L2 composite with content */
        createSpaceWithExtension(testSpaceId2Ext);

        /** Modify one feature on L1 composite space */
        postFeature(testSpaceId2Ext, newFeature()
                        .withId("foo_polygon")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates( 8.6199615,50.020093)))
                        .withProperties(new Properties().with("foo_new", "test")),
                AuthProfile.ACCESS_OWNER_1_ADMIN
        );

        /** Add one feature on L2 composite space */
        postFeature(testSpaceId2ExtExt, newFeature()
                        .withId("2LPoint")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates( 8.4828826,50.201185)))
                        .withProperties(new Properties().with("foo", "test")),
                AuthProfile.ACCESS_OWNER_1_ADMIN
        );

        deleteAllJobsOnSpace(testSpaceId1);
        deleteAllJobsOnSpace(testSpaceId2);
        deleteAllJobsOnSpace(testSpaceId2Ext);
        deleteAllJobsOnSpace(testSpaceId2ExtExt);
    }

    @AfterClass
    public static void tearDownClass() {
        removeSpace(testSpaceId1);
        removeSpace(testSpaceId2);
        removeSpace(testSpaceId2Ext);
        removeSpace(testSpaceId2ExtExt);
    }

    protected static ValidatableResponse postJob(Job job){
        return postJob(job, testSpaceId1);
    }

    protected static ValidatableResponse postJob(Job job, String spaceId){
        return given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .body(job)
                .post("/spaces/" + spaceId + "/jobs")
                .then();
    }

    protected static ValidatableResponse patchJob(Job job, String path){
        return given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .body(job)
                .patch(path)
                .then();
    }

    protected static Job createTestJobWithId(String id, Job.Type type) {
        return createTestJobWithId(testSpaceId1, id, type, Job.CSVFormat.JSON_WKB);
    }

    protected static Job createTestJobWithId( String spaceId, String id, Job.Type type, Job.CSVFormat csvFormat) {
        Job job;
        if(type.equals(Job.Type.Import)) {
            job = new Import()
                    .withDescription("Job Description")
                    .withCsvFormat(csvFormat);

            if (id != null)
                job.setId(id);

            postJob(job, spaceId)
                    .body("createdAt", notNullValue())
                    .body("updatedAt", notNullValue())
                    .body("id", id == null ? notNullValue() : equalTo(id))
                    .body("description", equalTo("Job Description"))
                    .body("status", equalTo(Job.Status.waiting.toString()))
                    .body("csvFormat", equalTo(csvFormat.toString()))
                    .body("importObjects.size()", equalTo(0))
                    .body("type", equalTo(Import.class.getSimpleName()))
                    .statusCode(CREATED.code());
        }else{
            job = new Export()
                    .withDescription("Job Description")
                    .withCsvFormat(csvFormat)
                    .withExportTarget(new Export.ExportTarget().withType(Export.ExportTarget.Type.DOWNLOAD));

            if (id != null)
                job.setId(id);

            postJob(job, spaceId)
                    .body("createdAt", notNullValue())
                    .body("updatedAt", notNullValue())
                    .body("finalizedAt", equalTo(null))
                    .body("executedAt", equalTo(null))
                    .body("id", id == null ? notNullValue() : equalTo(id))
                    .body("description", equalTo("Job Description"))
                    .body("status", equalTo(Job.Status.waiting.toString()))
                    .body("csvFormat", equalTo(csvFormat.toString()))
                    .body("importObjects", equalTo(null))
                    .body("exportObjects", equalTo(null))
                    .body("type", equalTo(Export.class.getSimpleName()))
                    .statusCode(CREATED.code());
        }

        return job;
    }

    protected static void deleteJob(String jobId) {
        deleteJob(jobId, testSpaceId1);
    }
    protected static void deleteJob(String jobId, String spaceId) {
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .delete("/spaces/" + spaceId + "/job/"+jobId)
                .then()
                .statusCode(OK.code());
    }

    protected static void deleteAllJobsOnSpace(String spaceId){
        List<String> allJobsOnSpace = getAllJobsOnSpace(spaceId);
        for (String id : allJobsOnSpace) {
            deleteJob(id,spaceId);
        }
    }

    protected static List<String> getAllJobsOnSpace(String spaceId) {
        /** Get all jobs */
        Response response = given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .get("/spaces/" + spaceId + "/jobs");

        assertEquals(OK.code(),response.getStatusCode());
        String body = response.getBody().asString();
        try {
            List<Job> list = DatabindCodec.mapper().readValue(body, new TypeReference<List<Job>>() {});
            return list.stream().map(Job::getId).collect(Collectors.toList());
        }catch (Exception e){
            return new ArrayList<>();
        }
    }

    protected static Job getJob(String spaceId, String jobId) {
        /** Get all jobs */
        Response response = given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .get("/spaces/" + spaceId + "/job/"+jobId);

        assertEquals(OK.code(),response.getStatusCode());
        String body = response.getBody().asString();
        try {
            Job job = DatabindCodec.mapper().readValue(body, new TypeReference<Job>() {});
            return job;
        }catch (Exception e){
            return null;
        }
    }

    protected static Job pollStatus(String spaceId, String jobId,
                                  Job.Status expectedStatus, Job.Status failStatus)
            throws InterruptedException {

        Job.Status status = Job.Status.waiting;
        Job job = null;

        while(!status.equals(expectedStatus)){
            /**
             waiting -> validating -> validated -> queued -> executing -> executed
             -> (executing_trigger -> trigger_executed) -> finalizing -> finalized
                OR
             failed
            */
            job = getJob(spaceId, jobId);
            status = job.getStatus();
            assertNotEquals(status, failStatus);

            System.out.println("Current Status of Job["+jobId+"]: "+status);
            Thread.sleep(150);
        }

        return job;
    }

    protected static void uploadDummyFile(URL url, int type) throws IOException {
        url = new URL(url.toString().replace("localstack","localhost"));

        String input;
        switch (type){
            default:
            case 0 :
                //valid JSON_WKB
                input = "\"{'\"root'\": '\"test'\", '\"properties'\": {'\"foo'\": '\"bar'\",'\"foo_nested'\": {'\"nested_bar'\":true}}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A400000000000000000";
                break;
            case 1 :
                //invalid json - JSON_WKB
                input = "\"'\"root'\": '\"test'\", '\"properties'\": {'\"foo'\": '\"bar'\",'\"foo_nested'\": {'\"nested_bar'\":true}}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A400000000000000000";
                break;
            case 2 :
                //invalid geometry - JSON_WKB
                input = "\"{'\"root'\": '\"test'\", '\"properties'\": {'\"foo'\": '\"bar'\",'\"foo_nested'\": {'\"nested_bar'\":true}}}\",00800000028BF4047640D1B71758E407E373333333333401A9443D46B26C040476412AD81ADEB407E3170A3D70A3D";
                break;
            case 3 :
                //unknown column - JSON_WKB
                input = "\"{'\"foo'\":'\"bar'\"}\",008000000200000002401A94B9CB6848BF4047640D1B71758E407E373333333333401A9443D46B26C040476412AD81ADEB407E3170A3D70A3D,notValid";
                break;
            case 10 :
                //valid GEOJSON
                input = "\"{'\"type'\":'\"Feature'\",'\"id'\":'\"BfiimUxHjj'\",'\"geometry'\":{'\"type'\":'\"Point'\",'\"coordinates'\":[-2.960847,53.430828]},'\"properties'\":{'\"name'\":'\"Anfield'\",'\"amenity'\":'\"Football Stadium'\",'\"capacity'\":54074,'\"description'\":'\"Home of Liverpool Football Club'\"}}\"";
                break;
        }
        System.out.println("Start Upload");

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);

        connection.setRequestProperty("Content-Type","text/csv");
        connection.setRequestMethod("PUT");
        OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());

        out.write(input);
        out.close();

        connection.getResponseCode();
        System.out.println("Upload finished with: " + connection.getResponseCode());
    }
}
