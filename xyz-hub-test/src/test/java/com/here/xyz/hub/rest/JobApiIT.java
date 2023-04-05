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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.ImportObject;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.jayway.restassured.response.Response;
import com.jayway.restassured.response.ValidatableResponse;
import io.vertx.core.json.jackson.DatabindCodec;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.*;

public class JobApiIT extends TestSpaceWithFeature {

    private static String testSpaceId1 = "x-psql-job-test1";
    private static String testSpaceId2 = "x-psql-job-test2";
    private static String testSpaceId2Ext = "x-psql-job-test2-ext";
    private static String testSpaceId2ExtExt = "x-psql-job-test2-ext-ext";
    private static String testJobId = "x-test-job";

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
    }

    @AfterClass
    public static void tearDownClass() {
        removeSpace(testSpaceId1);
        removeSpace(testSpaceId2);
        removeSpace(testSpaceId2Ext);
        removeSpace(testSpaceId2ExtExt);
        deleteAllJobsOnSpace(testSpaceId1);
    }

    private static ValidatableResponse postJob(Job job){
        return given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .body(job)
                .post("/spaces/" + testSpaceId1 + "/jobs")
                .then();
    }

    private static ValidatableResponse patchJob(Job job, String path){
        return given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .body(job)
                .patch(path)
                .then();
    }

    public static Job createTestJobWithId(String id, Job.Type type) {
        Job job;
        if(type.equals(Job.Type.Import)) {
            job = new Import()
                    .withDescription("Job Description")
                    .withCsvFormat(Job.CSVFormat.JSON_WKT);

            if (id != null)
                job.setId(id);

            postJob(job)
                    .body("createdAt", notNullValue())
                    .body("updatedAt", notNullValue())
                    .body("id", id == null ? notNullValue() : equalTo(id))
                    .body("description", equalTo("Job Description"))
                    .body("status", equalTo(Job.Status.waiting.toString()))
                    .body("csvFormat", equalTo(Job.CSVFormat.JSON_WKT.toString()))
                    .body("importObjects.size()", equalTo(0))
                    .body("type", equalTo(Import.class.getSimpleName()))
                    .statusCode(CREATED.code());
        }else{
            job = new Export()
                    .withDescription("Job Description")
                    .withCsvFormat(Job.CSVFormat.JSON_WKB)
                    .withExportTarget(new Export.ExportTarget().withType(Export.ExportTarget.Type.DOWNLOAD));

            if (id != null)
                job.setId(id);

            postJob(job)
                    .body("createdAt", notNullValue())
                    .body("updatedAt", notNullValue())
                    .body("finalizedAt", equalTo(null))
                    .body("executedAt", equalTo(null))
                    .body("id", id == null ? notNullValue() : equalTo(id))
                    .body("description", equalTo("Job Description"))
                    .body("status", equalTo(Job.Status.waiting.toString()))
                    .body("csvFormat", equalTo(Job.CSVFormat.JSON_WKB.toString()))
                    .body("importObjects", equalTo(null))
                    .body("exportObjects", equalTo(null))
                    .body("type", equalTo(Export.class.getSimpleName()))
                    .statusCode(CREATED.code());
        }

        return job;
    }

    public static void deleteJob(String jobId) {
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .delete("/spaces/" + testSpaceId1 + "/job/"+jobId)
                .then()
                .statusCode(OK.code());
    }

    public static void deleteAllJobsOnSpace(String spaceId){
        List<String> allJobsOnSpace = getAllJobsOnSpace(spaceId);
        for (String id : allJobsOnSpace) {
            deleteJob(id);
        }
    }

    public static List<String> getAllJobsOnSpace(String spaceId) {
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


    public static Job getJob(String spaceId, String jobId) {
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

    @Test
    public void getAllJobsOnSpace() throws JsonProcessingException {
        /** Create test job */
        createTestJobWithId(testJobId, Job.Type.Import);

        /** Get all jobs */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .get("/spaces/" + testSpaceId1 + "/jobs")
                .then()
                .body("size()", equalTo(1))
                .statusCode(OK.code());

        /** Delete Job */
        deleteJob(testJobId);
    }

    @Test
    public void createJobWithExistingId(){
        /** Create job */
        Job job = createTestJobWithId(testJobId, Job.Type.Import);

        /** Create job with same Id */
        postJob(job)
                .statusCode(BAD_REQUEST.code());

        /** Delete Job */
        deleteJob(testJobId);
    }

    @Test
    public void createJobWithInvalidFilter() throws InvalidGeometryException {
        /** Create job */
        Export job = new Export()
                .withId(testJobId)
                .withDescription("Job Description")
                .withExportTarget(new Export.ExportTarget().withType(Export.ExportTarget.Type.DOWNLOAD))
                .withCsvFormat(Job.CSVFormat.GEOJSON);

        job.setFilters(new Export.Filters().withSpatialFilter(new Export.SpatialFilter().withGeometry(new Point().withCoordinates(new PointCoordinates(399,399)))));

        postJob(job)
                .statusCode(BAD_REQUEST.code());
    }

    @Test
    public void getJob(){
        /** Create job */
        createTestJobWithId(testJobId, Job.Type.Import);

        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .get("/spaces/" + testSpaceId1 + "/job/"+testJobId)
                .then()
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .body("id", equalTo(testJobId))
                .body("description", equalTo("Job Description"))
                .body("status", equalTo(Job.Status.waiting.toString()))
                .body("csvFormat", equalTo(Job.CSVFormat.JSON_WKT.toString()))
                .body("importObjects.size()", equalTo(0))
                .body("type", equalTo(Import.class.getSimpleName()))
                .statusCode(OK.code());

        /** Delete Job */
        deleteJob(testJobId);
    }

    @Test
    public void updateMutableFields() {
        /** Create job */
        createTestJobWithId(testJobId, Job.Type.Import);

        /** Modify job */
        Job modified = new Import()
                .withId(testJobId)
                .withDescription("New Description")
                .withCsvFormat(Job.CSVFormat.JSON_WKB);

        patchJob(modified,"/spaces/" + testSpaceId1 + "/job/"+testJobId)
                .body("description", equalTo("New Description"))
                .body("csvFormat", equalTo(Job.CSVFormat.JSON_WKB.toString()))
                .statusCode(OK.code());

        /** Delete Job */
        deleteJob(testJobId);
    }

    @Test
    public void createJobOnLayerWithContent(){

    }

    @Test
    public void updateImmutableFields() throws MalformedURLException {
        /** Create job */
        createTestJobWithId(testJobId, Job.Type.Import);

        /** Modify job */
        Job modified = new Import()
                .withId(testJobId)
                .withStatus(Job.Status.prepared);

        patchJob(modified,"/spaces/" + testSpaceId1 + "/job/"+testJobId)
                .statusCode(BAD_REQUEST.code());

        /** Modify job */
        modified = new Import()
                .withId(testJobId)
                .withErrorType("test");

        patchJob(modified,"/spaces/" + testSpaceId1 + "/job/"+testJobId)
                .statusCode(BAD_REQUEST.code());

        /** Modify job */
        modified = new Import()
                .withId(testJobId)
                .withErrorDescription("test");

        patchJob(modified,"/spaces/" + testSpaceId1 + "/job/"+testJobId)
                .statusCode(BAD_REQUEST.code());

        /** Modify job */
        Import modified2 = new Import();
        modified2.setId(testJobId);
        modified2.setImportObjects(new HashMap<String, ImportObject>(){{put("test",new ImportObject("test",new URL("http://test.com")));}});

        patchJob(modified2,"/spaces/" + testSpaceId1 + "/job/"+testJobId)
                .statusCode(BAD_REQUEST.code());
        /** Delete Job */
        deleteJob(testJobId);
    }

    @Test
    public void createUploadUrlJob() throws InterruptedException {
        /** Create job */
        createTestJobWithId(testJobId, Job.Type.Import);

        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + testSpaceId1 + "/job/"+testJobId+"/execute?command=createUploadUrl")
                .then()
                .statusCode(CREATED.code());


        Import job = (Import) getJob(testSpaceId1,testJobId);
        Map<String, ImportObject> importObjects = job.getImportObjects();

        assertEquals(importObjects.size(), 1);
        assertNotNull(importObjects.get("part_0.csv"));
        assertNotNull(importObjects.get("part_0.csv").getUploadUrl());
        assertNull(importObjects.get("part_0.csv").getFilename());
        assertNull(importObjects.get("part_0.csv").getS3Key());
        assertNull(importObjects.get("part_0.csv").getStatus());
        assertNull(importObjects.get("part_0.csv").getDetails());

        /** Delete Job */
        deleteJob(testJobId);
    }

    @Test
    public void startIncompleteImportJob() throws InterruptedException {
        /** Create job */
        createTestJobWithId(testJobId, Job.Type.Import);

        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + testSpaceId1 + "/job/"+testJobId+"/execute?command=start")
                .then()
                .statusCode(NO_CONTENT.code());

        Job.Status status = Job.Status.waiting;
        Job job = null;
        while(!status.equals(Job.Status.failed)){
            job = getJob(testSpaceId1,testJobId);
            status = job.getStatus();
            System.out.println("Current Status of Job "+status);
            Thread.sleep(2000);
        }
        assertEquals(job.getErrorDescription(), Import.ERROR_DESCRIPTION_UPLOAD_MISSING );
        assertEquals(job.getErrorType(), Import.ERROR_TYPE_VALIDATION_FAILED);

        /** Delete Job */
        deleteJob(testJobId);
    }

    @Test
    public void createValidExportJob(){
        /** Create job */
        createTestJobWithId(testJobId, Job.Type.Export);

        /** Delete Job */
        deleteJob(testJobId);
    }

    @Test
    public void createInvalidS3ExportJob(){
        /** Create job */
        Export job = new Export()
                .withId(testJobId)
                .withDescription("Job Description");

        postJob(job)
                .statusCode(BAD_REQUEST.code());

        /** Add missing target */
        job.setExportTarget(new Export.ExportTarget().withType(Export.ExportTarget.Type.DOWNLOAD));

        postJob(job)
                .statusCode(BAD_REQUEST.code());

        /** Add missing format - but wrong one */
        job.setCsvFormat(Job.CSVFormat.TILEID_FC_B64);

        postJob(job)
                .statusCode(BAD_REQUEST.code());

        /** Add missing format - correct one */
        job.setCsvFormat(Job.CSVFormat.GEOJSON);

        postJob(job)
                .statusCode(CREATED.code());

        /** Delete Job */
        deleteJob(testJobId);
    }

    @Test
    public void createInvalidVMLExportJob() throws InvalidGeometryException {
        /** Create job */
        Export job = new Export()
                .withId(testJobId)
                .withDescription("Job Description");

        postJob(job)
                .statusCode(BAD_REQUEST.code());

        job.setExportTarget(new Export.ExportTarget().withType(Export.ExportTarget.Type.VML));

        postJob(job)
                .statusCode(BAD_REQUEST.code());

        /** Add missing target-id */
        job.setExportTarget(new Export.ExportTarget().withType(Export.ExportTarget.Type.VML).withTargetId("testId"));

        postJob(job)
                .statusCode(BAD_REQUEST.code());

        /** Add missing targetLevel - but invalid one */
        job.setTargetLevel(99);

        postJob(job)
                .statusCode(BAD_REQUEST.code());

        /** Add missing targetLevel - correct one +  invalid Filter*/
        job.setTargetLevel(10);

        postJob(job)
                .statusCode(CREATED.code());

        /** Delete Job */
        deleteJob(testJobId);
    }
}
