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

import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.ImportObject;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.geojson.implementation.Point;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.*;

public class JobApiGeneralIT extends JobApiIT {

    @Test
    public void getAllJobsOnSpace() {
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
    public void createJobWithInvalidFilter() {
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
                .body("csvFormat", equalTo(Job.CSVFormat.JSON_WKB.toString()))
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

        /** Add missing target-id - invalid one*/
        job.setExportTarget(new Export.ExportTarget().withType(Export.ExportTarget.Type.VML).withTargetId("testId"));

        postJob(job)
                .statusCode(BAD_REQUEST.code());

        /** Add correct target-id */
        job.setExportTarget(new Export.ExportTarget().withType(Export.ExportTarget.Type.VML).withTargetId(testSpaceId1+":id"));

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
