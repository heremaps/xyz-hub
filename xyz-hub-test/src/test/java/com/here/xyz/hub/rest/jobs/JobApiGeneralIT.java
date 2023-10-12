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
package com.here.xyz.hub.rest.jobs;

import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.JSON_WKB;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.ImportObject;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.hub.auth.TestAuthenticator;
import com.here.xyz.hub.rest.TestSpaceWithFeature;
import com.here.xyz.hub.rest.TestWithSpaceCleanup;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.geojson.implementation.Point;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class JobApiGeneralIT extends JobApiIT {
    protected static String testJobId = "x-test-general-job";
    protected static String scope = "general";
    protected static String newSpace = "x-psql-job-space-new";

    @BeforeClass
    public static void init(){
        cleanUpEnv(scope);
        prepareEnv(scope);
        TestSpaceWithFeature.createSpaceWithCustomStorage(getScopedSpaceId(newSpace, scope), "psql", null);
    }

    @AfterClass
    public static void clean(){
        cleanUpEnv(scope);

        deleteAllJobsOnSpace(getScopedSpaceId(newSpace, scope));
        TestWithSpaceCleanup.removeSpace(getScopedSpaceId(newSpace, scope));
    }

    @Test
    public void getAllJobsOnSpace() {
        deleteAllJobsOnSpace(getScopedSpaceId(newSpace, scope));
        /** Create test job */
        createTestJobWithId(getScopedSpaceId(newSpace, scope), testJobId,  JobApiIT.Type.Import, JSON_WKB);

        /** Get all jobs */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(TestAuthenticator.getAuthHeaders(AuthProfile.ACCESS_ALL))
                .get("/spaces/" + getScopedSpaceId(newSpace, scope) + "/jobs")
                .then()
                .body("size()", equalTo(1))
                .statusCode(OK.code());
    }

    @Test
    public void createJobWithExistingId(){
        deleteAllJobsOnSpace(getScopedSpaceId(testSpaceId1, scope));
        /** Create job */
        Job job = createTestJobWithId(getScopedSpaceId(testSpaceId1, scope), testJobId,  JobApiIT.Type.Import, JSON_WKB);

        /** Create job with same Id */
        postJob(job,getScopedSpaceId(testSpaceId1, scope))
                .statusCode(BAD_REQUEST.code());
    }

    @Test
    public void createJobWithInvalidFilter() {
        /** Create job */
        Export job = new Export()
                .withId(testJobId + CService.currentTimeMillis())
                .withDescription("Job Description")
                .withExportTarget(new Export.ExportTarget().withType(Export.ExportTarget.Type.DOWNLOAD))
                .withCsvFormat(Job.CSVFormat.GEOJSON);

        job.setFilters(new Export.Filters().withSpatialFilter(new Export.SpatialFilter().withGeometry(new Point().withCoordinates(new PointCoordinates(399,399)))));

        postJob(job,getScopedSpaceId(testSpaceId1, scope))
                .statusCode(BAD_REQUEST.code());

        //Add check if no config exists
    }

    @Test
    public void getJob(){
        /** Create job */
        Job job = createTestJobWithId(getScopedSpaceId(testSpaceId1, scope), testJobId,  JobApiIT.Type.Import, JSON_WKB);

        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(TestAuthenticator.getAuthHeaders(AuthProfile.ACCESS_ALL))
                .get("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/"+job.getId())
                .then()
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .body("id", equalTo(job.getId()))
                .body("description", equalTo("Job Description"))
                .body("status", equalTo(Job.Status.waiting.toString()))
                .body("csvFormat", equalTo(JSON_WKB.toString()))
                .body("importObjects.size()", equalTo(0))
                .body("type", equalTo(Import.class.getSimpleName()))
                .statusCode(OK.code());
    }

    @Test
    public void updateMutableFields() {
        //Create job
        Job job = createTestJobWithId(getScopedSpaceId(testSpaceId1, scope), testJobId,  JobApiIT.Type.Import, JSON_WKB);

        //Modify job
        Job modified = new Import()
                .withId(testJobId + CService.currentTimeMillis())
                .withDescription("New Description")
                .withCsvFormat(JSON_WKB);

        patchJob(modified,"/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/" + job.getId())
                .body("description", equalTo("New Description"))
                .body("csvFormat", equalTo(JSON_WKB.toString()))
                .statusCode(OK.code());
    }

    @Test
    public void updateImmutableFields() throws MalformedURLException {
        /** Create job */
        Job job = createTestJobWithId(getScopedSpaceId(testSpaceId1, scope), testJobId,  JobApiIT.Type.Import, JSON_WKB);

        /** Modify job */
        Job modified = new Import()
                .withId(job.getId())
                .withStatus(Job.Status.prepared);

        patchJob(modified,"/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/" + job.getId())
                .statusCode(BAD_REQUEST.code());

        /** Modify job */
        modified = new Import()
                .withId(job.getId())
                .withErrorType("test");

        patchJob(modified,"/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/" + job.getId())
                .statusCode(BAD_REQUEST.code());

        /** Modify job */
        modified = new Import()
                .withId(job.getId())
                .withErrorDescription("test");

        patchJob(modified,"/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/" + job.getId())
                .statusCode(BAD_REQUEST.code());

        /** Modify job */
        Import modified2 = new Import();
        modified2.setId(job.getId());
        modified2.setImportObjects(new HashMap<String, ImportObject>(){{put("test",new ImportObject("test",new URL("http://test.com")));}});

        patchJob(modified2,"/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/" + job.getId())
                .statusCode(BAD_REQUEST.code());
    }

    @Test
    public void createUploadUrlJob() throws InterruptedException {
        /** Create job */
        Import job = (Import) createTestJobWithId(getScopedSpaceId(testSpaceId1, scope), testJobId,  JobApiIT.Type.Import, JSON_WKB);

        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(TestAuthenticator.getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + getScopedSpaceId(testSpaceId1, scope) + "/job/" + job.getId() + "/execute?command=createUploadUrl")
                .then()
                .statusCode(CREATED.code());


        job = (Import) loadJob(getScopedSpaceId(testSpaceId1, scope), job.getId());
        Map<String, ImportObject> importObjects = job.getImportObjects();

        assertEquals(importObjects.size(), 1);
        assertNotNull(importObjects.get("part_0.csv"));
        assertNotNull(importObjects.get("part_0.csv").getUploadUrl());
        assertNull(importObjects.get("part_0.csv").getFilename());
        assertNull(importObjects.get("part_0.csv").getS3Key());
        assertNull(importObjects.get("part_0.csv").getStatus());
        assertNull(importObjects.get("part_0.csv").getDetails());
    }

    @Test
    public void createValidExportJob(){
        /** Create job */
        createTestJobWithId(getScopedSpaceId(testSpaceId1, scope), testJobId,  JobApiIT.Type.Export, JSON_WKB);
    }

    @Test
    public void createInvalidVMLExportJob() throws InvalidGeometryException {
        /** Create job */
        Export job = new Export()
                .withId(testJobId + CService.currentTimeMillis())
                .withDescription("Job Description");

        postJob(job,getScopedSpaceId(testSpaceId1, scope))
                .statusCode(BAD_REQUEST.code());

        job.setExportTarget(new Export.ExportTarget().withType(Export.ExportTarget.Type.VML));

        postJob(job,getScopedSpaceId(testSpaceId1, scope))
                .statusCode(BAD_REQUEST.code());

        /** Add missing target-id - invalid one*/
        job.setExportTarget(new Export.ExportTarget().withType(Export.ExportTarget.Type.VML).withTargetId("testId"));

        postJob(job,getScopedSpaceId(testSpaceId1, scope))
                .statusCode(BAD_REQUEST.code());

        /** Add correct target-id */
        job.setExportTarget(new Export.ExportTarget().withType(Export.ExportTarget.Type.VML).withTargetId(getScopedSpaceId(testSpaceId1, scope)+":id"));

        postJob(job,getScopedSpaceId(testSpaceId1, scope))
                .statusCode(BAD_REQUEST.code());

        /** Add missing targetLevel - but invalid one */
        job.setTargetLevel(99);

        postJob(job,getScopedSpaceId(testSpaceId1, scope))
                .statusCode(BAD_REQUEST.code());

        /** Add missing targetLevel - correct one +  invalid Filter*/
        job.setTargetLevel(10);

        postJob(job,getScopedSpaceId(testSpaceId1, scope))
                .statusCode(CREATED.code());
    }
}
