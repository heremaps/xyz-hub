package com.here.xyz.hub.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.ImportObject;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.jayway.restassured.response.Response;
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

    private static String testSpaceId = "x-psql-job-test";
    private static String testJobId = "x-test-job";

    @BeforeClass
    public static void setup() {
        /** Create test space */
        createSpaceWithCustomStorage(testSpaceId, "psql", null);
        deleteAllJobsOnSpace(testSpaceId);
    }

    @AfterClass
    public static void tearDownClass() {
        removeSpace(testSpaceId);
        deleteAllJobsOnSpace(testSpaceId);
    }

    public static Job createTestJobWithId(String id) {
        Job importJob = new Import()
                .withDescription("Job Description")
                .withCsvFormat(Job.CSVFormat.JSON_WKT);

        if(id != null)
            importJob.setId(id);

        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .body(importJob)
                .post("/spaces/" + testSpaceId + "/jobs")
                .then()
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .body("id",id == null ? notNullValue() : equalTo(id))
                .body("description", equalTo("Job Description"))
                .body("status", equalTo(Job.Status.waiting.toString()))
                .body("csvFormat", equalTo(Job.CSVFormat.JSON_WKT.toString()))
                .body("importObjects.size()", equalTo(0))
                .body("type", equalTo(Import.class.getSimpleName()))
                .statusCode(CREATED.code());
        return importJob;
    }

    public static void deleteJob(String jobId) {
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .delete("/spaces/" + testSpaceId + "/job/"+jobId)
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
        createTestJobWithId(testJobId);

        /** Get all jobs */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .get("/spaces/" + testSpaceId + "/jobs")
                .then()
                .body("size()", equalTo(1))
                .statusCode(OK.code());

        /** Delete Job */
        deleteJob(testJobId);
    }

    @Test
    public void createTestJob(){
        /** Create job */
        Job importJob = createTestJobWithId(testJobId);

        /** Create job with same Id */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .body(importJob)
                .post("/spaces/" + testSpaceId + "/jobs")
                .then()
                .statusCode(BAD_REQUEST.code());

        /** Delete Job */
        deleteJob(testJobId);
    }

    @Test
    public void getJob(){
        /** Create job */
        createTestJobWithId(testJobId);

        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .get("/spaces/" + testSpaceId + "/job/"+testJobId)
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
        createTestJobWithId(testJobId);

        /** Modify job */
        Job modified = new Import()
                .withId(testJobId)
                .withDescription("New Description")
                .withCsvFormat(Job.CSVFormat.JSON_WKB);

        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .body(modified)
                .patch("/spaces/" + testSpaceId + "/job/"+testJobId)
                .then()
                .body("description", equalTo("New Description"))
                .body("csvFormat", equalTo(Job.CSVFormat.JSON_WKB.toString()))
                .statusCode(OK.code());

        /** Delete Job */
        deleteJob(testJobId);
    }

    @Test
    public void updateImmutableFields() throws MalformedURLException {
        /** Create job */
        createTestJobWithId(testJobId);

        /** Modify job */
        Job modified = new Import()
                .withId(testJobId)
                .withStatus(Job.Status.prepared);

        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .body(modified)
                .patch("/spaces/" + testSpaceId + "/job/"+testJobId)
                .then()
                .statusCode(BAD_REQUEST.code());

        /** Modify job */
        modified = new Import()
                .withId(testJobId)
                .withErrorType("test");

        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .body(modified)
                .patch("/spaces/" + testSpaceId + "/job/"+testJobId)
                .then()
                .statusCode(BAD_REQUEST.code());

        /** Modify job */
        modified = new Import()
                .withId(testJobId)
                .withErrorDescription("test");

        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .body(modified)
                .patch("/spaces/" + testSpaceId + "/job/"+testJobId)
                .then()
                .statusCode(BAD_REQUEST.code());

        /** Modify job */
        Import modified2 = new Import();
        modified2.setId(testJobId);
        modified2.setImportObjects(new HashMap<String, ImportObject>(){{put("test",new ImportObject("test",new URL("http://test.com")));}});

        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .body(modified2)
                .patch("/spaces/" + testSpaceId + "/job/"+testJobId)
                .then()
                .statusCode(BAD_REQUEST.code());
    }

    @Test
    public void createUploadUrlJob() throws InterruptedException {
        /** Create job */
        createTestJobWithId(testJobId);

        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + testSpaceId + "/job/"+testJobId+"/execute?command=createUploadUrl")
                .then()
                .statusCode(NO_CONTENT.code());


        Import job = (Import) getJob(testSpaceId,testJobId);
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
    public void startIncompleteJob() throws InterruptedException {
        /** Create job */
        createTestJobWithId(testJobId);

        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + testSpaceId + "/job/"+testJobId+"/execute?command=start")
                .then()
                .statusCode(NO_CONTENT.code());

        Job.Status status = Job.Status.waiting;
        Job job = null;
        while(!status.equals(Job.Status.failed)){
            job = getJob(testSpaceId,testJobId);
            status = job.getStatus();
            System.out.println("Current Status of Job "+status);
            Thread.sleep(2000);
        }
        assertEquals(job.getErrorDescription(), Import.ERROR_DESCRIPTION_UPLOAD_MISSING );
        assertEquals(job.getErrorType(), Import.ERROR_TYPE_VALIDATION_FAILED);

        /** Delete Job */
        deleteJob(testJobId);
    }
}
