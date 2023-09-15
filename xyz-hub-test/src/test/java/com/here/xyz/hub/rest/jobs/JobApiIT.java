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

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.jobs.Job.Status;
import com.here.xyz.hub.rest.ApiParam.Query.Incremental;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.rest.TestSpaceWithFeature;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.restassured.response.Response;
import io.restassured.response.ValidatableResponse;
import io.vertx.core.json.jackson.DatabindCodec;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class JobApiIT extends TestSpaceWithFeature {

    protected static String testSpaceId1 = "x-psql-job-space1";
    protected static String testSpaceId2 = "x-psql-job-space2";
    protected static String testSpaceId2Ext = "x-psql-job-space2-ext";
    protected static String testSpaceId2ExtExt = "x-psql-job-space2-ext-ext";

    protected static String getScopedSpaceId(String spaceId, String scope){
        return scope + "-" + spaceId;
    }

    protected static void prepareEnv(String scope){
        /** Create empty test space */
        createSpaceWithCustomStorage(getScopedSpaceId(testSpaceId1, scope), "psql", null);

        /** Create test space with content */
        createSpaceWithCustomStorage(getScopedSpaceId(testSpaceId2, scope), "psql", null);
        addFeatures(getScopedSpaceId(testSpaceId2, scope), "/xyz/hub/mixedGeometryTypes.json", 11);
        /** Create L1 composite space with content*/
        createSpaceWithExtension(getScopedSpaceId(testSpaceId2, scope));

        addFeatures(getScopedSpaceId(testSpaceId2Ext,scope),"/xyz/hub/processedData.json",252);
        /** Create L2 composite with content */
        createSpaceWithExtension(getScopedSpaceId(testSpaceId2Ext,scope));

        /** Modify one feature on L1 composite space */
        postFeature(getScopedSpaceId(testSpaceId2Ext,scope), newFeature()
                        .withId("foo_polygon")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates( 8.6199615,50.020093)))
                        .withProperties(new Properties().with("foo_new", "test")),
                AuthProfile.ACCESS_OWNER_1_ADMIN
        );

        /** Add one feature on L2 composite space */
        postFeature(getScopedSpaceId(testSpaceId2ExtExt,scope), newFeature()
                        .withId("2LPoint")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates( 8.4828826,50.201185)))
                        .withProperties(new Properties().with("foo", "test")),
                AuthProfile.ACCESS_OWNER_1_ADMIN
        );

        deleteAllJobsOnSpace(getScopedSpaceId(testSpaceId1, scope));
        deleteAllJobsOnSpace(getScopedSpaceId(testSpaceId2, scope));
        deleteAllJobsOnSpace(getScopedSpaceId(testSpaceId2Ext, scope));
        deleteAllJobsOnSpace(getScopedSpaceId(testSpaceId2ExtExt, scope));
    }

    protected static void cleanUpEnv(String scope){
        deleteAllJobsOnSpace(getScopedSpaceId(testSpaceId1, scope));
        deleteAllJobsOnSpace(getScopedSpaceId(testSpaceId2, scope));
        deleteAllJobsOnSpace(getScopedSpaceId(testSpaceId2Ext, scope));
        deleteAllJobsOnSpace(getScopedSpaceId(testSpaceId2ExtExt, scope));

        removeSpace(getScopedSpaceId(testSpaceId1, scope));
        removeSpace(getScopedSpaceId(testSpaceId2, scope));
        removeSpace(getScopedSpaceId(testSpaceId2Ext, scope));
        removeSpace(getScopedSpaceId(testSpaceId2ExtExt, scope));
    }

    public enum Type {
        Import, Export;
        public static Type of(String value) {
            if (value == null) {
                return null;
            }
            try {
                return valueOf(value);
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
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

    protected static Job createTestJobWithId(String spaceId, String id, Type type, Job.CSVFormat csvFormat) {
        id = id + CService.currentTimeMillis();
        Job job;
        if(type.equals(Type.Import)) {
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

    protected static void abortJob(Job job, String spaceId) {
        String postUrl = "/spaces/{spaceId}/job/{jobId}/execute?command=abort"
                .replace("{spaceId}", spaceId)
                .replace("{jobId}", job.getId());

        /** abort job */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post(postUrl)
                .then()
                .statusCode(CREATED.code());
    }

    protected static void deleteJob(String jobId, String spaceId, Boolean force) {
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .delete("/spaces/" + spaceId + "/job/"+jobId+(force == null ? "" :"?force="+force))
                .then()
                .statusCode(OK.code());
    }

    protected static void deleteAllJobsOnSpace(String spaceId){
        List<String> allJobsOnSpace = getAllJobsOnSpace(spaceId);
        for (String id : allJobsOnSpace) {
            deleteJob(id,spaceId, true);
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
        }
        catch (Exception e) {
            return Collections.emptyList();
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
            //Should be higher than JOB_CHECK_QUEUE_INTERVAL_MILLISECONDS
            Thread.sleep(120);
        }
        return job;
    }

    protected static Job abortExecution(String spaceId, String jobId)
            throws InterruptedException {

        Job.Status status = Job.Status.waiting;
        Job job = null;

        while(!status.equals(Job.Status.executing)){
            job = getJob(spaceId, jobId);
            status = job.getStatus();
            System.out.println("Current Status of Job["+jobId+"]: "+status);
            Thread.sleep(150);
        }

        abortJob(job, spaceId);

        while(!status.equals(Job.Status.failed)){
            job = getJob(spaceId, jobId);
            status = job.getStatus();
            System.out.println("Current Status of Job["+jobId+"]: "+status);
            Thread.sleep(150);
        }
        return job;
    }

    protected static void uploadDummyFile(URL url, int type) throws IOException {
        url = new URL(url.toString().replace("//localstack","//localhost"));

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
            case 11 :
                //valid JSON_WKB with id
                input = "\"{'\"id'\": '\"foo'\", '\"properties'\": {'\"foo'\": '\"bar'\",'\"foo_nested'\": {'\"nested_bar'\":true}}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A400000000000000000";
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

    protected static void uploadLocalFile(URL url, String filePath) throws IOException, URISyntaxException {
        File file = new File(filePath);
        BufferedInputStream inStream = new BufferedInputStream(new FileInputStream(file));

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);

        //does not work locally
        if(filePath.endsWith(".gz"))
            connection.setRequestProperty( "Content-Encoding","gzip");

        connection.setRequestProperty("Content-Type","text/csv");
        connection.setRequestMethod("PUT");
        OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());

        int b;
        while ((b = inStream.read()) != -1) {
            out.write(b);
        }
        out.flush();
        out.close();
        inStream.close();

        connection.getResponseCode();
        System.out.println("Upload finished with: " + connection.getResponseCode());
    }

    /** ------------------- HELPER EXPORT  -------------------- */
    protected List<URL> performExport(Export job, String spaceId, Status expectedStatus, Status failStatus) throws Exception {
        return performExport(job, spaceId, expectedStatus, failStatus, null, null);
    }

    protected List<URL> performExport(Export job, String spaceId, Status expectedStatus, Status failStatus, SpaceContext context,
        Incremental incremental) throws Exception {
        if (incremental != null)
            job.addParam("incremental", incremental.toString());
        if (context != null)
            job.addParam("context", context.toString());

        //Create job
        Response resp = given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .body(job)
                .post("/spaces/" + spaceId + "/jobs");

        if(resp.getStatusCode() != 201){
            XyzResponse xyzResponse = XyzSerializable.deserialize(resp.getBody().asString());
            if(xyzResponse instanceof ErrorResponse)
                throw new HttpException(HttpResponseStatus.valueOf(resp.getStatusCode()), ((ErrorResponse) xyzResponse).getErrorMessage());
        }


        String postUrl = "/spaces/{spaceId}/job/{jobId}/execute?command=start&{context}&{incremental}"
                .replace("{spaceId}", spaceId)
                .replace("{jobId}", job.getId())
                .replace("{context}", "")
                .replace("{incremental}", "");

        /** start import */
        resp = given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post(postUrl);

        if(resp.getStatusCode() != 204) { //NO_CONTENT
            XyzResponse xyzResponse = XyzSerializable.deserialize(resp.getBody().asString());
            if(xyzResponse instanceof ErrorResponse)
                throw new HttpException(HttpResponseStatus.valueOf(resp.getStatusCode()), ((ErrorResponse) xyzResponse).getErrorMessage());
        }

        //Poll status
        pollStatus(spaceId, job.getId(), expectedStatus, failStatus);
        job = (Export) getJob(spaceId, job.getId());

        List<URL> urlList = new ArrayList<>();
        if (job.getExportObjects() != null) {
            for (String key : job.getExportObjects().keySet()) {
                urlList.add(job.getExportObjects().get(key).getDownloadUrl());
            }
        }

        if(job.getSuperExportObjects() != null) {
            for (String key : job.getSuperExportObjects().keySet()) {
                urlList.add(job.getSuperExportObjects().get(key).getDownloadUrl());
            }
        }
        return urlList;
    }

    protected static String downloadAndCheck(List<URL> urls, Integer expectedByteSize, Integer expectedFeatureCount, List<String> csvMustContain) throws IOException, InterruptedException {
        String result = "";
        long totalByteSize = 0;

        for (URL url : urls) {
            System.out.println("Download: "+url);
            url = new URL(url.toString().replace("//localstack","//localhost"));
            BufferedInputStream bis = new BufferedInputStream(url.openStream());
            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            byte[] buffer = new byte[1024];
            int count=0;
            while((count = bis.read(buffer,0,1024)) != -1)
            {
                bos.write(buffer, 0, count);
            }
            bos.close();
            bis.close();
            totalByteSize += bos.size();
            result += new String(bos.toByteArray(), StandardCharsets.UTF_8);
        }

        if(expectedByteSize != null)
            assertEquals(expectedByteSize.intValue(), totalByteSize);

        if(expectedFeatureCount != null)
            assertEquals(expectedFeatureCount.intValue(), result.split("'\"id'\"", -1).length-1);

        for (String word : csvMustContain) {
            assertNotEquals(-1, result.indexOf(word));
        }

        return result;
    }

    protected static void downloadAndCheckFC(List<URL> urls, int expectedByteSize, int expectedFeatureCount, List<String> csvMustContain, Integer expectedTileCount) throws IOException, InterruptedException {
        List<String> tileIds = new ArrayList<>();
        int featureCount = 0;

        String result = downloadAndCheck(urls, expectedByteSize, 0, csvMustContain);
        for (String fc64: result.split("\n")) {
            tileIds.add(fc64.substring(0,fc64.lastIndexOf("\t")));
            FeatureCollection fc = XyzSerializable.deserialize(new String(Base64.getDecoder().decode((fc64.substring(fc64.lastIndexOf("\t")+1)))));
            featureCount += fc.getFeatures().size();
        }

        if(expectedTileCount != null)
            assertEquals(expectedTileCount.intValue(), tileIds.size());

        assertEquals(expectedFeatureCount, featureCount);
    }

    protected Export buildTestJob(String id, Export.Filters filters, Export.ExportTarget target, Job.CSVFormat format){
        return new Export()
                .withId(id + CService.currentTimeMillis())
                .withFilters(filters)
                .withExportTarget(target)
                .withCsvFormat(format);
    }

    protected Export buildVMTestJob(String id, Export.Filters filters, Export.ExportTarget target, Job.CSVFormat format, int targetLevel, int maxTilesPerFile){
        return buildTestJob(id, filters, target, format)
                .withMaxTilesPerFile(maxTilesPerFile)
                .withTargetLevel(targetLevel);
    }
}
