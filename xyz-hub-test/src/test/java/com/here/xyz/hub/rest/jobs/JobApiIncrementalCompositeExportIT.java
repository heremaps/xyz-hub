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

import static com.here.xyz.httpconnector.util.jobs.Export.ExportTarget.Type.DOWNLOAD;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.GEOJSON;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.failed;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.finalized;
import static com.here.xyz.hub.rest.ApiParam.Query.Incremental.CHANGES;
import static com.here.xyz.hub.rest.ApiParam.Query.Incremental.DEACTIVATED;
import static com.here.xyz.hub.rest.ApiParam.Query.Incremental.FULL;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.PRECONDITION_FAILED;
import static org.junit.Assert.assertEquals;

import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.rest.TestSpaceWithFeature;
import com.here.xyz.hub.rest.TestWithSpaceCleanup;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class JobApiIncrementalCompositeExportIT extends JobApiIT{
    protected String testExportJobId = "x-test-composite-export-job";
    protected static String scope = "incremental";

    String testSpaceId1 = "composite-export-space";
    String testSpaceId1Ext = "composite-export-space-ext";
    String testSpaceId1ExtExt = "composite-export-space-ext-ext";

    @BeforeClass
    public static void setup() { }

    @Before
    public void prepare() {
        TestSpaceWithFeature.createSpaceWithCustomStorage(testSpaceId1, "psql", null, true);
        TestSpaceWithFeature.addFeatures(testSpaceId1, "/xyz/hub/fcWithPoints.json", 7);

        TestSpaceWithFeature.createSpaceWithExtension(testSpaceId1);

        /** Add one feature on L1 composite space */
        TestSpaceWithFeature.postFeature(testSpaceId1Ext, TestSpaceWithFeature.newFeature()
                        .withId("idX")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates(-1.56676045, 39.63936801)))
                        .withProperties(new Properties().with("foo", "test")),
                AuthProfile.ACCESS_OWNER_1_ADMIN
        );

        /** Delete Feature from Base */
        deleteFeature(testSpaceId1Ext,"id2");

        /** Edit Base Feature */
        TestSpaceWithFeature.postFeature(testSpaceId1Ext, TestSpaceWithFeature.newFeature()
                        .withId("id3")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates(  41.65012568226706, 38.96805602991469)))
                        .withProperties(new Properties().with("foo", "test2")),
                AuthProfile.ACCESS_OWNER_1_ADMIN
        );

        /** Edit Base Feature and Move*/
        TestSpaceWithFeature.postFeature(testSpaceId1Ext, TestSpaceWithFeature.newFeature()
                        .withId("id7")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates(  -80.6915562469239, 45.6444425342321)))
                        .withProperties(new Properties().with("foo", "test2")),
                AuthProfile.ACCESS_OWNER_1_ADMIN
        );

        /** Add Delta2 Feature */
        TestSpaceWithFeature.createSpaceWithExtension(testSpaceId1Ext);
        TestSpaceWithFeature.postFeature(testSpaceId1ExtExt, TestSpaceWithFeature.newFeature()
                        .withId("id8")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates(  -1,1)))
                        .withProperties(new Properties().with("foo", "test3")),
                AuthProfile.ACCESS_OWNER_1_ADMIN
        );

        /** Remove Delta1 Feature */
        deleteFeature(testSpaceId1ExtExt,"id4");

        deleteAllJobsOnSpace(testSpaceId1);
        deleteAllJobsOnSpace(testSpaceId1Ext);
        deleteAllJobsOnSpace(testSpaceId1ExtExt);
    }

    @After
    public void after(){
        TestWithSpaceCleanup.removeSpace(testSpaceId1);
        TestWithSpaceCleanup.removeSpace(testSpaceId1Ext);
        TestWithSpaceCleanup.removeSpace(testSpaceId1ExtExt);
    }

    @Test
    public void invalidConfig() throws Exception{
        int exceptionCnt = 0;

        Export job = buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(DOWNLOAD), GEOJSON);
        try {
            /** Invalid Type - creation fails */
            performExport(job, testSpaceId1Ext, failed, finalized, FULL);
        }
        catch (HttpException e){
            assertEquals(BAD_REQUEST, e.status);
            exceptionCnt++;
        }
        deleteAllJobsOnSpace(testSpaceId1Ext);;

        job = buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(DOWNLOAD), GEOJSON);
        try {
            /** No extended layer - creation fails */
            performExport(job, testSpaceId1, failed, finalized, CHANGES);
        }catch (HttpException e){
            assertEquals(BAD_REQUEST, e.status);
            exceptionCnt++;
        }

        /** Check if we got the expected amount of failures */
        assertEquals(2, exceptionCnt);
    }

    @Test
    public void missingPersistentBaseExport() throws Exception{
        Export job =  generateExportJob(testExportJobId, 4);
        try {
            performExport(job, testSpaceId1Ext, failed, finalized, FULL);
        }
        catch (HttpException e){
            assertEquals(PRECONDITION_FAILED, e.status);
        }

        deleteJob(job.getId(), testSpaceId1Ext, true);
        job =  generateExportJob(testExportJobId, 4);
        try {
            performExport(job, testSpaceId1ExtExt, failed, finalized, FULL);
        }
        catch (HttpException e){
            assertEquals(PRECONDITION_FAILED, e.status);
        }
    }

    @Test
    public void wrongTargetLevel() throws Exception {
        Export job =  generateExportJob(testExportJobId, 4);

        /** Initial Base Export on testSpaceId1 */
        performExport(job, testSpaceId1, failed, finalized, DEACTIVATED);
        deleteAllJobsOnSpace(testSpaceId1);

        job =  generateExportJob(testExportJobId, 8);
        /** Incremental Export on testSpaceId1Ext - Base layer got exported on level 4 now we want an incremental export on another level */
        try {
            performExport(job, testSpaceId1Ext, failed, finalized, FULL);
        }
        catch (HttpException e){
            assertEquals(PRECONDITION_FAILED, e.status);
        }
    }

    @Test
    public void validIncrementalL1ExportFull() throws Exception {
        Export job =  generateExportJob(testExportJobId, 6);

        /** Initial Base Export */
        List<URL> urls = performExport(job, testSpaceId1, failed, finalized, DEACTIVATED);
        assertEquals(1, urls.size());

        /** Incremental Export */
        job =  generateExportJob(testExportJobId+"_2", 6);
        urls = performExport(job, testSpaceId1Ext, failed, finalized, FULL);
        /** Expect 3 files from persistent super layer + 3 files from composite compound */
        assertEquals(2, urls.size());

        List<String> mustContain = Arrays.asList(
            "NjQ3LCAyNS45MTM",
            "4807",
            "4953",
            "5681",
            "5693",
            "5749",
            "5760",
            "5831"
        );

        //7 Features from base + 7 Features from base+delta
        downloadAndCheckFC(urls, 5936, 14, mustContain, 14);
    }

    @Test
    public void validIncrementalL1ExportChanges() throws Exception {
        Export job =  generateExportJob(testExportJobId, 6);

        /** Initial Base Export */
        List<URL> urls = performExport(job, testSpaceId1, failed, finalized, DEACTIVATED);
        assertEquals(1, urls.size());

        List<String> mustContain = Arrays.asList(
            "NjQ3LCAyNS45MTM",
            "4807",
            "4953",
            "5681",
            "5693",
            "5749",
            "5760",
            "5831"
        );

        downloadAndCheckFC(urls, 2930, 7, mustContain, 7);

        /** Incremental Export */
        job =  generateExportJob(testExportJobId+"_2", 6);
        urls = performExport(job, testSpaceId1Ext, failed, finalized, CHANGES);
        /** Expect 1 file with base+delta */
        assertEquals(1, urls.size());

        mustContain = Arrays.asList(
            "eyJ0eXBlIjogIkZlYXR1cmVDb2xsZWN0aW9uIiwgImZlYXR1cmVzIjpbXX0", //EmptyFC
            "4807",
            "4991",
            "4993",
            "5693",
            "5760"
        );

        //Includes two empty tiles
        downloadAndCheckFC(urls, 1466, 3, mustContain, 5);
    }

    @Test
    public void validIncrementalL2ExportFull() throws Exception {
        Export job =  generateExportJob(testExportJobId, 6);

        /** Initial Base Export */
        List<URL> urls = performExport(job, testSpaceId1, failed, finalized, DEACTIVATED);
        assertEquals(1, urls.size());

        /** Incremental Export */
        job =  generateExportJob(testExportJobId+"_2", 6);
        urls = performExport(job, testSpaceId1ExtExt, failed, finalized, FULL);
        /** Expect 1 files from persistent super layer + 1 files from composite compound */
        assertEquals(2, urls.size());

        List<String> mustContain = Arrays.asList(
            "sICJwcm9wZXJ0aW",
            "4807",
            "4953",
            "5681",
            "5693",
            "5749",
            "5760",
            "5831"
        );

        //7 Features from base + 7 Features from base+delta
        downloadAndCheckFC(urls, 5944, 14, mustContain, 14);
    }

    @Test
    public void validIncrementalL2ExportChanges() throws Exception {
        Export job =  generateExportJob(testExportJobId, 6);

        /** Initial Base Export */
        List<URL> urls = performExport(job, testSpaceId1, failed, finalized, DEACTIVATED);
        assertEquals(1, urls.size());

        /** Incremental Export */
        job =  generateExportJob(testExportJobId+"_2", 6);

        urls = performExport(job, testSpaceId1ExtExt, failed, finalized, CHANGES);

        assertEquals(1, urls.size());

        List<String> mustContain = Arrays.asList(
            "eyJ0eXBlIjogIkZlYXR1cmVDb2xsZWN0aW9uIiwgImZlYXR1cmVzIjpbXX0=",  //EmptyFC
            "pbeyJpZCI6ICJpZDgiLCAidHlwZ",
            "4949",
            "5831"
        );

        downloadAndCheckFC(urls, 492, 1, mustContain, 2);

    }

    public Export generateExportJob(String id, int targetLevel){
        int maxTilesPerFile= 15;

        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId("dummy");

        return buildVMTestJob(id, null, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
    }
}
