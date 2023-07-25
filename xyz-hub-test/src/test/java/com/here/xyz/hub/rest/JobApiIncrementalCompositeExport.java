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

import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import org.junit.*;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static org.junit.Assert.assertEquals;

public class JobApiIncrementalCompositeExport extends JobApiIT{
    protected String testExportJobId = "x-test-composite-export-job";

    String testSpaceId1 = "composite-export-space";
    String testSpaceId1Ext = "composite-export-space-ext";
    String testSpaceId1ExtExt = "composite-export-space-ext-ext";

    @BeforeClass
    public static void setup() { }

    @Before
    public void prepare() {
        createSpaceWithCustomStorage(testSpaceId1, "psql", null, true);
        addFeatures(testSpaceId1, "/xyz/hub/fcWithPoints.json", 7);

        createSpaceWithExtension(testSpaceId1);

        /** Add one feature on L1 composite space */
        postFeature(testSpaceId1Ext, newFeature()
                        .withId("idX")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates(-1.56676045, 39.63936801)))
                        .withProperties(new Properties().with("foo", "test")),
                AuthProfile.ACCESS_OWNER_1_ADMIN
        );

        /** Delete Feature from Base */
        deleteFeature(testSpaceId1Ext,"id2");

        /** Edit Base Feature */
        postFeature(testSpaceId1Ext, newFeature()
                        .withId("id3")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates(  41.65012568226706, 38.96805602991469)))
                        .withProperties(new Properties().with("foo", "test2")),
                AuthProfile.ACCESS_OWNER_1_ADMIN
        );

        /** Edit Base Feature and Move*/
        postFeature(testSpaceId1Ext, newFeature()
                        .withId("id7")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates(  -80.6915562469239, 45.6444425342321)))
                        .withProperties(new Properties().with("foo", "test2")),
                AuthProfile.ACCESS_OWNER_1_ADMIN
        );

        /** Add Delta2 Feature */
        createSpaceWithExtension(testSpaceId1Ext);
        postFeature(testSpaceId1ExtExt, newFeature()
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
        removeSpace(testSpaceId1);
        removeSpace(testSpaceId1Ext);
        removeSpace(testSpaceId1ExtExt);
    }

    @Test
    public void invalidConfig() throws Exception{
        int exceptionCnt = 0;

        Export job = buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(Export.ExportTarget.Type.DOWNLOAD), Job.CSVFormat.GEOJSON);
        try {
            /** Invalid Type - creation fails */
            performExport(job, testSpaceId1Ext, Job.Status.failed, Job.Status.finalized, ContextAwareEvent.SpaceContext.DEFAULT, ApiParam.Query.Incremental.FULL);
        }catch (HttpException e){
            assertEquals(BAD_REQUEST, e.status);
            exceptionCnt++;
        }
        deleteAllJobsOnSpace(testSpaceId1Ext);;

        job = buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(Export.ExportTarget.Type.DOWNLOAD), Job.CSVFormat.GEOJSON);
        try {
            /** No extended layer - creation fails */
            performExport(job, testSpaceId1, Job.Status.failed, Job.Status.finalized, ContextAwareEvent.SpaceContext.DEFAULT, ApiParam.Query.Incremental.CHANGES);
        }catch (HttpException e){
            assertEquals(BAD_REQUEST, e.status);
            exceptionCnt++;
        }

        job = buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(Export.ExportTarget.Type.VML).withTargetId("dummy"), Job.CSVFormat.PARTITIONID_FC_B64);
        try {
            /** unsupported Format - creation fails */
            performExport(job, testSpaceId1Ext, Job.Status.failed, Job.Status.finalized, ContextAwareEvent.SpaceContext.DEFAULT, ApiParam.Query.Incremental.CHANGES);
        }catch (HttpException e){
            assertEquals(BAD_REQUEST, e.status);
            exceptionCnt++;
        }
        /** Check if we got the expected amount of failures */
        assertEquals(3, exceptionCnt);
    }

    @Test
    public void missingPersistentBaseExport() throws Exception{
        Export job =  generateExportJob(testExportJobId, 4);
        try {
            performExport(job, testSpaceId1Ext, Job.Status.failed, Job.Status.finalized, ContextAwareEvent.SpaceContext.DEFAULT, ApiParam.Query.Incremental.FULL);
        }catch (HttpException e){
            assertEquals(PRECONDITION_FAILED, e.status);
        }

        deleteJob(testExportJobId, testSpaceId1Ext, true);
        job =  generateExportJob(testExportJobId, 4);
        try {
            performExport(job, testSpaceId1ExtExt, Job.Status.failed, Job.Status.finalized, ContextAwareEvent.SpaceContext.DEFAULT, ApiParam.Query.Incremental.FULL);
        }catch (HttpException e){
            assertEquals(PRECONDITION_FAILED, e.status);
        }
    }

    @Test
    public void wrongTargetLevel() throws Exception {
        Export job =  generateExportJob(testExportJobId, 4);

        /** Initial Base Export on testSpaceId1 */
        performExport(job, testSpaceId1, Job.Status.failed, Job.Status.finalized, ContextAwareEvent.SpaceContext.DEFAULT, ApiParam.Query.Incremental.DEACTIVATED);
        deleteAllJobsOnSpace(testSpaceId1);

        job =  generateExportJob(testExportJobId, 8);
        /** Incremental Export on testSpaceId1Ext - Base layer got exported on level 4 now we want an incremental export on another level */
        try {
            performExport(job, testSpaceId1Ext, Job.Status.failed, Job.Status.finalized, ContextAwareEvent.SpaceContext.DEFAULT, ApiParam.Query.Incremental.FULL);
        }catch (HttpException e){
            assertEquals(PRECONDITION_FAILED, e.status);
        }
    }

    @Test
    public void validIncrementalL1ExportFull() throws Exception {
        Export job =  generateExportJob(testExportJobId, 6);

        /** Initial Base Export */
        List<URL> urls = performExport(job, testSpaceId1, Job.Status.failed, Job.Status.finalized, ContextAwareEvent.SpaceContext.DEFAULT, ApiParam.Query.Incremental.DEACTIVATED);
        assertEquals(1, urls.size());

        /** Incremental Export */
        job =  generateExportJob(testExportJobId+"_2", 6);
        urls = performExport(job, testSpaceId1Ext, Job.Status.failed, Job.Status.finalized, ContextAwareEvent.SpaceContext.DEFAULT, ApiParam.Query.Incremental.FULL);
        /** Expect 3 files from persistent super layer + 3 files from composite compound */
        assertEquals(2, urls.size());

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("NjQ3LCAyNS45MTM");
            add("4807");
            add("4953");
            add("5681");
            add("5693");
            add("5749");
            add("5760");
            add("5831");
        }};

        //7 Features from base + 7 Features from base+delta
        downloadAndCheckFC(urls, 5660, 14, mustContains, 14 );
    }

    @Test
    public void validIncrementalL1ExportChanges() throws Exception {
        Export job =  generateExportJob(testExportJobId, 6);

        /** Initial Base Export */
        List<URL> urls = performExport(job, testSpaceId1, Job.Status.failed, Job.Status.finalized, ContextAwareEvent.SpaceContext.DEFAULT, ApiParam.Query.Incremental.DEACTIVATED);
        assertEquals(1, urls.size());

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("NjQ3LCAyNS45MTM");
            add("4807");
            add("4953");
            add("5681");
            add("5693");
            add("5749");
            add("5760");
            add("5831");
        }};

        downloadAndCheckFC(urls, 2790, 7, mustContains, 7 );

        /** Incremental Export */
        job =  generateExportJob(testExportJobId+"_2", 6);
        urls = performExport(job, testSpaceId1Ext, Job.Status.failed, Job.Status.finalized, ContextAwareEvent.SpaceContext.DEFAULT, ApiParam.Query.Incremental.CHANGES);
        /** Expect 1 file with base+delta */
        assertEquals(1, urls.size());

        mustContains = new ArrayList<String>(){{
            add("eyJ0eXBlIjogIkZlYXR1cmVDb2xsZWN0aW9uIiwgImZlYXR1cmVzIjpbXX0"); //EmptyFC
            add("4807");
            add("4991");
            add("4993");
            add("5693");
            add("5760");
        }};

        //Includes two empty tiles
        downloadAndCheckFC(urls, 1410, 3, mustContains, 5 );
    }

    @Test
    public void validIncrementalL2ExportFull() throws Exception {
        Export job =  generateExportJob(testExportJobId, 6);

        /** Initial Base Export */
        List<URL> urls = performExport(job, testSpaceId1, Job.Status.failed, Job.Status.finalized, ContextAwareEvent.SpaceContext.DEFAULT, ApiParam.Query.Incremental.DEACTIVATED);
        assertEquals(1, urls.size());

        /** Incremental Export */
        job =  generateExportJob(testExportJobId+"_2", 6);
        urls = performExport(job, testSpaceId1ExtExt, Job.Status.failed, Job.Status.finalized, ContextAwareEvent.SpaceContext.DEFAULT, ApiParam.Query.Incremental.FULL);
        /** Expect 1 files from persistent super layer + 1 files from composite compound */
        assertEquals(2, urls.size());

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("sICJwcm9wZXJ0aW");
            add("4807");
            add("4953");
            add("5681");
            add("5693");
            add("5749");
            add("5760");
            add("5831");
        }};

        //7 Features from base + 7 Features from base+delta
        downloadAndCheckFC(urls, 5668, 14, mustContains, 14 );
    }

    @Test
    public void validIncrementalL2ExportChanges() throws Exception {
        Export job =  generateExportJob(testExportJobId, 6);

        /** Initial Base Export */
        List<URL> urls = performExport(job, testSpaceId1, Job.Status.failed, Job.Status.finalized, ContextAwareEvent.SpaceContext.DEFAULT, ApiParam.Query.Incremental.DEACTIVATED);
        assertEquals(1, urls.size());

        /** Incremental Export */
        job =  generateExportJob(testExportJobId+"_2", 6);

        try {
            performExport(job, testSpaceId1ExtExt, Job.Status.failed, Job.Status.finalized, ContextAwareEvent.SpaceContext.DEFAULT, ApiParam.Query.Incremental.CHANGES);
        }catch (HttpException e){
            assertEquals(NOT_IMPLEMENTED, e.status);
        }
    }

    public Export generateExportJob(String id, int targetLevel){
        int maxTilesPerFile= 15;

        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId("dummy");

        return buildVMTestJob(id, null, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
    }
}
