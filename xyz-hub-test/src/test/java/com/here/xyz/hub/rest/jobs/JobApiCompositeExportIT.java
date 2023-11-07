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

import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.rest.TestSpaceWithFeature;
import com.here.xyz.hub.rest.TestWithSpaceCleanup;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.here.xyz.httpconnector.util.jobs.Export.ExportTarget.Type.DOWNLOAD;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.*;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.failed;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.finalized;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.PRECONDITION_FAILED;
import static org.junit.Assert.*;

public class JobApiCompositeExportIT extends JobApiIT{
    protected String testExportJobId = "x-test-composite-export-job";

    protected String testSpaceId1 = "composite-export-space";
    protected String testSpaceId1Ext = "composite-export-space-ext";
    protected String testSpaceId1ExtExt = "composite-export-space-ext-ext";

    private static List<String> baseContent = new ArrayList<>(){
        {
            add("4807");
            add("4953");
            add("5681");
            add("5693");
            add("5749");
            add("5760");
            add("5831");
        }};

    private static List<String> l1ChangesContent = new ArrayList<>(){
        {
            add("4807");
            add("4991");
            add("4993");
            add("5693");
            add("5760");
        }};

    private static List<String> l2ChangesContent = new ArrayList<>(){
        {
            add("5831");
        }};

    private static List<String> baseContentWKB = new ArrayList<>(){
        {
            addAll(baseContent);
            add("id1");
            add("id2");
            add("id3");
            add("id4");
            add("id5");
            add("id6");
            add("id7");
            add("01010000A0E6100000B0CD1A681EE23C403411B4C4CEE939400000000000000000");
        }};

    private static List<String> l1ChangesContentWKB = new ArrayList<>(){
        {
            addAll(l1ChangesContent);
            add("idX");
            add("id3");
            add("id7");
            add("01010000A0E61000007DF82175422C54C0D056CC177DD246400000000000000000");
            add("01010000A0E610000038D07F5137D34440F0968E42E97B43400000000000000000");
            //empty tiles
            add("4807,,");
            add("5760,,");
        }};

    private static List<String> l2ChangesContentWKB = new ArrayList<>(){
        {
            addAll(l2ChangesContent);
            add("id8");
            //empty tiles
            add("5831,,");
        }};

    @BeforeClass
    public static void setup() { }

    @Before
    public void prepare() {
        TestSpaceWithFeature.createSpaceWithCustomStorage(testSpaceId1, "psql", null);
        TestSpaceWithFeature.addFeatures(testSpaceId1, "/xyz/hub/fcWithPoints.json", 7);
        TestSpaceWithFeature.setReadOnly(testSpaceId1);

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
        //deleteAllJobsOnSpace(testSpaceId1);
        //deleteAllJobsOnSpace(testSpaceId1Ext);
        //deleteAllJobsOnSpace(testSpaceId1ExtExt);

        TestWithSpaceCleanup.removeSpace(testSpaceId1);
        TestWithSpaceCleanup.removeSpace(testSpaceId1Ext);
        TestWithSpaceCleanup.removeSpace(testSpaceId1ExtExt);
    }

    @Test
    public void missingPersistentBaseExport() throws Exception{
        Export job =  generateExportJob(testExportJobId, 4);
        List<URL> urls = performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);
        checkUrls(urls, true);

//        job =  generateExportJob(testExportJobId, 4);
//        urls = performExport(job, testSpaceId1ExtExt, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);
//        checkUrls(urls, true);
    }

    @Test
    public void wrongTargetLevel() throws Exception {
        Export job =  generateExportJob(testExportJobId, 4);

        /** Initial Base Export on testSpaceId1 */
        performExport(job, testSpaceId1, finalized, failed);
        deleteAllJobsOnSpace(testSpaceId1);

        job =  generateExportJob(testExportJobId, 8);
        /** Composite Export on testSpaceId1Ext - Base layer got exported on level 4 now we want an Composite export on another level */
        try {
            performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);
        }
        catch (HttpException e){
            assertEquals(PRECONDITION_FAILED, e.status);
        }
    }

    @Test
    public void validCompositeL1ExportDifferentFilter() throws Exception {
        Export job =  generateExportJob(testExportJobId, 6);

        /** Initial Base Export */
        List<URL> urls = performExport(job, testSpaceId1, finalized, failed);
        assertEquals(1, urls.size());

        /** Composite Export with filter - requires new Export of base */
        job =  generateExportJob(testExportJobId, 6);
        Export.SpatialFilter spatialFilter = new Export.SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(41.65012, 38.968056)))
                .withRadius(5000);

        Export.Filters filters = new Export.Filters().withSpatialFilter(spatialFilter);
        job.setFilters(filters);
        performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);
        Thread.sleep(100);
        assertNotNull(loadJob(testSpaceId1, job.getId()+ "_missing_base"));
        assertEquals(finalized, loadJob(testSpaceId1, job.getId()+ "_missing_base").getStatus());

        /** Composite Export with different targetLevel - requires new Export of base */
        job =  generateExportJob(testExportJobId, 7);
        job.setFilters(filters);

        performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);
        Thread.sleep(100);
        assertNotNull(loadJob(testSpaceId1, job.getId()+ "_missing_base"));
        assertEquals(finalized, loadJob(testSpaceId1, job.getId()+ "_missing_base").getStatus());

        /** Composite Export with new property filter - requires new Export of base */
        job =  generateExportJob(testExportJobId, 7);
        filters = new Export.Filters()
                .withPropertyFilter( "p.foo=test2");
        job.setFilters(filters);

        performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);
        Thread.sleep(100);
        assertNotNull(loadJob(testSpaceId1, job.getId()+ "_missing_base"));
        assertEquals(finalized, loadJob(testSpaceId1, job.getId()+ "_missing_base").getStatus());

        /** Composite Export with existing configuration - requires NO new Export of base */
        job =  generateExportJob(testExportJobId, 7);
        filters = new Export.Filters()
                .withPropertyFilter( "p.foo=test2");
        job.setFilters(filters);
        performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);
        assertNull(loadJob(testSpaceId1, job.getId()+ "_missing_base"));
    }

    @Test
    public void validCompositeL1ExportFullOptimizedWithExistingBase() throws Exception {
        Export job =  generateExportJob(testExportJobId, 6);

        /** Initial Base Export */
        List<URL> urls = performExport(job, testSpaceId1, finalized, failed);
        assertEquals(1, urls.size());

        /** Composite Export */
        job =  generateExportJob(testExportJobId+"_2", 6);
        urls = performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);

        /** Expect 2 files - 1 persistent 1 not persistent*/
        checkUrls(urls, true);

        List<String> mustContain = new ArrayList<>();
        mustContain.addAll(baseContent);
        mustContain.add("NjQ3LCAyNS45MTM");
        mustContain.addAll(l1ChangesContent);

        //7 Features from base + 3 Features from base+delta changes. 12 Tiles including two empty tiles.
        downloadAndCheckFC(urls, 4276, 10, mustContain, 12);
    }

    @Test
    public void validCompositeL1ExportFull() throws Exception {
        /** Full Export base+delta1 */
        //uses automatically FULL_OPTIMIZED because base layer is readOnly
        Export job =  generateExportJob(testExportJobId, 6);

        /** Initial Base Export */
        List<URL> urls = performExport(job, testSpaceId1Ext, finalized, failed);
        checkUrls(urls, true);

        List<String> mustContain = new ArrayList<>();
        mustContain.addAll(baseContent);
        mustContain.add("NjQ3LCAyNS45MTM");
        mustContain.addAll(l1ChangesContent);

        //7 Features from base + 3 Features from base+delta changes. 12 Tiles including two empty tiles.
        downloadAndCheckFC(urls, 4276, 10, mustContain, 12);
    }

    @Test
    public void validCompositeL1ExportFullOptimizedWithoutBase() throws Exception {
        /** Composite Export */
        Export job =  generateExportJob(testExportJobId, 6);
        List<URL> urls = performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);

        checkUrls(urls, true);

        List<String> mustContain = new ArrayList<>();
        mustContain.addAll(baseContent);
        mustContain.add("NjQ3LCAyNS45MTM");
        mustContain.addAll(l1ChangesContent);

        //7 Features from base + 3 Features from base+delta changes. 12 Tiles including two empty tiles.
        downloadAndCheckFC(urls, 4276, 10, mustContain, 12);
    }

    @Test
    public void validCompositeL1ExportChanges() throws Exception {
        /** Composite Export */
        Export job =  generateExportJob(testExportJobId, 6);
        List<URL> urls = performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.CHANGES);
        checkUrls(urls, false);

        //3 Features from base+delta changes. 5 Tiles including two empty tiles.
        downloadAndCheckFC(urls, 1346, 3, l1ChangesContent, 5);
    }

    @Test
    public void validCompositeL2ExportFullOptimizedWithExistingBase() throws Exception {
        Export job =  generateExportJob(testExportJobId, 6);

        /** Initial persistent Base Export */
        List<URL> urls = performExport(job, testSpaceId1, finalized, failed);
        assertEquals(1, urls.size());
        assertNotEquals(-1, urls.get(0).toString().indexOf("persistent"));

        /** Composite Export */
        job =  generateExportJob(testExportJobId, 6);
        urls = performExport(job, testSpaceId1ExtExt, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);
        checkUrls(urls, true);

        List<String> mustContain = new ArrayList<>();
        mustContain.addAll(baseContent);
        mustContain.add("NjQ3LCAyNS45MTM");
        mustContain.addAll(l2ChangesContent);
        
        downloadAndCheckFC(urls, 3362, 8 , mustContain, 9);
    }

    @Test
    public void validCompositeL2ExportChanges() throws Exception {
        /** Composite Export */
        Export job =  generateExportJob(testExportJobId, 6);
        List<URL> urls = performExport(job, testSpaceId1ExtExt, finalized, failed, Export.CompositeMode.CHANGES);

        checkUrls(urls, false);

        //* One Feature got added and one got deleted .. so we expect 1 Feature + 2 Tiles (one is empty) */
        downloadAndCheckFC(urls, 432, 1, l2ChangesContent, 2);
    }

    /** ######################## JSON_WKB / PARTITIONED_JSON_WKB ################################ **/

    @Test
    public void validCompositeL1ExportFullOptimizedWithoutBaseJSON_WKB() throws Exception {
        /** Composite Export */
        Export job =  buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(DOWNLOAD), JSON_WKB);

        List<URL> urls = performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);

        checkUrls(urls, true);

        List<String> mustContain = new ArrayList<>();
        mustContain.add("id1");
        mustContain.add("id2");
        mustContain.add("id3");
        mustContain.add("id4");
        mustContain.add("id5");
        mustContain.add("id6");
        mustContain.add("id7");
        //delta
        mustContain.add("idX");
        mustContain.add("id3");
        mustContain.add("id3");
        mustContain.add("id7");
        //Feature with deleted flag
        mustContain.add("deleted");

        //7 Features from base + 4 Features from delta - including one deletion.
        downloadAndCheck(urls, 3002, 11, mustContain);
    }

    @Test
    public void validCompositeL1ExportFullOptimizedWithoutBasePARTITIONED_JSON_WKB() throws Exception {
        /** Composite Export */
        Export job = generateExportJob(testExportJobId, 6 , PARTITIONED_JSON_WKB);
        List<URL> urls = performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);

        checkUrls(urls, true);

        List<String> mustContain = new ArrayList<>();
        mustContain.addAll(baseContentWKB);
        mustContain.addAll(l1ChangesContentWKB);

        //7 Features from base + 4 Features from delta - including one deletion.
        downloadAndCheck(urls, 2945, 10, mustContain);

        // Same test with type DOWNLOAD
        job =  buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(DOWNLOAD), PARTITIONED_JSON_WKB);
        job.setTargetLevel(6);

        urls = performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);
        checkUrls(urls, true);

        //7 Features from base + 4 Features from delta - including one deletion.
        downloadAndCheck(urls, 2945, 10, mustContain);
    }

    @Test
    public void validCompositeL1ExportFullOptimizedWithExistingBasePARTITIONED_JSON_WKB() throws Exception {
        Export job = generateExportJob(testExportJobId, 6 , PARTITIONED_JSON_WKB);

        /** Initial Base Export */
        List<URL> urls = performExport(job, testSpaceId1, finalized, failed);
        assertEquals(1, urls.size());

        /** Composite Export */
        job =  generateExportJob(testExportJobId+"_2", 6 , PARTITIONED_JSON_WKB);
        urls = performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);

        /** Expect 2 files - 1 persistent 1 not persistent*/
        checkUrls(urls, true);

        List<String> mustContain = new ArrayList<>();
        mustContain.addAll(baseContentWKB);
        mustContain.addAll(l1ChangesContentWKB);

        //7 Features from base + 4 Features from delta - including one deletion.
        downloadAndCheck(urls, 2945, 10, mustContain);
    }

    @Test
    public void validCompositeL1ExportChangesPARTITIONED_JSON_WKB() throws Exception {
        /** Composite Export */
        Export job =  generateExportJob(testExportJobId, 6 , PARTITIONED_JSON_WKB);
        List<URL> urls = performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.CHANGES);
        checkUrls(urls, false);

        List<String> mustContain = new ArrayList<>();
        mustContain.addAll(l1ChangesContentWKB);

        //3 Features from delta + 2 empty tiles
        downloadAndCheck(urls, 943, 3, mustContain);
    }

    @Test
    public void validCompositeL2ExportFullOptimizedWithExistingBasePARTITIONED_JSON_WKB() throws Exception {
        Export job =  generateExportJob(testExportJobId, 6 , PARTITIONED_JSON_WKB);

        /** Initial persistent Base Export */
        List<URL> urls = performExport(job, testSpaceId1, finalized, failed);
        assertEquals(1, urls.size());
        assertNotEquals(-1, urls.get(0).toString().indexOf("persistent"));

        /** Composite Export */
        job =  generateExportJob(testExportJobId, 6 , PARTITIONED_JSON_WKB);
        urls = performExport(job, testSpaceId1ExtExt, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);
        checkUrls(urls, true);

        List<String> mustContain = new ArrayList<>();
        mustContain.addAll(baseContentWKB);
        mustContain.addAll(l2ChangesContentWKB);
        //empty tiles
        mustContain.add("5831,,");

        //8 Features from base + delta + 1 empty tile
        downloadAndCheck(urls, 2323, 8 , mustContain);
    }

    /** ######################################################## **/

    private void checkUrls(List<URL> urls, boolean expectPersistent){
        if(expectPersistent) {
            assertEquals(2, urls.size());
            assertEquals(-1, urls.get(0).toString().indexOf("persistent"));
            assertNotEquals(-1, urls.get(1).toString().indexOf("persistent"));
        }else {
            assertEquals(1, urls.size());
            assertEquals(-1, urls.get(0).toString().indexOf("persistent"));
        }
    }

    public Export generateExportJob(String id, int targetLevel){
        return generateExportJob(id, targetLevel, Job.CSVFormat.TILEID_FC_B64);
    }

    public Export generateExportJob(String id, int targetLevel, Job.CSVFormat format){
        int maxTilesPerFile= 15;

        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId("dummy");

        return buildVMTestJob(id, null, exportTarget, format, targetLevel, maxTilesPerFile);
    }
}
