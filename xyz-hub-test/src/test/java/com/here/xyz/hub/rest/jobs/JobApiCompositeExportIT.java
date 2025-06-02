/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.JSON_WKB;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.PARTITIONED_JSON_WKB;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.failed;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.finalized;
import static io.netty.handler.codec.http.HttpResponseStatus.PRECONDITION_FAILED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.hub.rest.TestSpaceWithFeature;
import com.here.xyz.hub.rest.TestWithSpaceCleanup;
import com.here.xyz.jobs.datasets.filters.Filters;
import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.util.service.HttpException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;

@Ignore("Obsolete / Deprecated")
@Disabled("Obsolete / Deprecated")
public class JobApiCompositeExportIT extends JobApiIT{
    protected String testExportJobId = "x-test-composite-export-job";

    protected String testSpaceId1 = "composite-export-space";
    protected String testSpaceId1Ext = "composite-export-space-ext";
    protected String testSpaceId1ExtExt = "composite-export-space-ext-ext";

    private static List<String> baseContentWKB = new ArrayList<>(){
        {
            add("id1");
            add("id2");
            add("id3");
            add("id4");
            add("id5");
            add("id6");
            add("id7");
            add("01010000A0E6100000F0F91F681EE23C40FBAA9EC4CEE939400000000000000000");
        }};

    private static List<String> l1ChangesContentTILEID_FC_B64 = new ArrayList<>(){
        {
            add("4807");
            add("4991");
            add("4993");
            add("5693");
            add("5760");
        }};

    private static List<String> l2ChangesContentTILEID_FC_B64 = new ArrayList<>(){
        {
            add("5831");
        }};

    private static List<String> l1ChangesContentJSONWKB = new ArrayList<>(){
        {
            add("idX");
            add("id3");
            add("id7");
            add("01010000A0E61000000B462575422C54C03140C3177DD246400000000000000000");
            add("01010000A0E6100000E4F17A5137D34440D6C58E42E97B43400000000000000000");
        }};

    private static List<String> l1ChangesContentPARTITIONED_JSON_WKB = new ArrayList<>(){
        {
            //add features
            addAll(l1ChangesContentJSONWKB);
            //empty tiles
            add("4807,,");
            add("5760,,");
        }};

    private static List<String> l2ChangesContentPARTITIONED_JSON_WKB = new ArrayList<>(){
        {
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

        Export compositeJob = (Export)loadJob(testSpaceId1Ext, job.getId());
        Export baseJob = (Export)loadJob(testSpaceId1, job.getId()+ "_missing_base");

        assertEquals(4, compositeJob.getMaxSpaceVersion());
        assertEquals(1, compositeJob.getMaxSuperSpaceVersion());
        assertEquals(1, baseJob.getMaxSpaceVersion());
        assertEquals(-42, baseJob.getMaxSuperSpaceVersion());
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
        SpatialFilter spatialFilter = new SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(41.65012, 38.968056)))
                .withRadius(5000);

        Filters filters = new Filters().withSpatialFilter(spatialFilter);
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
        filters = new Filters()
                .withPropertyFilter( "p.foo=test2");
        job.setFilters(filters);

        performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);
        Thread.sleep(100);
        assertNotNull(loadJob(testSpaceId1, job.getId()+ "_missing_base"));
        assertEquals(finalized, loadJob(testSpaceId1, job.getId()+ "_missing_base").getStatus());

        /** Composite Export with existing configuration - requires NO new Export of base */
        job =  generateExportJob(testExportJobId, 7);
        filters = new Filters()
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
        mustContain.addAll(baseContentWKB);
        mustContain.addAll(l1ChangesContentTILEID_FC_B64);

        //7 Features from base + 5 tiles from base+delta changes.
        downloadAndCheck(urls, 2738, 7, mustContain);
    }

    @Test
    public void validCompositeL1ExportWithoutCompositeMode() throws Exception {
        /** Full Export base+delta1 */
        //uses automatically FULL_OPTIMIZED because base layer is readOnly
        Export job =  generateExportJob(testExportJobId, 6);

        /** Initial Base Export */
        List<URL> urls = performExport(job, testSpaceId1Ext, finalized, failed);
        checkUrls(urls, true);

        List<String> mustContain = new ArrayList<>();
        mustContain.addAll(baseContentWKB);
        mustContain.addAll(l1ChangesContentTILEID_FC_B64);

        //7 Features from base + 5 tiles from base+delta changes.
        downloadAndCheck(urls, 2738, 7, mustContain);
    }

    @Test
    public void validCompositeL1ExportFull() throws Exception {
        /** Full Export base+delta1 */
        Export job =  generateExportJob(testExportJobId, 6);

        /** Initial Base Export */
        List<URL> urls = performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.FULL);
        //Only one file (base+delta)
        checkUrls(urls, false);

        List<String> mustContain = new ArrayList<>();
        mustContain.add("4953");
        mustContain.add("4991");
        mustContain.add("4993");
        mustContain.add("5681");
        mustContain.add("5693");
        mustContain.add("5749");
        mustContain.add("5831");
        mustContain.add("iQG5zOmNvbTpoZXJlOnh5");

        //7 Features from base + 5 tiles from base+delta changes.
        downloadAndCheckFC(urls, 2546, 7, mustContain, 7);
    }

    @Test
    public void validCompositeL1ExportFullOptimizedWithoutBase() throws Exception {
        /** Composite Export */
        Export job =  generateExportJob(testExportJobId, 6);
        List<URL> urls = performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);

        checkUrls(urls, true);

        List<String> mustContain = new ArrayList<>();
        mustContain.addAll(baseContentWKB);
        mustContain.addAll(l1ChangesContentTILEID_FC_B64);

        //7 Features from base + 5 tiles from base+delta changes.
        downloadAndCheck(urls, 2738, 7, mustContain);
    }

    @Test
    public void validCompositeL1ExportChanges() throws Exception {
        /** Composite Export */
        Export job =  generateExportJob(testExportJobId, 6);
        List<URL> urls = performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.CHANGES);
        checkUrls(urls, false);

        //3 Features from base+delta changes. 5 Tiles including two empty tiles.
        downloadAndCheckFC(urls, 1142, 3, l1ChangesContentTILEID_FC_B64, 5);
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
        mustContain.addAll(baseContentWKB);
        mustContain.addAll(l2ChangesContentTILEID_FC_B64);

        //7 Features from base + 2 tiles from base+delta changes.
        downloadAndCheck(urls, 1952, 7 , mustContain);
    }

    @Test
    public void validCompositeL2ExportChanges() throws Exception {
        /** Composite Export */
        Export job =  generateExportJob(testExportJobId, 6);
        List<URL> urls = performExport(job, testSpaceId1ExtExt, finalized, failed, Export.CompositeMode.CHANGES);

        checkUrls(urls, false);

        //* One Feature got added and one got deleted .. so we expect 1 Feature + 2 Tiles (one is empty) */
        downloadAndCheckFC(urls, 356, 1, l2ChangesContentTILEID_FC_B64, 2);
    }

    /** ######################## JSON_WKB / PARTITIONED_JSON_WKB ################################ **/

    @Test
    public void validCompositeL1ExportFullOptimizedWithoutBaseJSON_WKB() throws Exception {
        /** Composite Export - format gets automatically override to PARTITIONED_JSON_WKB */
        Export job =  buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(DOWNLOAD), JSON_WKB);
        job.setTargetLevel(6);

        List<URL> urls = performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);
        checkUrls(urls, true);

        List<String> mustContain = new ArrayList<>();
        mustContain.addAll(baseContentWKB);
        mustContain.addAll(l1ChangesContentPARTITIONED_JSON_WKB);

        //7 Features from base + 5 Features from delta - including 2 deletions.
        downloadAndCheck(urls, 2368, 10, mustContain);

        Job job1 = loadJob(testSpaceId1Ext, job.getId());
        assertEquals(PARTITIONED_JSON_WKB, job1.getCsvFormat());

        Job spawendJob = loadJob(testSpaceId1, job.getId()+ "_missing_base");
        assertEquals(JSON_WKB, spawendJob.getCsvFormat());
        assertEquals(-1, spawendJob.getKeepUntil());
    }

    @Test
    public void validCompositeL1ExportFullOptimizedWithoutBasePARTITIONED_JSON_WKB() throws Exception {
        /** Composite Export */
        Export job = generateExportJob(testExportJobId, 6 , PARTITIONED_JSON_WKB);
        List<URL> urls = performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);

        checkUrls(urls, true);

        List<String> mustContain = new ArrayList<>();
        mustContain.addAll(baseContentWKB);
        mustContain.addAll(l1ChangesContentPARTITIONED_JSON_WKB);

        //7 Features from base + 5 Features from delta - including 2 deletions.
        downloadAndCheck(urls, 2368, 10, mustContain);

        // Same test with type DOWNLOAD
        job =  buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(DOWNLOAD), PARTITIONED_JSON_WKB);
        job.setTargetLevel(6);

        urls = performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.FULL_OPTIMIZED);
        checkUrls(urls, true);

        //7 Features from base + 2 base+delta tiles with changes
        downloadAndCheck(urls, 2368, 10, mustContain);
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
        mustContain.addAll(l1ChangesContentPARTITIONED_JSON_WKB);

        //7 Features from base + 5 Features from delta - including 2 deletions.
        downloadAndCheck(urls, 2368, 10, mustContain);
    }

    @Test
    public void validCompositeL1ExportChangesPARTITIONED_JSON_WKB() throws Exception {
        /** Composite Export */
        Export job =  generateExportJob(testExportJobId, 6 , PARTITIONED_JSON_WKB);
        List<URL> urls = performExport(job, testSpaceId1Ext, finalized, failed, Export.CompositeMode.CHANGES);
        checkUrls(urls, false);

        List<String> mustContain = new ArrayList<>();
        mustContain.addAll(l1ChangesContentPARTITIONED_JSON_WKB);

        //3 Features from delta + 2 empty tiles
        downloadAndCheck(urls, 772, 3, mustContain);
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
        mustContain.addAll(l2ChangesContentPARTITIONED_JSON_WKB);

        //8 Features from base + delta + 1 empty tile
        downloadAndCheck(urls, 1856, 8 , mustContain);
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
