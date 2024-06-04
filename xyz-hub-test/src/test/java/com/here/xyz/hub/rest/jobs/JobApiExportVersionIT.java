/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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
import static com.here.xyz.httpconnector.util.jobs.Job.Status.failed;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.finalized;

import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class JobApiExportVersionIT extends JobApiIT {

    protected String testExportJobId = "export-version-job";
    protected static String scope = "export";
    protected static String testVersionedSpaceId1 = "version-space-1";
    protected static String testVersionedSpaceId1Ext = testVersionedSpaceId1 + "-ext";
    private static int versionsToKeep = 10;

    @BeforeClass
    public static void init(){

        final Feature[]
        baseFeatures =
        { newFeature().withId("id000")
                      .withGeometry(new Point().withCoordinates(new PointCoordinates( 8.61,50.020093)))  // base  - htile 122001322003 , 23600771
                      .withProperties(new Properties().with("group", "shouldBeEmpty")
                                                      .with("vflag", "oldest")),
          newFeature().withId("id000")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates( 8.61,50.020093)))  // base  - htile 122001322003 , 23600771
                        .withProperties(new Properties().with("group", "shouldBeEmpty")
                                                        .with("vflag", "inbetween")),
          newFeature().withId("id000")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates( 8.61,50.020093)))  // base  - htile 122001322003 , 23600771
                        .withProperties(new Properties().with("group", "shouldBeEmpty")
                                                        .with("vflag", "newest")),
          newFeature().withId("id001")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates( 8.6199615,50.020093))) // htile 122001322012 , 23600774
                        .withProperties(new Properties().with("group", "baseonly")
                                                        .with("vflag", "oldest")),
          newFeature().withId("id001")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates( 8.6199615,50.020093))) // htile 122001322012 , 23600774
                        .withProperties(new Properties().with("group", "baseonly")
                                                        .with("vflag", "inbetween")),
          newFeature().withId("id001")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates( 8.6199615,50.020093))) // htile 122001322012 , 23600774
                        .withProperties(new Properties().with("group", "baseonly")
                                                        .with("vflag", "newest")),
          newFeature().withId("id003")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates( 8.6199615,50.020093))) // htile 122001322012 , 23600774
                        .withProperties(new Properties().with("group", "deletedInDelta")
                                                        .with("vflag", "oldest")),
          newFeature().withId("id003")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates( 8.6199615,50.020093))) // htile 122001322012 , 23600774
                        .withProperties(new Properties().with("group", "deletedInDelta")
                                                        .with("vflag", "inbetween")),
          newFeature().withId("id003")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates( 8.6199615,50.020093))) // htile 122001322012 , 23600774
                        .withProperties(new Properties().with("group", "deletedInDelta")
                                                        .with("vflag", "newest"))
        },

        deltaFeatures =
        { newFeature().withId("id000")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates( 8.71,50.020093))) // delta - htile 122001322013 , 23600775
                        .withProperties(new Properties().with("group", "movedFromEmpty")
                                                        .with("vflag", "oldest")),
          newFeature().withId("id000")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates( 8.71,50.020093))) // delta - htile 122001322013 , 23600775
                        .withProperties(new Properties().with("group", "movedFromEmpty")
                                                        .with("vflag", "inbetween")),
          newFeature().withId("id000")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates( 8.71,50.020093))) // delta - htile 122001322013 , 23600775
                        .withProperties(new Properties().with("group", "movedFromEmpty")
                                                        .with("vflag", "newest")),
          newFeature().withId("id002")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates( 8.6199615,50.020093))) // htile 122001322012 , 23600774
                        .withProperties(new Properties().with("group", "deltaonly")
                                                        .with("vflag", "oldest")),
          newFeature().withId("id002")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates( 8.6199615,50.020093))) // htile 122001322012 , 23600774
                        .withProperties(new Properties().with("group", "deltaonly")
                                                        .with("vflag", "inbetween")),
          newFeature().withId("id002")
                        .withGeometry(new Point().withCoordinates(new PointCoordinates( 8.6199615,50.020093))) // htile 122001322012 , 23600774
                        .withProperties(new Properties().with("group", "deltaonly")
                                                        .with("vflag", "newest"))
        };

        /** Create test space with CompostitSpace, Version and content */
        createSpaceWithCustomStorage(getScopedSpaceId(testVersionedSpaceId1, scope), "psql", null, versionsToKeep);

        for( Feature bft : baseFeatures )
         postFeature(getScopedSpaceId(testVersionedSpaceId1,scope), bft, AuthProfile.ACCESS_OWNER_1_ADMIN );

        createSpaceWithExtension(getScopedSpaceId(testVersionedSpaceId1, scope),versionsToKeep);

        for( Feature dft : deltaFeatures )
         postFeature(getScopedSpaceId(testVersionedSpaceId1Ext,scope), dft, AuthProfile.ACCESS_OWNER_1_ADMIN );

        deleteFeature(getScopedSpaceId(testVersionedSpaceId1Ext,scope), "id003");  // feature in base got deleted in delta

        deleteAllJobsOnSpace(getScopedSpaceId(testVersionedSpaceId1, scope));
        deleteAllJobsOnSpace(getScopedSpaceId(testVersionedSpaceId1Ext, scope));
    }

    @AfterClass
    public static void clean(){

        deleteAllJobsOnSpace(getScopedSpaceId(testVersionedSpaceId1, scope));
        deleteAllJobsOnSpace(getScopedSpaceId(testVersionedSpaceId1Ext, scope));

        removeSpace(getScopedSpaceId(testVersionedSpaceId1, scope));
        removeSpace(getScopedSpaceId(testVersionedSpaceId1Ext, scope));
    }

    @Test
    public void compositeL1Export_id_partitionidFcB64() throws Exception {
// export by ID
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testVersionedSpaceId1Ext+":dummy");

        /** Create job */
        Export job =  buildTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.PARTITIONID_FC_B64);
        job.addParam("skipTrigger", true);
        List<URL> urls = performExport(job, getScopedSpaceId(testVersionedSpaceId1Ext, scope), finalized, failed);

        List<String> mustContain = Arrays.asList("id000,", "id001,", "id002,", "aWQwMDAi","aWQwMDIi","aWQwMDEi"); // ids + b64(~ids)

        downloadAndCheckFC(urls, 1445, 3, mustContain, 3);
    }

    @Test
    public void compositeL1Export_Changes_partitionkey_partitionidFcB64() throws Exception {

        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testVersionedSpaceId1Ext+":dummy");

        /** Create job */
        Export job =  buildTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.PARTITIONID_FC_B64).withPartitionKey("p.group");
        job.addParam("skipTrigger", true);

        List<URL> urls = performExport(job, getScopedSpaceId(testVersionedSpaceId1Ext, scope), finalized, failed, Export.CompositeMode.CHANGES );

        List<String> mustContain = Arrays.asList("deltaonly","movedFromEmpty","shouldBeEmpty","deletedInDelta");

        downloadAndCheckFC(urls, 1002, 2, mustContain, 4);
    }

    @Test
    public void compositeL1Export_Changes_tileidFcB64() throws Exception {

        int targetLevel = 12;
        int maxTilesPerFile= 300;

        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testVersionedSpaceId1Ext+":dummy");

        /** Create job */
        Export job =  buildVMTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);

        List<URL> urls = performExport(job, getScopedSpaceId(testVersionedSpaceId1Ext, scope), finalized, failed,  Export.CompositeMode.CHANGES );

        List<String> mustContain = Arrays.asList("23600771","23600774","23600775", "AwMiIs","AwMCIs");

        downloadAndCheckFC(urls, 1390, 3, mustContain, 3);
    }

    @Test
    public void compositeL1Export_Changes_jsonWkb() throws Exception {

        Export.ExportTarget exportTarget = new Export.ExportTarget().withType(DOWNLOAD);

        /** Create job */
        Export job =  buildTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.JSON_WKB);
        List<URL> urls = performExport(job, getScopedSpaceId(testVersionedSpaceId1Ext, scope), finalized, failed, Export.CompositeMode.CHANGES );

        List<String> mustContain = Arrays.asList("id000", "id002", "id003", "movedFromEmpty", "deltaonly", "'\"deleted'\": true");

        downloadAndCheck(urls, 802, 3, mustContain);
    }

    @Test
    public void compositeL1Export_ByVersion_jsonWkb() throws Exception {
        Export.ExportTarget exportTarget = new Export.ExportTarget().withType(DOWNLOAD);

        // create job
        Export job =  buildTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.JSON_WKB);

        // set explict as targetVersion - filters are only mapped by data-hub-dp to legacy jobconfig
        List<URL> urls = performExport(job.withTargetVersion("5"), getScopedSpaceId(testVersionedSpaceId1Ext, scope), finalized, failed, Export.CompositeMode.CHANGES);

        List<String> mustContain = Arrays.asList("id000", "id002", "movedFromEmpty", "deltaonly");

        downloadAndCheck(urls, 682, 2, mustContain);
    }


    @Test
    public void compositeL1Export_Changes_tileid_partitionedJsonWkb() throws Exception {

        int targetLevel = 12;
        int maxTilesPerFile= 300;

        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testVersionedSpaceId1Ext+":dummy");

        /** Create job */
        Export job =  buildVMTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.PARTITIONED_JSON_WKB, targetLevel, maxTilesPerFile)
                      .withPartitionKey("tileid");

        List<URL> urls = performExport(job, getScopedSpaceId(testVersionedSpaceId1Ext, scope), finalized, failed,  Export.CompositeMode.CHANGES );

        List<String> mustContain = Arrays.asList("23600771,,","23600774","23600775", "deltaonly","baseonly","movedFromEmpty");

        downloadAndCheck(urls, 1053, 3, mustContain);
    }

    @Test
    public void compositeL1Export_Changes_id_partitionedJsonWkb() throws Exception {

        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testVersionedSpaceId1Ext+":dummy");

        /** Create job */
        Export job = buildTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.PARTITIONED_JSON_WKB)
                     .withPartitionKey("id");
        job.addParam("skipTrigger", true);

        List<URL> urls = performExport(job, getScopedSpaceId(testVersionedSpaceId1Ext, scope), finalized, failed,  Export.CompositeMode.CHANGES );

        List<String> mustContain = Arrays.asList("id000,","id002,","id003,,", "deltaonly","movedFromEmpty");

        downloadAndCheck(urls, 699, 2, mustContain);
    }

    @Test
    public void compositeL1Export_Changes_partitionkey_partitionedJsonWkb() throws Exception {

        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testVersionedSpaceId1Ext+":dummy");

        /** Create job */
        Export job = buildTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.PARTITIONED_JSON_WKB)
                     .withPartitionKey("p.group");
        job.addParam("skipTrigger", true);

        List<URL> urls = performExport(job, getScopedSpaceId(testVersionedSpaceId1Ext, scope), finalized, failed,  Export.CompositeMode.CHANGES );

        List<String> mustContain = Arrays.asList("deletedInDelta,,","deltaonly,","movedFromEmpty,", "deltaonly","shouldBeEmpty,,");

        downloadAndCheck(urls, 737, 2, mustContain);
    }

}
