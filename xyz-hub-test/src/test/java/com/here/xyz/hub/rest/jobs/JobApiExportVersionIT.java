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
import static com.here.xyz.httpconnector.util.jobs.Job.Status.failed;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.finalized;

import com.here.xyz.XyzSerializable;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.models.geojson.implementation.Feature;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;

@Ignore("Obsolete / Deprecated")
@Disabled("Obsolete / Deprecated")
public class JobApiExportVersionIT extends JobApiIT {

    protected String testExportJobId = "export-version-job";
    protected static String scope = "export";
    protected static String testVersionedSpaceId1 = "version-space-1";
    protected static String testVersionedSpaceId1Ext = testVersionedSpaceId1 + "-ext";
    protected static String testVersionedSpaceId2 = "version-space-2";
    private static int versionsToKeep = 29;

    private static void addSomeData(String spaceId, String filename) throws Exception
    {
     try
     {
      URL url = JobApiExportVersionIT.class.getResource(filename);

      for( String line : Files.readAllLines(Paths.get(url.toURI()), StandardCharsets.UTF_8))
      { String geojsonLine = line.split("//")[0].trim();
        if( geojsonLine == null || geojsonLine.isBlank() )
         continue;
        Feature ft = XyzSerializable.deserialize(geojsonLine);
        postFeature(spaceId, ft, AuthProfile.ACCESS_OWNER_1_ADMIN );
      }
      } catch (IOException | URISyntaxException e) {
          e.printStackTrace();
          throw e;
      }
    }

    @BeforeClass
    public static void init() throws Exception {

        String baseDataFile  = "/xyz/hub/export-version-space-1.jsonl.txt",
               deltaDataFile = "/xyz/hub/export-version-space-1-ext.jsonl.txt";

        /** Create test space with CompostitSpace, Version and content */
        createSpaceWithCustomStorage(getScopedSpaceId(testVersionedSpaceId1, scope), "psql", null, versionsToKeep);
        addSomeData( getScopedSpaceId(testVersionedSpaceId1,scope), baseDataFile);

        createSpaceWithExtension(getScopedSpaceId(testVersionedSpaceId1, scope),versionsToKeep);
        addSomeData( getScopedSpaceId(testVersionedSpaceId1Ext,scope), deltaDataFile);

        deleteFeature(getScopedSpaceId(testVersionedSpaceId1Ext,scope), "id003");  // feature in base got deleted in delta

        createSpaceWithCustomStorage(getScopedSpaceId(testVersionedSpaceId2, scope), "psql", null, versionsToKeep);
        addSomeData( getScopedSpaceId(testVersionedSpaceId2,scope), deltaDataFile);

        deleteAllJobsOnSpace(getScopedSpaceId(testVersionedSpaceId1, scope));
        deleteAllJobsOnSpace(getScopedSpaceId(testVersionedSpaceId1Ext, scope));

        deleteAllJobsOnSpace(getScopedSpaceId(testVersionedSpaceId2, scope));
    }

    @AfterClass
    public static void clean(){

        deleteAllJobsOnSpace(getScopedSpaceId(testVersionedSpaceId1, scope));
        deleteAllJobsOnSpace(getScopedSpaceId(testVersionedSpaceId1Ext, scope));
        deleteAllJobsOnSpace(getScopedSpaceId(testVersionedSpaceId2, scope));

        removeSpace(getScopedSpaceId(testVersionedSpaceId1, scope));
        removeSpace(getScopedSpaceId(testVersionedSpaceId1Ext, scope));
        removeSpace(getScopedSpaceId(testVersionedSpaceId2, scope));
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

        downloadAndCheckFC(urls, List.of(1852, 1680), 4, mustContain, 4);
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

        downloadAndCheckFC(urls, List.of(1431, 1331), 3, mustContain, 6);
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

        downloadAndCheckFC(urls, List.of(1810, 1638), 4, mustContain, 5);
    }

    @Test
    public void compositeL1Export_Changes_jsonWkb() throws Exception {

        Export.ExportTarget exportTarget = new Export.ExportTarget().withType(DOWNLOAD);

        /** Create job */
        Export job =  buildTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.JSON_WKB);
        List<URL> urls = performExport(job, getScopedSpaceId(testVersionedSpaceId1Ext, scope), finalized, failed, Export.CompositeMode.CHANGES );

        List<String> mustContain = Arrays.asList("id000", "id002", "id003", "movedFromEmpty", "deltaonly", "'\"deleted'\": true");

        downloadAndCheck(urls, List.of(1200, 1058), 4, mustContain);
    }

    @Test
    public void compositeL1Export_ByVersion_jsonWkb() throws Exception {
        Export.ExportTarget exportTarget = new Export.ExportTarget().withType(DOWNLOAD);

        // create job
        Export job =  buildTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.JSON_WKB);

        // set explict as targetVersion - filters are only mapped by data-hub-dp to legacy jobconfig
        List<URL> urls = performExport(job.withTargetVersion("5"), getScopedSpaceId(testVersionedSpaceId1Ext, scope), finalized, failed, Export.CompositeMode.CHANGES);

        List<String> mustContain = Arrays.asList("id000", "id002", "movedFromEmpty", "deltaonly");

        downloadAndCheck(urls, List.of(622, 566), 2, mustContain);
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

        downloadAndCheck(urls, List.of(1352, 1210), 4, mustContain);
    }

    @Test
    public void compositeL1Export_VersionRange_tileid_partitionedJsonWkb() throws Exception {

            int targetLevel = 12;
            int maxTilesPerFile= 300;

            Export.ExportTarget exportTarget = new Export.ExportTarget()
                    .withType(Export.ExportTarget.Type.VML)
                    .withTargetId(testVersionedSpaceId1Ext+":dummy");

            /** Create job */
            Export job =  buildVMTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.PARTITIONED_JSON_WKB, targetLevel, maxTilesPerFile)
                          .withPartitionKey("tileid");

            /*set explict as targetVersion - filters are only mapped by data-hub-dp to legacy jobconfig*/
             job.setTargetVersion("10..14"); // from,to
            /* */

            List<URL> urls = performExport(job, getScopedSpaceId(testVersionedSpaceId1Ext, scope), finalized, failed,  Export.CompositeMode.CHANGES );

            List<String> mustContain = Arrays.asList("23600772,,","jumpPoint delta","122001322020");

            downloadAndCheck(urls, List.of(359, 331), 1, mustContain);

    }

    @Test
    public void export_VersionRange_tileid_partitionedJsonWkb() throws Exception {
/* non composite */ /* check semantic for next_version with range ? */
            int targetLevel = 12;
            int maxTilesPerFile= 300;

            Export.ExportTarget exportTarget = new Export.ExportTarget()
                    .withType(Export.ExportTarget.Type.VML)
                    .withTargetId(testVersionedSpaceId2+":dummy");

            /** Create job */
            Export job =  buildVMTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.PARTITIONED_JSON_WKB, targetLevel, maxTilesPerFile)
                          .withPartitionKey("tileid");

            /*set explict as targetVersion - filters are only mapped by data-hub-dp to legacy jobconfig*/
             job.setTargetVersion("10..14"); // from,to
            /* */

            List<URL> urls = performExport(job, getScopedSpaceId(testVersionedSpaceId2, scope), finalized, failed,  Export.CompositeMode.CHANGES );

            List<String> mustContain = Arrays.asList("23600776,","jumpPoint delta","122001322020");

            downloadAndCheck(urls, List.of(389, 331), 1, mustContain);
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

        downloadAndCheck(urls, List.of(984, 900), 3, mustContain);
    }

    @Test
    public void compositeL1Export_VersionRange_id_partitionedJsonWkb() throws Exception {

        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testVersionedSpaceId1Ext+":dummy");

        /** Create job */
        Export job = buildTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.PARTITIONED_JSON_WKB)
                     .withPartitionKey("id");
        job.addParam("skipTrigger", true);

        /*set explict as targetVersion - filters are only mapped by data-hub-dp to legacy jobconfig*/
          job.setTargetVersion("10..14"); // from,to
        /* */

        List<URL> urls = performExport(job, getScopedSpaceId(testVersionedSpaceId1Ext, scope), finalized, failed,  Export.CompositeMode.CHANGES );

        List<String> mustContain = Arrays.asList("id007,","jumpPoint delta","122001322020");

        downloadAndCheck(urls, List.of(345, 317), 1, mustContain);
    }

    @Test
    public void export_VersionRange_id_partitionedJsonWkb() throws Exception {
/* non composite */ /* check semantic for next_version with range ? */
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testVersionedSpaceId2+":dummy");

        /** Create job */
        Export job = buildTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.PARTITIONED_JSON_WKB)
                     .withPartitionKey("id");
        job.addParam("skipTrigger", true);

        /*set explict as targetVersion - filters are only mapped by data-hub-dp to legacy jobconfig*/
          job.setTargetVersion("10..14"); // from,to
        /* */

        List<URL> urls = performExport(job, getScopedSpaceId(testVersionedSpaceId2, scope), finalized, failed,  Export.CompositeMode.CHANGES );

        List<String> mustContain = Arrays.asList("id007,","jumpPoint delta","122001322020");

        downloadAndCheck(urls, List.of(375, 317), 1, mustContain);
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

        downloadAndCheck(urls, List.of(1049, 965), 3, mustContain);
    }

}
