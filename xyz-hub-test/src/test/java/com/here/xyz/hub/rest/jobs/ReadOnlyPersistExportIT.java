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
import com.here.xyz.hub.rest.TestSpaceWithFeature;
import com.here.xyz.hub.rest.TestWithSpaceCleanup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URL;
import java.util.List;

import static com.here.xyz.httpconnector.util.jobs.Export.ExportTarget.Type.DOWNLOAD;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.GEOJSON;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.failed;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.finalized;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ReadOnlyPersistExportIT extends JobApiIT {
    protected String testExportJobId = "x-test-persistent-export-job";
    protected static String testSpaceId1 = "persistent-export-space";

    @BeforeClass
    public static void init(){
        TestSpaceWithFeature.createSpaceWithCustomStorage(testSpaceId1, "psql", null);
        TestSpaceWithFeature.addFeatures(testSpaceId1, "/xyz/hub/fcWithPoints.json", 7);
        TestSpaceWithFeature.setReadOnly(testSpaceId1);

        deleteAllJobsOnSpace(testSpaceId1);
    }

    @AfterClass
    public static void clean(){
        TestWithSpaceCleanup.removeSpace(testSpaceId1);
    }

    @Test
    public void testFullReadOnlyWKBExport() throws Exception {
        /** Export JSON_WKB */
        Export job = buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(DOWNLOAD), Job.CSVFormat.JSON_WKB);
        List<URL> urls = performExport(job, testSpaceId1 , finalized, failed);
        assertNotEquals(-1, urls.get(0).toString().indexOf("persistent"));

        /** Same Export-Config - reuse existing persistent one */
        job = buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(DOWNLOAD), Job.CSVFormat.JSON_WKB);
        List<URL> urls2 = performExport(job, testSpaceId1 , finalized, failed);
        assertEquals(urls.get(0).getPath(), urls2.get(0).getPath());

        /** Different Export-Config - need new Export */
        job = buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(DOWNLOAD), GEOJSON);
        urls2 = performExport(job, testSpaceId1 , finalized, failed);
        assertNotEquals(urls.get(0).getPath(), urls2.get(0).getPath());
    }
}
