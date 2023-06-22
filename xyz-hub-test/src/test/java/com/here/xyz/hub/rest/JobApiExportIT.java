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

import com.here.xyz.XyzSerializable;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class JobApiExportIT extends JobApiIT {

    protected String testExportJobId = "x-test-export-job";

    @Before
    public void clean(){
        deleteAllJobsOnSpace(testSpaceId1);
        deleteAllJobsOnSpace(testSpaceId2);
        deleteAllJobsOnSpace(testSpaceId2Ext);
        deleteAllJobsOnSpace(testSpaceId2ExtExt);
    }

    /**
     *  EXPORT full space
     *  TYPE: "DOWNLOAD"
     *  CSVFormat: "WKB"
     * */
    @Test
    public void testFullWKBExport() throws Exception {
        /** Create job */
        Export job = buildTestJob(null, new Export.ExportTarget().withType(Export.ExportTarget.Type.DOWNLOAD), Job.CSVFormat.JSON_WKB);
        List<URL> urls = performExport(job, testSpaceId2, Job.Status.finalized, Job.Status.failed);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("01020000A0E61000000300000041C17BFDFF662140E08442041C14494000000000000000006BDE27FD732F21406C5CFFAECF0A49400000000000000000A9B7ABFCD7162140A96A82A8FB0E49400000000000000000");
            add("01030000A0E6100000010000000500000083E4F8FC8B17214074F04C689202494000000000000000004DF564FED1A3214074F04C689202494000000000000000004DF564FED1A32140876D8B321B184940000000000000000083E4F8FC8B172140876D8B321B184940000000000000000083E4F8FC8B17214074F04C68920249400000000000000000");
            add("foo_polygon");
        }};

        downloadAndCheck(urls, 5401, 11, mustContains );
    }

    /**
     *  EXPORT filtered space
     *  TYPE: "DOWNLOAD"
     *  CSVFormat: "WKB"
     * */
    @Test
    public void testFilteredWKBExportWithSpatialFilterClipped() throws Exception {
        Export.SpatialFilter spatialFilter = new Export.SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(true)
                .withRadius(5500);

        Export.Filters filters = new Export.Filters().withSpatialFilter(spatialFilter);
        Export.ExportTarget exportTarget = new Export.ExportTarget().withType(Export.ExportTarget.Type.DOWNLOAD);

        Export job = buildTestJob(filters, exportTarget, Job.CSVFormat.JSON_WKB);
        List<URL> urls = performExport(job, testSpaceId2, Job.Status.finalized, Job.Status.failed);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("foo_polygon");
            add("01030000A0E61000000100000021000000DA81E05D2D7E21404E355FE7F70B49400000000000000000DE78CF5FF67B2140D4078B84C70A494000000000000000007F932C6359782140671DB2C2AE094940000000000000000058FB14157A73214019160768B8084940000000000000000093A77370886D21405E1C90E7ED0749400000000000000000C6A82FE4BE6621400654E3045707494000000000000000000415FE15605F2140A53D7888F906494000000000000000008B4C9B57B457214055905B07D90649400000000000000000DD61AFE6065021400D6A58C0F6064940000000000000000048B14F11A34821409417DE8F5107494000000000000000005360E959D141214006CD13FBE507494000000000000000002CBA64B5D43B2140D53CB251AE0849400000000000000000513B80FDE7362140F1D664E5A209494000000000000000007591BCAF3B332140B75FA854BA0A49400000000000000000CDA0BB0EF4302140BB2F61E6E90B49400000000000000000A718B2B8273021402E50BFF2250D494000000000000000000E6B89C1DE3021402B058055620E49400000000000000000FB77A35A1233214044E038E5920F494000000000000000000C98F80CAD362140990F1DEBAB1049400000000000000000B81DAE848B3B2140A3089E96A211494000000000000000008C8A7CE77D412140E42A67686D1249400000000000000000BA1D89A94948214051F18090041349400000000000000000682AFCCEAB4F2140F011E23B62134940000000000000000019C8E4835B572140F29357CE821349400000000000000000590827F00C5F2140C5447206651349400000000000000000020F7E2A7466214009DE070A0A134940000000000000000016D4312C486D2140E6D7C65A751249400000000000000000DFBF17A745732140407152B3AC11494000000000000000002B55CEA03178214034B34DCEB71049400000000000000000A010C9B8DB7B2140EA379D19A00F494000000000000000008FD98802207E21407169EE58700E4940000000000000000054590D62E87E21406979373B340D49400000000000000000DA81E05D2D7E21404E355FE7F70B49400000000000000000");
        }};

        downloadAndCheck(urls, 4062, 5, mustContains );
    }

    @Test
    public void testFilteredWKBExportWithSpatialFilterUnclipped() throws Exception {
        Export.SpatialFilter spatialFilter = new Export.SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(false)
                .withRadius(5500);

        Export.Filters filters = new Export.Filters().withSpatialFilter(spatialFilter);
        Export.ExportTarget exportTarget = new Export.ExportTarget().withType(Export.ExportTarget.Type.DOWNLOAD);

        Export job = buildTestJob(filters, exportTarget, Job.CSVFormat.JSON_WKB);
        List<URL> urls = performExport(job, testSpaceId2, Job.Status.finalized, Job.Status.failed);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("foo_polygon");
            add("01020000A0E610000004000000901FE0FF7D70214087A4164A260B4940000000000000000041237DFF8B4421402A7288B8390549400000000000000000407E80FFF70521400264E8D8410749400000000000000000F60E12FD3504214019A9F7544E0349400000000000000000");
        }};

        downloadAndCheck(urls, 2622, 5, mustContains );
    }

    @Test
    public void testFilteredWKBExportWithPropFilter() throws Exception {
        String propertyFilter = "p.description=Point";

        Export.Filters filters = new Export.Filters()
                .withPropertyFilter(propertyFilter);
        Export.ExportTarget exportTarget = new Export.ExportTarget().withType(Export.ExportTarget.Type.DOWNLOAD);

        Export job = buildTestJob(filters, exportTarget, Job.CSVFormat.JSON_WKB);
        List<URL> urls = performExport(job, testSpaceId2, Job.Status.finalized, Job.Status.failed);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("onPropertyLevel");
            add("01010000A0E610000037E38FFD875721402A55A2EC2D0D49400000000000000000");
        }};

        downloadAndCheck(urls, 983, 3, mustContains );
    }

    @Test
    public void testFilteredWKBExportWithPropAndSpatialFilter() throws Exception {
        Export.SpatialFilter spatialFilter = new Export.SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(false)
                .withRadius(5500);
        String propertyFilter = "p.description=Point";

        Export.Filters filters = new Export.Filters()
                .withSpatialFilter(spatialFilter)
                .withPropertyFilter(propertyFilter);
        Export.ExportTarget exportTarget = new Export.ExportTarget().withType(Export.ExportTarget.Type.DOWNLOAD);

        Export job = buildTestJob(filters, exportTarget, Job.CSVFormat.JSON_WKB);
        List<URL> urls = performExport(job, testSpaceId2, Job.Status.finalized, Job.Status.failed);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("Point");
            add("01010000A0E610000037E38FFD875721402A55A2EC2D0D49400000000000000000");
        }};

        downloadAndCheck(urls, 359, 1, mustContains );
    }

    /**
     *  EXPORT full space
     *  TYPE: "DOWNLOAD"
     *  CSVFormat: "GEOJSON"
     * */
    @Test
    public void testFullGEOJSONExport() throws Exception {
        /** Create job */
        Export job = buildTestJob(null, new Export.ExportTarget().withType(Export.ExportTarget.Type.DOWNLOAD), Job.CSVFormat.GEOJSON);
        List<URL> urls = performExport(job, testSpaceId2, Job.Status.finalized, Job.Status.failed);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("MultiPolygon");
            add("MultiLineString");
            add("LineString");
            add("Point");
            add("Polygon with hole");
        }};

        downloadAndCheck(urls, 4425, 10, mustContains );
    }

    /**
     *  EXPORT (full+filtered) composite L1 & L2 space
     *  TYPE: "DOWNLOAD"
     *  CSVFormat: "GEOJSON"
     * */
    @Test
    public void testFullGEOJSONCompositeL1Export() throws Exception {
        /** Create job */
        Export job = buildTestJob(null, new Export.ExportTarget().withType(Export.ExportTarget.Type.DOWNLOAD), Job.CSVFormat.GEOJSON);
        List<URL> urls = performExport(job, testSpaceId2Ext, Job.Status.finalized, Job.Status.failed);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("MultiPolygon");
            add("MultiLineString");
            add("LineString");
            add("Point");
            add("Polygon with hole");
            add("Q601162"); //in delta of composite
            add("foo_new"); //Got overridden in composite space
        }};

        // 252 + 11 = 263
        downloadAndCheck(urls, 114630, 262, mustContains );
    }

    @Test
    public void testFilteredGEOJSONCompositeL1ExportWithSpatialFilter() throws Exception {
        Export.SpatialFilter spatialFilter = new Export.SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(true)
                .withRadius(550000);
        Export.Filters filters = new Export.Filters().withSpatialFilter(spatialFilter);

        Export job = buildTestJob(filters, new Export.ExportTarget().withType(Export.ExportTarget.Type.DOWNLOAD), Job.CSVFormat.GEOJSON);
        List<URL> urls = performExport(job, testSpaceId2Ext, Job.Status.finalized, Job.Status.failed);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("MultiPolygon");
            add("Q193431"); //in delta of composite
            add("foo_new"); //Got overridden in composite space
        }};

        downloadAndCheck(urls, 12972, 30, mustContains );
    }

    @Test
    public void testFilteredGEOJSONCompositeL1ExportWithPropAndSpatialFilter() throws Exception {
        Export.SpatialFilter spatialFilter = new Export.SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(true)
                .withRadius(550000);
        String propertyFilter = "p.foo_new=test";

        Export.Filters filters = new Export.Filters()
                .withSpatialFilter(spatialFilter)
                .withPropertyFilter(propertyFilter);

        Export job = buildTestJob(filters, new Export.ExportTarget().withType(Export.ExportTarget.Type.DOWNLOAD), Job.CSVFormat.GEOJSON);
        List<URL> urls= performExport(job, testSpaceId2Ext, Job.Status.finalized, Job.Status.failed);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("x-psql-job-test2-ext");
            add("foo_new"); //Got overridden in composite space
        }};

        downloadAndCheck(urls, 350, 1, mustContains );
    }

    @Test
    public void testFilteredGEOJSONCompositeL2WithPropAndSpatialFilter() throws Exception {
        Export.SpatialFilter spatialFilter = new Export.SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(true)
                .withRadius(550000);

        Export.Filters filters = new Export.Filters()
                .withSpatialFilter(spatialFilter);

        /** Create job */
        Export job = buildTestJob(filters, new Export.ExportTarget().withType(Export.ExportTarget.Type.DOWNLOAD), Job.CSVFormat.GEOJSON);
        List<URL> urls = performExport(job, testSpaceId2ExtExt, Job.Status.finalized, Job.Status.failed);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("MultiPolygon");
            add("Q193431"); //in delta of composite
            add("foo_new"); //Got overridden in composite space
            add("2LPoint");  //Got added in composite space L2
        }};

        downloadAndCheck(urls, 13276, 31, mustContains );
    }

    @Test
    public void testFilteredGEOJSONCompositeL2Export() throws Exception {
        /** Create job */
        Export job = buildTestJob(null, new Export.ExportTarget().withType(Export.ExportTarget.Type.DOWNLOAD), Job.CSVFormat.GEOJSON);
        List<URL> urls = performExport(job, testSpaceId2ExtExt, Job.Status.finalized, Job.Status.failed);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("MultiPolygon");
            add("MultiLineString");
            add("LineString");
            add("Point");
            add("Polygon with hole");
            add("Q601162"); //in delta of composite L1
            add("foo_polygon"); //Got overridden in composite space L1
            add("2LPoint");  //Got added in composite space L2
        }};
        // 252 + 11 + 1 = 263
        downloadAndCheck(urls, 114934, 263, mustContains );
    }

    /**
     *  EXPORT full space
     *  TYPE: "VML"
     *  CSVFormat: "TILEID_FC_B64"
     * */
    @Test
    public void testFullVMLExport() throws Exception {
        /** Create job */
        int targetLevel = 4;
        int maxTilesPerFile= 20;
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId2+":dummy");

        Export job =  buildVMTestJob(null, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        /** VML Trigger cant get tested here - so its okay that the job will fail in Triggerstate */
        List<URL> urls = performExport(job, testSpaceId2, Job.Status.failed, Job.Status.finalized);

        ArrayList<String> csvMustContains = new ArrayList<String>(){{
            add("360");
            add("eyJ0eXBlIjogIkZl");
        }};

        /** Expect 10 features instead of 11 because feature without geometry is not included in result */
        downloadAndCheckFC(urls, 5429, 10, csvMustContains, 1);
    }

    /**
     *  EXPORT full space
     *  TYPE: "VML"
     *  CSVFormat: "TILEID_FC_B64"
     * */
    @Test
    public void testFullVMLExportMaxTilesPerFile2() throws Exception {
        /** Create job */
        int targetLevel = 12;
        int maxTilesPerFile= 2;
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId2+":dummy");

        Export job =  buildVMTestJob(null, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        /** VML Trigger cant get tested here - so its okay that the job will fail in Triggerstate */
        List<URL> urls = performExport(job, testSpaceId2, Job.Status.failed, Job.Status.finalized);

        ArrayList<String> csvMustContains = new ArrayList<String>(){{
            add("23600780");
            add("23600794");
            add("W9uIiwgImZlYXR1cmV");
        }};

        /** Expect 39 features because the geometry of some features is intersecting multiple tiles */
        downloadAndCheckFC(urls, 22034, 39, csvMustContains, 17);
    }

    /**
     *  EXPORT filtered space
     *  TYPE: "VML"
     *  CSVFormat: "TILEID_FC_B64"
     * */
    @Test
    public void testFilteredVMLExportWithSpatialFilterClipped() throws Exception {
        /** Create job */
        int targetLevel = 4;
        int maxTilesPerFile= 20;
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId2+":dummy");


        Export.SpatialFilter spatialFilter = new Export.SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(true)
                .withRadius(5500);

        Export.Filters filters = new Export.Filters().withSpatialFilter(spatialFilter);
        Export job =  buildVMTestJob(filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, testSpaceId2, Job.Status.failed, Job.Status.finalized);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("360");
            add("eyJ0eXBlIjogIkZlYXR1cmVDb2xsZWN0aW9uIiwg");
        }};

        downloadAndCheckFC(urls, 4033, 5, mustContains, 1);
    }

    @Test
    public void testFilteredVMLExportWithSpatialFilterUnclipped() throws Exception {
        int targetLevel = 4;
        int maxTilesPerFile= 20;
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId2+":dummy");


        Export.SpatialFilter spatialFilter = new Export.SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(false)
                .withRadius(5500);

        Export.Filters filters = new Export.Filters().withSpatialFilter(spatialFilter);
        Export job =  buildVMTestJob(filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, testSpaceId2, Job.Status.failed, Job.Status.finalized);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("360");
            add("VzIjpbeyJpZC");
        }};

        downloadAndCheckFC(urls, 2801, 5, mustContains, 1);
    }

    @Test
    public void testFilteredVMLExportWithPropFilter() throws Exception {
        int targetLevel = 4;
        int maxTilesPerFile= 20;
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId2+":dummy");

        String propertyFilter = "p.description=Point";
        Export.Filters filters = new Export.Filters()
                .withPropertyFilter(propertyFilter);

        Export job =  buildVMTestJob(filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, testSpaceId2, Job.Status.failed, Job.Status.finalized);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("360");
            add("Onh5eiI6IHsidGFncyI6IF");
        }};

        downloadAndCheckFC(urls, 1269, 3, mustContains, 1);
    }

    @Test
    public void testFilteredVMLExportWithPropAndSpatialFilter() throws Exception {
        int targetLevel = 4;
        int maxTilesPerFile= 20;
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId2+":dummy");

        Export.SpatialFilter spatialFilter = new Export.SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(false)
                .withRadius(5500);
        String propertyFilter = "p.description=Point";

        Export.Filters filters = new Export.Filters()
                .withSpatialFilter(spatialFilter)
                .withPropertyFilter(propertyFilter);

        Export job =  buildVMTestJob(filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, testSpaceId2, Job.Status.failed, Job.Status.finalized);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("360");
            add("aW9uIiwgImZlYXR");
        }};

        downloadAndCheckFC(urls, 501, 1, mustContains, 1 );
    }

    /**
     *  EXPORT (full+filtered) composite L1 & L2 space
     *  TYPE: "VML"
     *  CSVFormat: "TILEID_FC_B64"
     * */
    @Test
    public void testFullVMLCompositeL1Export() throws Exception {
        int targetLevel = 4;
        int maxTilesPerFile= 300;
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId2Ext+":dummy");

        /** Create job */
        Export job =  buildVMTestJob(null, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, testSpaceId2Ext, Job.Status.failed, Job.Status.finalized);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("280");
            add("306");
            add("IkZlYXR1cmU");
        }};

        // 252 + 10 (-feature w/o geometry) = 262mh o
        downloadAndCheckFC(urls, 137797, 262, mustContains, 37);
    }

    @Test
    public void testFullVMLCompositeL2Export() throws Exception {
        int targetLevel = 4;
        int maxTilesPerFile= 300;
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId2ExtExt+":dummy");

        /** Create job */
        Export job =  buildVMTestJob(null, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, testSpaceId2ExtExt, Job.Status.failed, Job.Status.finalized);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("374");
            add("352");
            add("HVyZSIsICJnZW9");
            add("374");
        }};
        // 252 + 10 (-feature w/o geometry) + 1 = 263
        downloadAndCheckFC(urls, 138153, 263, mustContains, 37);
    }

    @Test
    public void testFilteredVMLCompositeL1ExportWithSpatialFilter() throws Exception {
        int targetLevel = 4;
        int maxTilesPerFile= 300;
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId2Ext+":dummy");

        Export.SpatialFilter spatialFilter = new Export.SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(true)
                .withRadius(550000);
        Export.Filters filters = new Export.Filters().withSpatialFilter(spatialFilter);

        Export job =  buildVMTestJob(filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, testSpaceId2Ext, Job.Status.failed, Job.Status.finalized);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("360");
            add("RyeSI6IHsidHlwZSI6I");
        }};

        downloadAndCheckFC(urls, 15493, 30, mustContains, 1 );
    }

    @Test
    public void testFilteredVMLCompositeL1ExportWithPropAndSpatialFilter() throws Exception {
        int targetLevel = 4;
        int maxTilesPerFile= 300;
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId2Ext+":dummy");

        Export.SpatialFilter spatialFilter = new Export.SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(true)
                .withRadius(550000);
        String propertyFilter = "p.foo_new=test";

        Export.Filters filters = new Export.Filters()
                .withSpatialFilter(spatialFilter)
                .withPropertyFilter(propertyFilter);

        Export job =  buildVMTestJob(filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, testSpaceId2Ext, Job.Status.failed, Job.Status.finalized);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("360");
            add("ZXMiOiBbOC42MTk5NjE1");
        }};

        downloadAndCheckFC(urls, 473, 1, mustContains, 1);
    }

    @Test
    public void testFilteredVMLCompositeL2WithPropAndSpatialFilter() throws Exception {
        int targetLevel = 4;
        int maxTilesPerFile= 300;
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId2ExtExt+":dummy");

        Export.SpatialFilter spatialFilter = new Export.SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(true)
                .withRadius(550000);

        Export.Filters filters = new Export.Filters()
                .withSpatialFilter(spatialFilter);

        /** Create job */
        Export job =  buildVMTestJob(filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, testSpaceId2ExtExt, Job.Status.failed, Job.Status.finalized);

        ArrayList<String> mustContains = new ArrayList<String>(){{
            add("360");
            add("ZXh0IiwgImNyZWF0ZWRBdCI6IDE2O");
        }};

        downloadAndCheckFC(urls, 15849, 31, mustContains, 1);
    }

    /** ------------------- only for local testing with big spaces  -------------------- */
//    @Test
    public void testParallelFullWKBExport() throws Exception {
        /** Create job */
        String spaceId = "test-space";
        deleteAllJobsOnSpace(spaceId);

        Export job = buildTestJob(null, new Export.ExportTarget().withType(Export.ExportTarget.Type.DOWNLOAD), Job.CSVFormat.JSON_WKB);
        List<URL> urls =  performExport(job, spaceId, Job.Status.finalized, Job.Status.failed);
        System.out.println(urls.size());
    }

//    @Test
    public void testParallelFullVMLExport() throws Exception {
        /** Create job */
        String spaceId = "test-space";
        deleteAllJobsOnSpace(spaceId);

        int targetLevel = 8;
        int maxTilesPerFile= 20000;

        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(spaceId+":dummy");

        deleteAllJobsOnSpace(spaceId);
        Export job =  buildVMTestJob(null, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);

        List<URL> urls =  performExport(job, spaceId, Job.Status.failed, Job.Status.finalized);
        System.out.println(urls.size());
    }

    /** ------------------- HELPER -------------------- */
    public List<URL> performExport(Export job, String spaceId, Job.Status expectedStatus, Job.Status failStatus) throws Exception {
        /** Create job */
        postJob(job, spaceId)
                .body("status", equalTo(Job.Status.waiting.toString()))
                .statusCode(CREATED.code());

        /** start import */
        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .post("/spaces/" + spaceId + "/job/"+job.getId()+"/execute?command=start")
                .then()
                .statusCode(NO_CONTENT.code());

        /** Poll status */
        pollStatus(spaceId, job.getId(), expectedStatus, failStatus);

        job = (Export) getJob(spaceId, job.getId());

        ArrayList<URL> urlList = new ArrayList<>();
        if(job.getExportObjects() != null) {
            for (String key : job.getExportObjects().keySet()) {
                urlList.add(job.getExportObjects().get(key).getDownloadUrl());
            }
        }
        return urlList;
    }

    public static String downloadAndCheck(List<URL> urls, Integer expectedByteSize, Integer expectedFeatureCount, List<String> csvMustContains) throws IOException {
        String result = "";
        long totalByteSize = 0;

        for (URL url : urls) {
            System.out.println("Download: "+url);
            url = new URL(url.toString().replace("localstack","localhost"));
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

        for (String word : csvMustContains) {
            assertNotEquals(-1, result.indexOf(word));
        }

        return result;
    }

    public static void downloadAndCheckFC(List<URL> urls, int expectedByteSize, int expectedFeatureCount, List<String> csvMustContains, Integer expectedTileCount) throws IOException {
        List<String> tileIds = new ArrayList<>();
        int lineCount = 0;
        int featureCount = 0;

        String result = downloadAndCheck(urls, expectedByteSize, 0, csvMustContains);
        for (String fc64: result.split("\n")) {
            tileIds.add(fc64.substring(0,fc64.lastIndexOf("\t")));
            FeatureCollection fc = XyzSerializable.deserialize(new String(Base64.getDecoder().decode((fc64.substring(fc64.lastIndexOf("\t")+1)))));
            featureCount += fc.getFeatures().size();
            lineCount++;
        }

        if(expectedTileCount != null)
            assertEquals(expectedTileCount.intValue(), tileIds.size());

        assertEquals(expectedFeatureCount, featureCount);
    }

    private Export buildTestJob(Export.Filters filters, Export.ExportTarget target, Job.CSVFormat format){
        return new Export()
                .withId(testExportJobId)
                .withFilters(filters)
                .withExportTarget(target)
                .withCsvFormat(format);
    }

    private Export buildVMTestJob(Export.Filters filters, Export.ExportTarget target, Job.CSVFormat format, int targetLevel, int maxTilesPerFile){
        return buildTestJob(filters, target, format)
                .withMaxTilesPerFile(maxTilesPerFile)
                .withTargetLevel(targetLevel);
    }
}
