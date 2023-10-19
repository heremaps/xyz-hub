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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static com.here.xyz.httpconnector.util.jobs.Export.ExportTarget.Type.DOWNLOAD;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.GEOJSON;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.failed;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.finalized;

import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.hub.rest.ApiParam.Query.Incremental;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Point;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class JobApiExportIT extends JobApiIT {

    protected String testExportJobId = "x-test-export-job";
    protected static String scope = "export";

    @BeforeClass
    public static void init(){
        prepareEnv(scope);
    }

    @AfterClass
    public static void clean(){
        cleanUpEnv(scope);
    }

    /**
     *  EXPORT full space
     *  TYPE: "DOWNLOAD"
     *  CSVFormat: "WKB"
     * */
    @Test
    public void testFullWKBExport() throws Exception {
        /** Create job */
        Export job = buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(DOWNLOAD), Job.CSVFormat.JSON_WKB);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2, scope), finalized, failed);

        List<String> mustContain = Arrays.asList(
            "01020000A0E61000000300000041C17BFDFF662140E08442041C14494000000000000000006BDE27FD732F21406C5CFFAECF0A49400000000000000000A9B7ABFCD7162140A96A82A8FB0E49400000000000000000",
            "01030000A0E6100000010000000500000083E4F8FC8B17214074F04C689202494000000000000000004DF564FED1A3214074F04C689202494000000000000000004DF564FED1A32140876D8B321B184940000000000000000083E4F8FC8B172140876D8B321B184940000000000000000083E4F8FC8B17214074F04C68920249400000000000000000",
            "foo_polygon"
        );

        downloadAndCheck(urls, 5665, 11, mustContain);
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
        Export.ExportTarget exportTarget = new Export.ExportTarget().withType(DOWNLOAD);

        Export job = buildTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.JSON_WKB);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2, scope), finalized, failed);

        List<String> mustContain = Arrays.asList(
            "foo_polygon",
            "01030000A0E61000000100000021000000DA81E05D2D7E21404E355FE7F70B49400000000000000000DE78CF5FF67B2140D4078B84C70A494000000000000000007F932C6359782140671DB2C2AE094940000000000000000058FB14157A73214019160768B8084940000000000000000093A77370886D21405E1C90E7ED0749400000000000000000C6A82FE4BE6621400654E3045707494000000000000000000415FE15605F2140A53D7888F906494000000000000000008B4C9B57B457214055905B07D90649400000000000000000DD61AFE6065021400D6A58C0F6064940000000000000000048B14F11A34821409417DE8F5107494000000000000000005360E959D141214006CD13FBE507494000000000000000002CBA64B5D43B2140D53CB251AE0849400000000000000000513B80FDE7362140F1D664E5A209494000000000000000007591BCAF3B332140B75FA854BA0A49400000000000000000CDA0BB0EF4302140BB2F61E6E90B49400000000000000000A718B2B8273021402E50BFF2250D494000000000000000000E6B89C1DE3021402B058055620E49400000000000000000FB77A35A1233214044E038E5920F494000000000000000000C98F80CAD362140990F1DEBAB1049400000000000000000B81DAE848B3B2140A3089E96A211494000000000000000008C8A7CE77D412140E42A67686D1249400000000000000000BA1D89A94948214051F18090041349400000000000000000682AFCCEAB4F2140F011E23B62134940000000000000000019C8E4835B572140F29357CE821349400000000000000000590827F00C5F2140C5447206651349400000000000000000020F7E2A7466214009DE070A0A134940000000000000000016D4312C486D2140E6D7C65A751249400000000000000000DFBF17A745732140407152B3AC11494000000000000000002B55CEA03178214034B34DCEB71049400000000000000000A010C9B8DB7B2140EA379D19A00F494000000000000000008FD98802207E21407169EE58700E4940000000000000000054590D62E87E21406979373B340D49400000000000000000DA81E05D2D7E21404E355FE7F70B49400000000000000000"
        );

        downloadAndCheck(urls, 4182, 5, mustContain);
    }

    @Test
    public void testFilteredWKBExportWithSpatialFilterUnclipped() throws Exception {
        Export.SpatialFilter spatialFilter = new Export.SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(false)
                .withRadius(5500);

        Export.Filters filters = new Export.Filters().withSpatialFilter(spatialFilter);
        Export.ExportTarget exportTarget = new Export.ExportTarget().withType(DOWNLOAD);

        Export job = buildTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.JSON_WKB);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2, scope), finalized, failed);

        List<String> mustContain = Arrays.asList(
            "foo_polygon",
            "01020000A0E610000004000000901FE0FF7D70214087A4164A260B4940000000000000000041237DFF8B4421402A7288B8390549400000000000000000407E80FFF70521400264E8D8410749400000000000000000F60E12FD3504214019A9F7544E0349400000000000000000"
        );

        downloadAndCheck(urls, 2742, 5, mustContain);
    }

    @Test
    public void testFilteredWKBExportWithPropFilter() throws Exception {
        String propertyFilter = "p.description=Point";

        Export.Filters filters = new Export.Filters()
                .withPropertyFilter(propertyFilter);
        Export.ExportTarget exportTarget = new Export.ExportTarget().withType(DOWNLOAD);

        Export job = buildTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.JSON_WKB);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2, scope), finalized, failed);

        List<String> mustContain = Arrays.asList(
            "onPropertyLevel",
            "01010000A0E610000037E38FFD875721402A55A2EC2D0D49400000000000000000"
        );

        downloadAndCheck(urls, 1055, 3, mustContain);
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
        Export.ExportTarget exportTarget = new Export.ExportTarget().withType(DOWNLOAD);

        Export job = buildTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.JSON_WKB);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2, scope), finalized, failed);

        List<String> mustContain = Arrays.asList(
            "Point",
            "01010000A0E610000037E38FFD875721402A55A2EC2D0D49400000000000000000"
        );

        downloadAndCheck(urls, 383, 1, mustContain);
    }

    /**
     *  EXPORT full space
     *  TYPE: "DOWNLOAD"
     *  CSVFormat: "GEOJSON"
     * */
    @Test
    public void testFullGEOJSONExport() throws Exception {
        /** Create job */
        Export job = buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(DOWNLOAD), GEOJSON);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2, scope), finalized, failed);

        List<String> mustContain = Arrays.asList(
            "MultiPolygon",
            "MultiLineString",
            "LineString",
            "Point",
            "Polygon with hole"
        );

        downloadAndCheck(urls, 4964, 11, mustContain);
    }

    /**
     *  EXPORT (full+filtered) composite L1 space
     *  TYPE: "DOWNLOAD"
     *  CSVFormat: "GEOJSON"
     * */
    @Test
    public void testFullGEOJSONCompositeL1Export() throws Exception {
        /** Create job */
        Export job = buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(DOWNLOAD), GEOJSON);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2Ext, scope), finalized, failed);

        List<String> mustContain = Arrays.asList(
            "MultiPolygon",
            "MultiLineString",
            "LineString",
            "Point",
            "Polygon with hole",
            "Q601162", //in delta of composite
            "foo_new" //Got overridden in composite space
        );

        // 252 + 11 = 263
        downloadAndCheck(urls, 121217, 263, mustContain);
    }

    /**
     *  EXPORT (full+filtered) composite L1 space with context=super
     *  TYPE: "DOWNLOAD"
     *  CSVFormat: "GEOJSON"
     * */
    @Test
    public void testFullGEOJSONCompositeL1SuperExport() throws Exception {
        Export job = buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(DOWNLOAD), GEOJSON);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2Ext, scope), finalized, failed, SUPER);

        List<String> mustContain = Arrays.asList(
            "MultiPolygon",
            "MultiLineString",
            "LineString",
            "Point",
            "Polygon with hole"
        );

        // 252 + 11 = 263
        downloadAndCheck(urls, 4964, 11, mustContain);
    }

    /**
     *  EXPORT (full+filtered) composite L1 space with context=extension
     *  TYPE: "DOWNLOAD"
     *  CSVFormat: "GEOJSON"
     * */
    @Test
    public void testFullGEOJSONCompositeL1ExtensionExport() throws Exception {
        Export job = buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(DOWNLOAD), GEOJSON);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2Ext, scope), finalized, failed, EXTENSION);

        List<String> mustContain = Arrays.asList(
            "Point",
            "Q601162", //in delta of composite
            "foo_new" //Got overridden in composite space
        );

        // 252 + 1 (Edit of Base);
        downloadAndCheck(urls, 116712, 253, mustContain);
    }

    @Test
    public void testFilteredGEOJSONCompositeL1ExportWithSpatialFilter() throws Exception {
        Export.SpatialFilter spatialFilter = new Export.SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(true)
                .withRadius(550000);
        Export.Filters filters = new Export.Filters().withSpatialFilter(spatialFilter);

        Export job = buildTestJob(testExportJobId, filters, new Export.ExportTarget().withType(DOWNLOAD), GEOJSON);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2Ext, scope), finalized, failed);

        List<String> mustContain = Arrays.asList(
            "MultiPolygon",
            "Q193431", //in delta of composite
            "foo_new" //Got overridden in composite space
        );

        downloadAndCheck(urls, 13692, 30, mustContain);
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

        Export job = buildTestJob(testExportJobId, filters, new Export.ExportTarget().withType(DOWNLOAD), GEOJSON);
        List<URL> urls= performExport(job, getScopedSpaceId(testSpaceId2Ext, scope), finalized, failed);

        List<String> mustContain = Arrays.asList(
            getScopedSpaceId(testSpaceId2Ext, scope),
            "foo_new" //Got overridden in composite space
        );

        downloadAndCheck(urls, 374, 1, mustContain);
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
        Export job = buildTestJob(testExportJobId, filters, new Export.ExportTarget().withType(DOWNLOAD), GEOJSON);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2ExtExt, scope), finalized, failed);

        List<String> mustContain = Arrays.asList(
            "MultiPolygon",
            "Q193431", //in delta of composite
            "foo_new", //Got overridden in composite space
            "2LPoint"  //Got added in composite space L2
        );

        downloadAndCheck(urls, 14020, 31, mustContain);
    }

    @Test
    public void testFullGEOJSONCompositeL2Export() throws Exception {
        /** Create job */
        Export job = buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(DOWNLOAD), GEOJSON);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2ExtExt, scope), finalized, failed);

        List<String> mustContain = Arrays.asList(
            "MultiPolygon",
            "MultiLineString",
            "LineString",
            "Point",
            "Polygon with hole",
            "Q601162", //in delta of composite L1
            "foo_polygon", //Got overridden in composite space L1
            "2LPoint"  //Got added in composite space L2
        );
        // 252 + 11 + 1 = 264
        downloadAndCheck(urls, 121545, 264, mustContain);
    }

    @Test
    public void testFullGEOJSONCompositeL2SuperExport() throws Exception {
        Export job = buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(DOWNLOAD), GEOJSON);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2ExtExt, scope), finalized, failed, SUPER);

        List<String> mustContain = Arrays.asList(
            "MultiPolygon",
            "MultiLineString",
            "LineString",
            "Point",
            "Polygon with hole",
            "Q601162", //in delta of composite L1
            "foo_polygon" //Got overridden in composite space L1
        );

        // 252 + 11 = 263
        downloadAndCheck(urls, 121217, 263, mustContain);
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

        Export job =  buildVMTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        /** VML Trigger cant get tested here - so its okay that the job will fail in Triggerstate */
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2, scope), failed, finalized);

        List<String> mustContain = Arrays.asList("360", "eyJ0eXBlIjogIkZl");

        /** Expect 10 features instead of 11 because feature without geometry is not included in result */
        downloadAndCheckFC(urls, 5725, 10, mustContain, 1);
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

        Export job =  buildVMTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        /** VML Trigger cant get tested here - so its okay that the job will fail in Triggerstate */
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2, scope), failed, finalized);

        List<String> mustContain = Arrays.asList("23600780", "23600794", "W9uIiwgImZlYXR1cmV");

        /** Expect 39 features because the geometry of some features is intersecting multiple tiles */
        downloadAndCheckFC(urls, 25638, 39, mustContain, 17);
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
        Export job =  buildVMTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2,scope), failed, finalized);

        List<String> mustContain = Arrays.asList("360", "eyJ0eXBlIjogIkZlYXR1cmVDb2xsZWN0aW9uIiwg");

        downloadAndCheckFC(urls, 4177, 5, mustContain, 1);
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
        Export job =  buildVMTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2, scope), failed, finalized);

        List<String> mustContain = Arrays.asList("360", "VzIjpbeyJpZC");

        downloadAndCheckFC(urls, 2949, 5, mustContain, 1);
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

        Export job =  buildVMTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2, scope), failed, finalized);

        List<String> mustContain = Arrays.asList("360", "Onh5eiI6IHsidGFncyI6IF");

        downloadAndCheckFC(urls, 1357, 3, mustContain, 1);
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

        Export job =  buildVMTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2,scope), failed, finalized);

        List<String> mustContain = Arrays.asList("360", "aW9uIiwgImZlYXR");

        downloadAndCheckFC(urls, 533, 1, mustContain, 1 );
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
        Export job =  buildVMTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2Ext, scope), failed, finalized);

        List<String> mustContain = Arrays.asList("280", "306", "IkZlYXR1cmU");

        // 252 + 10 (-feature w/o geometry) = 262mh o
        downloadAndCheckFC(urls, 145485, 262, mustContain, 37);
    }

    @Test
    public void testFullVMLCompositeL2Export() throws Exception {
        int targetLevel = 4;
        int maxTilesPerFile= 300;
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(getScopedSpaceId(testSpaceId2ExtExt, scope)+":dummy");

        /** Create job */
        Export job =  buildVMTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2ExtExt, scope), failed, finalized);

        List<String> mustContain = Arrays.asList("374", "352", "HVyZSIsICJnZW9", "374");

        // 252 + 10 (-feature w/o geometry) + 1 = 263
        downloadAndCheckFC(urls, 145873, 263, mustContain, 37);
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

        Export job =  buildVMTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2Ext,scope), failed, finalized);

        List<String> mustContain = Arrays.asList("360", "RyeSI6IHsidHlwZSI6I");

        downloadAndCheckFC(urls, 16369, 30, mustContain, 1 );
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

        Export job =  buildVMTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2Ext, scope), failed, finalized);

        List<String> mustContain = Arrays.asList("360", "ZXMiOiBbOC42MTk5NjE1");

        downloadAndCheckFC(urls, 505, 1, mustContain, 1);
    }

    @Test
    public void testFilteredVMLCompositeL2WithPropAndSpatialFilter() throws Exception {
        int targetLevel = 4;
        int maxTilesPerFile= 300;
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(getScopedSpaceId(testSpaceId2ExtExt, scope)+":dummy");

        Export.SpatialFilter spatialFilter = new Export.SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(true)
                .withRadius(550000);

        Export.Filters filters = new Export.Filters()
                .withSpatialFilter(spatialFilter);

        /** Create job */
        Export job =  buildVMTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2ExtExt, scope), failed, finalized);

        List<String> mustContain = Arrays.asList(
            "360",
            "2ludCIsICJjb29yZGluYXRlcyI6IFs4LjQ4M"
        );

        downloadAndCheckFC(urls, 16757, 31, mustContain, 1);
    }

    /**
     *  EXPORT composite L1 & L2 space
     *  TYPE: "VML"
     *  CSVFormat: "PARTITIONID_FC_B64"
     * */
    @Test
    public void testFullVMLCompositeL1ExportByID() throws Exception {
// export by ID
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId2Ext+":dummy");

        /** Create job */
        Export job =  buildTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.PARTITIONID_FC_B64);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2Ext, scope), failed, finalized);

        List<String> mustContain = Arrays.asList("Q3107495", "Q2907951", "foo_polygon", "IkZlYXR1cmV");

        downloadAndCheckFC(urls, 161813, 263, mustContain, 263);
    }

@Test
    public void testFullVMLCompositeL1ExportByPropertyChanges() throws Exception {
// export by propertie.group only changes
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId3Ext+":dummy");

        /** Create job */
        Export job =  buildTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.PARTITIONID_FC_B64).withPartitionKey("p.group");
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId3Ext, scope), failed, finalized, Incremental.CHANGES );

        List<String> mustContain = Arrays.asList("deltaonly","movedFromEmpty","shouldBeEmpty");

        downloadAndCheckFC(urls, 938, 2, mustContain, 3);
    }


    /** ------------------- only for local testing with big spaces  -------------------- */
//    @Test
    public void testParallelFullWKBExport() throws Exception {
        /** Create job */
        String spaceId = "test-space";
        deleteAllJobsOnSpace(spaceId);

        Export job = buildTestJob(testExportJobId, null, new Export.ExportTarget().withType(DOWNLOAD), Job.CSVFormat.JSON_WKB);
        List<URL> urls =  performExport(job, spaceId, finalized, failed);
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
        Export job =  buildVMTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);

        List<URL> urls =  performExport(job, spaceId, failed, finalized);
        System.out.println(urls.size());
    }
}
