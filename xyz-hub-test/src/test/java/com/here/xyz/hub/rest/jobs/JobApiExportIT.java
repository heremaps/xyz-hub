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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static com.here.xyz.httpconnector.util.jobs.Export.ExportTarget.Type.DOWNLOAD;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.GEOJSON;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.failed;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.finalized;

import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.jobs.datasets.filters.Filters;
import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Point;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;

@Ignore("Obsolete / Deprecated")
@Disabled("Obsolete / Deprecated")
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

        downloadAndCheck(urls, 5060, 11, mustContain);
    }

    /**
     *  EXPORT filtered space
     *  TYPE: "DOWNLOAD"
     *  CSVFormat: "WKB"
     * */
    @Test
    public void testFilteredWKBExportWithSpatialFilterClipped() throws Exception {
        SpatialFilter spatialFilter = new SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(true)
                .withRadius(5500);

        Filters filters = new Filters().withSpatialFilter(spatialFilter);
        Export.ExportTarget exportTarget = new Export.ExportTarget().withType(DOWNLOAD);

        Export job = buildTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.JSON_WKB);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2, scope), finalized, failed);

        List<String> mustContain = Arrays.asList(
            "foo_polygon",
            "01030000A0E610000001000000210000003629DC5D2D7E214081BD57E7F70B494000000000000000001D34C35FF67B21409A6F8E84C70A4940000000000000000097B11D6359782140891DB6C2AE094940000000000000000092ED19157A73214078E01068B80849400000000000000000C0765170886D2140E61A8FE7ED074940000000000000000027D756E4BE662140EB0CDF04570749400000000000000000104DEA15605F2140211A8288F906494000000000000000006EF4B557B4572140FC385807D90649400000000000000000680EA4E60650214088BD50C0F6064940000000000000000077866711A3482140AD1FD58F5107494000000000000000005947FA59D14121408A151CFBE50749400000000000000000EC804EB5D43B2140B982B751AE08494000000000000000002ABBAAFDE7362140713C5DE5A20949400000000000000000FF76D9AF3B332140DC1DA554BA0A49400000000000000000121CB00EF4302140E22063E6E90B4940000000000000000098ADABB8273021408156C3F2250D4940000000000000000094F06BC1DE30214003A18A55620E49400000000000000000977DCB5A1233214085F834E5920F494000000000000000009CF1E80CAD3621403D6215EBAB104940000000000000000042EDD8848B3B2140595D9896A21149400000000000000000B46B8CE77D412140C24063686D12494000000000000000009F3D97A949482140E07888900413494000000000000000004350FBCEAB4F21403954DF3B6213494000000000000000002548DF835B572140580651CE821349400000000000000000707EFDEF0C5F214073FC6906651349400000000000000000EE3A9F2A74662140B390040A0A134940000000000000000052A23E2C486D2140BEFCCA5A7512494000000000000000001E31FEA64573214084D151B3AC114940000000000000000040EFB6A03178214066804DCEB7104940000000000000000054E7CDB8DB7B214067EEA519A00F4940000000000000000079D3A902207E2140D202EE58700E49400000000000000000B3A2FE61E87E2140AA82303B340D494000000000000000003629DC5D2D7E214081BD57E7F70B49400000000000000000"
        );

        downloadAndCheck(urls, 3907, 5, mustContain);
    }

    @Test
    public void testFilteredWKBExportWithSpatialFilterUnclipped() throws Exception {
        SpatialFilter spatialFilter = new SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(false)
                .withRadius(5500);

        Filters filters = new Filters().withSpatialFilter(spatialFilter);
        Export.ExportTarget exportTarget = new Export.ExportTarget().withType(DOWNLOAD);

        Export job = buildTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.JSON_WKB);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2, scope), finalized, failed);

        List<String> mustContain = Arrays.asList(
            "foo_polygon",
            "01020000A0E610000004000000901FE0FF7D70214087A4164A260B4940000000000000000041237DFF8B4421402A7288B8390549400000000000000000407E80FFF70521400264E8D8410749400000000000000000F60E12FD3504214019A9F7544E0349400000000000000000"
        );

        downloadAndCheck(urls, 2467, 5, mustContain);
    }

    @Test
    public void testFilteredWKBExportWithPropFilter() throws Exception {
        String propertyFilter = "p.description=Point";

        Filters filters = new Filters()
                .withPropertyFilter(propertyFilter);
        Export.ExportTarget exportTarget = new Export.ExportTarget().withType(DOWNLOAD);

        Export job = buildTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.JSON_WKB);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2, scope), finalized, failed);

        List<String> mustContain = Arrays.asList(
            "onPropertyLevel",
            "01010000A0E610000037E38FFD875721402A55A2EC2D0D49400000000000000000"
        );

        downloadAndCheck(urls, 890, 3, mustContain);
    }

    @Test
    public void testFilteredWKBExportWithPropAndSpatialFilter() throws Exception {
        SpatialFilter spatialFilter = new SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(false)
                .withRadius(5500);
        String propertyFilter = "p.description=Point";

        Filters filters = new Filters()
                .withSpatialFilter(spatialFilter)
                .withPropertyFilter(propertyFilter);
        Export.ExportTarget exportTarget = new Export.ExportTarget().withType(DOWNLOAD);

        Export job = buildTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.JSON_WKB);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2, scope), finalized, failed);

        List<String> mustContain = Arrays.asList(
            "Point",
            "01010000A0E610000037E38FFD875721402A55A2EC2D0D49400000000000000000"
        );

        downloadAndCheck(urls, 328, 1, mustContain);
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

        downloadAndCheck(urls, 3973, 11, mustContain);
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
        downloadAndCheck(urls, 96744, 263, mustContain);
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
        downloadAndCheck(urls, 3973, 11, mustContain);
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
        downloadAndCheck(urls, 93141, 253, mustContain);
    }

    @Test
    public void testFilteredGEOJSONCompositeL1ExportWithSpatialFilter() throws Exception {
        SpatialFilter spatialFilter = new SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(true)
                .withRadius(550000);
        Filters filters = new Filters().withSpatialFilter(spatialFilter);

        Export job = buildTestJob(testExportJobId, filters, new Export.ExportTarget().withType(DOWNLOAD), GEOJSON);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2Ext, scope), finalized, failed);

        List<String> mustContain = Arrays.asList(
            "MultiPolygon",
            "Q193431", //in delta of composite
            "foo_new" //Got overridden in composite space
        );

        downloadAndCheck(urls, 10916, 30, mustContain);
    }

    @Test
    public void testFilteredGEOJSONCompositeL1ExportWithPropAndSpatialFilter() throws Exception {
        SpatialFilter spatialFilter = new SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(true)
                .withRadius(550000);
        String propertyFilter = "p.foo_new=test";

        Filters filters = new Filters()
                .withSpatialFilter(spatialFilter)
                .withPropertyFilter(propertyFilter);

        Export job = buildTestJob(testExportJobId, filters, new Export.ExportTarget().withType(DOWNLOAD), GEOJSON);
        List<URL> urls= performExport(job, getScopedSpaceId(testSpaceId2Ext, scope), finalized, failed);

        List<String> mustContain = Arrays.asList(
            "foo_new" //Got overridden in composite space
        );

        downloadAndCheck(urls, 277, 1, mustContain);
    }

    @Test
    public void testFilteredGEOJSONCompositeL2WithPropAndSpatialFilter() throws Exception {
        SpatialFilter spatialFilter = new SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(true)
                .withRadius(550000);

        Filters filters = new Filters()
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

        downloadAndCheck(urls, 11149, 31, mustContain);
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
        downloadAndCheck(urls, 96977, 264, mustContain);
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
        downloadAndCheck(urls, 96744, 263, mustContain);
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
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2, scope), finalized, failed);

        List<String> mustContain = Arrays.asList("360", "eyJ0eXBlIjogIkZl");

        /** Expect 10 features instead of 11 because feature without geometry is not included in result */
        downloadAndCheckFC(urls, 5073, 10, mustContain, 1);
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
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2, scope), finalized, failed);

        List<String> mustContain = Arrays.asList("23600780", "23600794", "W9uIiwgImZlYXR1cmV");

        /** Expect 39 features because the geometry of some features is intersecting multiple tiles */
        downloadAndCheckFC(urls, 23098, 39, mustContain, 17);
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


        SpatialFilter spatialFilter = new SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(true)
                .withRadius(5500);

        Filters filters = new Filters().withSpatialFilter(spatialFilter);
        Export job =  buildVMTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2,scope), finalized, failed);

        List<String> mustContain = Arrays.asList("360", "eyJ0eXBlIjogIkZlYXR1cmVDb2xsZWN0aW9uIiwg");

        downloadAndCheckFC(urls, 3853, 5, mustContain, 1);
    }

    @Test
    public void testFilteredVMLExportWithSpatialFilterUnclipped() throws Exception {
        int targetLevel = 4;
        int maxTilesPerFile= 20;
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId2+":dummy");


        SpatialFilter spatialFilter = new SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(false)
                .withRadius(5500);

        Filters filters = new Filters().withSpatialFilter(spatialFilter);
        Export job =  buildVMTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2, scope), finalized, failed);

        List<String> mustContain = Arrays.asList("360", "VzIjpbeyJpZC");

        downloadAndCheckFC(urls, 2621, 5, mustContain, 1);
    }

    @Test
    public void testFilteredVMLExportWithPropFilter() throws Exception {
        int targetLevel = 4;
        int maxTilesPerFile= 20;
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId2+":dummy");

        String propertyFilter = "p.description=Point";
        Filters filters = new Filters()
                .withPropertyFilter(propertyFilter);

        Export job =  buildVMTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2, scope), finalized, failed);

        List<String> mustContain = Arrays.asList("360");

        downloadAndCheckFC(urls, 1161, 3, mustContain, 1);
    }

    @Test
    public void testFilteredVMLExportWithPropAndSpatialFilter() throws Exception {
        int targetLevel = 4;
        int maxTilesPerFile= 20;
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId2+":dummy");

        SpatialFilter spatialFilter = new SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(false)
                .withRadius(5500);
        String propertyFilter = "p.description=Point";

        Filters filters = new Filters()
                .withSpatialFilter(spatialFilter)
                .withPropertyFilter(propertyFilter);

        Export job =  buildVMTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2,scope), finalized, failed);

        List<String> mustContain = Arrays.asList("360", "aW9uIiwgImZlYXR");

        downloadAndCheckFC(urls, 465, 1, mustContain, 1 );
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
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2Ext, scope), finalized, failed);

        List<String> mustContain = Arrays.asList("280", "306", "IkZlYXR1cmU");

        // 252 + 10 (-feature w/o geometry) = 262mh o
        downloadAndCheckFC(urls, 131057, 262, mustContain, 37);
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
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2ExtExt, scope), finalized, failed);

        List<String> mustContain = Arrays.asList("374", "352", "HVyZSIsICJnZW9", "374");

        // 252 + 10 (-feature w/o geometry) + 1 = 263
        downloadAndCheckFC(urls, 131369, 263, mustContain, 37);
    }

    @Test
    public void testFilteredVMLCompositeL1ExportWithSpatialFilter() throws Exception {
        int targetLevel = 4;
        int maxTilesPerFile= 300;
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId2Ext+":dummy");

        SpatialFilter spatialFilter = new SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(true)
                .withRadius(550000);
        Filters filters = new Filters().withSpatialFilter(spatialFilter);

        Export job =  buildVMTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2Ext,scope), finalized, failed);

        List<String> mustContain = Arrays.asList("360", "RyeSI6IHsidHlwZSI6I");

        downloadAndCheckFC(urls, 14617, 30, mustContain, 1 );
    }

    @Test
    public void testFilteredVMLCompositeL1ExportWithPropAndSpatialFilter() throws Exception {
        int targetLevel = 4;
        int maxTilesPerFile= 300;
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId2Ext+":dummy");

        SpatialFilter spatialFilter = new SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(true)
                .withRadius(550000);
        String propertyFilter = "p.foo_new=test";

        Filters filters = new Filters()
                .withSpatialFilter(spatialFilter)
                .withPropertyFilter(propertyFilter);

        Export job =  buildVMTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2Ext, scope), finalized, failed);

        List<String> mustContain = Arrays.asList("360", "ZXMiOiBbOC42MTk5NjE1");

        downloadAndCheckFC(urls, 433, 1, mustContain, 1);
    }

    @Test
    public void testFilteredVMLCompositeL2WithPropAndSpatialFilter() throws Exception {
        int targetLevel = 4;
        int maxTilesPerFile= 300;
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(getScopedSpaceId(testSpaceId2ExtExt, scope)+":dummy");

        SpatialFilter spatialFilter = new SpatialFilter()
                .withGeometry(new Point().withCoordinates(new PointCoordinates(8.6709594, 50.102964)))
                .withClipped(true)
                .withRadius(550000);

        Filters filters = new Filters()
                .withSpatialFilter(spatialFilter);

        /** Create job */
        Export job =  buildVMTestJob(testExportJobId, filters, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId2ExtExt, scope), finalized, failed);

        List<String> mustContain = Arrays.asList(
                "360",
                "2ludCIsICJjb29yZGluYXRlcyI6IFs4LjQ4M"
        );

        downloadAndCheckFC(urls, 14929, 31, mustContain, 1);
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

        downloadAndCheckFC(urls, 147345, 263, mustContain, 263);
    }

    @Test
    public void testFullVMLCompositeL1ExportByPropertyChanges() throws Exception {
// export by propertie.group only changes
        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId3Ext+":dummy");

        /** Create job */
        Export job =  buildTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.PARTITIONID_FC_B64).withPartitionKey("p.group");
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId3Ext, scope), failed, finalized, Export.CompositeMode.CHANGES );

        List<String> mustContain = Arrays.asList("deltaonly","movedFromEmpty","shouldBeEmpty","deletedInDelta");

        downloadAndCheckFC(urls, 802, 2, mustContain, 4);
    }

    @Test
    public void testFullVMLCompositeL1ExportByTileChanges() throws Exception {
// export by tiles only changes
        int targetLevel = 12;
        int maxTilesPerFile= 300;

        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId3Ext+":dummy");

        /** Create job */
        Export job =  buildVMTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.TILEID_FC_B64, targetLevel, maxTilesPerFile);

        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId3Ext, scope), finalized, failed,  Export.CompositeMode.CHANGES );

        List<String> mustContain = Arrays.asList("23600771","23600774","23600775", "xsZWN0aW9u","xsZWN0aW");

        downloadAndCheckFC(urls, 1098, 3, mustContain, 3);
    }

    @Test
    public void testFullVMLCompositeL1ExportJsonWkbChanges() throws Exception {
// export json_wkb only changes
        Export.ExportTarget exportTarget = new Export.ExportTarget().withType(DOWNLOAD);

        /** Create job */
        Export job =  buildTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.JSON_WKB);
        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId3Ext, scope), finalized, failed, Export.CompositeMode.CHANGES );

        List<String> mustContain = Arrays.asList("id000", "id002", "id003", "movedFromEmpty", "deltaonly", "'\"deleted'\": true");

        downloadAndCheck(urls, 700, 3, mustContain);
    }

    @Test
    public void testFullVMLExportByTilesAsPartJsonWkb() throws Exception {
// export by tiles only changes
        int targetLevel = 12;
        int maxTilesPerFile= 300;

        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId3+":dummy");

        /** Create job */
        Export job =  buildVMTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.PARTITIONED_JSON_WKB, targetLevel, maxTilesPerFile);

        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId3, scope), finalized, failed,  Export.CompositeMode.DEACTIVATED );

        List<String> mustContain = Arrays.asList("23600771,","23600774,", "baseonly","shouldBeEmpty");

        downloadAndCheck(urls, 803, 3, mustContain);
    }

    @Test
    public void testFullVMLCompositeL1ExportByTilesAsPartJsonWkbChanges() throws Exception {
// export by tiles only changes
        int targetLevel = 12;
        int maxTilesPerFile= 300;

        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId3Ext+":dummy");

        /** Create job */
        Export job =  buildVMTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.PARTITIONED_JSON_WKB, targetLevel, maxTilesPerFile);

        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId3Ext, scope), finalized, failed,  Export.CompositeMode.CHANGES );

        List<String> mustContain = Arrays.asList("23600771,,","23600774","23600775", "deltaonly","baseonly","movedFromEmpty");

        downloadAndCheck(urls, 810, 3, mustContain);
    }

    @Test
    public void testFullVMLExportByIdAsPartJsonWkb() throws Exception {

        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId3+":dummy");

        /** Create job */
        Export job = buildTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.PARTITIONED_JSON_WKB).withPartitionKey("id");
        job.addParam("skipTrigger", true);

        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId3, scope), finalized, failed,  Export.CompositeMode.DEACTIVATED );

        List<String> mustContain = Arrays.asList("id000,","id001,","id003,", "baseonly","shouldBeEmpty","deletedInDelta");

        downloadAndCheck(urls, 794, 3, mustContain);
    }

    @Test
    public void testFullVMLCompositeL1ExportByIdAsPartJsonWkbChanges() throws Exception {

        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId3Ext+":dummy");

        /** Create job */
        Export job = buildTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.PARTITIONED_JSON_WKB).withPartitionKey("id");
        job.addParam("skipTrigger", true);

        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId3Ext, scope), finalized, failed,  Export.CompositeMode.CHANGES );

        List<String> mustContain = Arrays.asList("id000,","id002,","id003,,", "deltaonly","movedFromEmpty");

        downloadAndCheck(urls, 537, 2, mustContain);
    }

    @Test
    public void testFullVMLExportByPropertyAsPartJsonWkb() throws Exception {

        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId3+":dummy");

        /** Create job */
        Export job = buildTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.PARTITIONED_JSON_WKB).withPartitionKey("p.group");
        job.addParam("skipTrigger", true);

        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId3, scope), finalized, failed,  Export.CompositeMode.DEACTIVATED );

        List<String> mustContain = Arrays.asList("baseonly,","deletedInDelta,","shouldBeEmpty,","id001","id003","id000", "D214074F04C68920", "B5138214074F04C6892");

        downloadAndCheck(urls, 814, 3, mustContain);
    }

    @Test
    public void testFullVMLCompositeL1ExportByPropertyAsPartJsonWkbChanges() throws Exception {

        Export.ExportTarget exportTarget = new Export.ExportTarget()
                .withType(Export.ExportTarget.Type.VML)
                .withTargetId(testSpaceId3Ext+":dummy");

        /** Create job */
        Export job = buildTestJob(testExportJobId, null, exportTarget, Job.CSVFormat.PARTITIONED_JSON_WKB).withPartitionKey("p.group");
        job.addParam("skipTrigger", true);

        List<URL> urls = performExport(job, getScopedSpaceId(testSpaceId3Ext, scope), finalized, failed,  Export.CompositeMode.CHANGES );

        List<String> mustContain = Arrays.asList("deletedInDelta,,","deltaonly,","movedFromEmpty,", "deltaonly","shouldBeEmpty,,");

        downloadAndCheck(urls, 575, 2, mustContain);
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
