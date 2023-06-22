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

package com.here.xyz.httpconnector.config;

import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.httpconnector.rest.HApiParam;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Job.CSVFormat;
import com.here.xyz.models.geojson.coordinates.WKTHelper;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.PSQLXyzConnector;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.config.PSQLConfig;
import com.here.xyz.psql.query.GetFeaturesByGeometry;
import com.here.xyz.psql.query.SearchForFeatures;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.impl.ArrayTuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Client for handle Export-Jobs (RDS -> S3)
 */
public class JDBCExporter extends JDBCClients{
    private static final Logger logger = LogManager.getLogger();

    public static Future<Export.ExportStatistic> executeExport(Export j, String schema, String s3Bucket, String s3Path, String s3Region){
        try{
            String propertyFilter = (j.getFilters() == null ? null : j.getFilters().getPropertyFilter());
            Export.SpatialFilter spatialFilter= (j.getFilters() == null ? null : j.getFilters().getSpatialFilter());
            SQLQuery exportQuery = generateFilteredExportQuery(schema, j.getTargetSpaceId(), propertyFilter, spatialFilter,
                    j.getTargetVersion(), j.getParams(), j.getCsvFormat(), null);

            switch (j.getExportTarget().getType()){
                case DOWNLOAD:
                    return calculateThreadCountForDownload(j, schema, exportQuery)
                            .compose(threads -> {
                                try{
                                    Promise<Export.ExportStatistic> promise = Promise.promise();
                                    List<Future> exportFutures = new ArrayList<>();

                                    for (int i = 0; i < threads; i++) {
                                        String s3Prefix = i + "_";
                                        SQLQuery q2 = buildS3ExportQuery(j, schema, s3Bucket, s3Path, s3Prefix, s3Region,
                                                new SQLQuery("AND i%% " + threads + " = "+i));
                                        exportFutures.add( exportTypeDownload(j.getTargetConnector(), q2, j, s3Path));
                                    }

                                    return executeParallelExportAndCollectStatistics( j, promise, exportFutures);
                                }catch (SQLException e){
                                    logger.warn(e);
                                    return Future.failedFuture(e);
                                }
                            });
                case VML:
                default:
                    return calculateTileListForVMLExport(j, schema, exportQuery)
                            .compose(tileList-> {
                                try{
                                    Promise<Export.ExportStatistic> promise = Promise.promise();
                                    List<Future> exportFutures = new ArrayList<>();
                                    j.setProcessingList(tileList);

                                    for (int i = 0; i < tileList.size() ; i++) {
                                        SQLQuery q2 = buildVMLExportQuery(j, schema, s3Bucket, s3Path, s3Region, null, tileList.get(i));
                                        exportFutures.add(exportTypeVML(j.getTargetConnector(), q2, j, s3Path));
                                    }

                                    return executeParallelExportAndCollectStatistics( j, promise, exportFutures);
                                }catch (SQLException e){
                                    logger.warn(e);
                                    return Future.failedFuture(e);
                                }
                            });
            }
        }catch (SQLException e){
            return Future.failedFuture(e);
        }
    }

    private static Future<Export.ExportStatistic> executeParallelExportAndCollectStatistics(Export j, Promise<Export.ExportStatistic> promise, List<Future> exportFutures) {
        CompositeFuture
                .all(exportFutures)
                .onComplete(
                        t -> {
                            if(t.succeeded()){
                                long overallRowsUploaded = 0;
                                long overallFilesUploaded = 0;
                                long overallBytesUploaded = 0;

                                // Collect all results of future and summarize them into ExportStatistics
                                for (Future fut : exportFutures) {
                                    Export.ExportStatistic eo = (Export.ExportStatistic)fut.result();
                                    overallRowsUploaded += eo.getRowsUploaded();
                                    overallFilesUploaded += eo.getFilesUploaded();
                                    overallBytesUploaded += eo.getBytesUploaded();
                                }

                                promise.complete(new Export.ExportStatistic()
                                        .withBytesUploaded(overallBytesUploaded)
                                        .withFilesUploaded(overallFilesUploaded)
                                        .withRowsUploaded(overallRowsUploaded)
                                );
                            }else{
                                logger.warn("[{}] Export failed {}: {}", j.getId(), j.getTargetSpaceId(), t.cause());
                                promise.fail(t.cause());
                            }
                        });
        return promise.future();
    }

    private static Future<Integer> calculateThreadCountForDownload(Export j, String schema, SQLQuery exportQuery) throws SQLException {
        /**
         * Currently we are ignoring filters and count only all features
         **/
        SQLQuery q = buildS3CalculateQuery(j, schema, exportQuery);
        logger.info("[{}] Calculate S3-Export {}: {}", j.getId(), j.getTargetSpaceId(), q.text());

        return getClient(j.getTargetConnector())
                .preparedQuery(q.text())
                .execute(new ArrayTuple(q.parameters()))
                .map(row -> {
                    Row res = row.iterator().next();
                    if (res != null) {
                        return res.getInteger("thread_cnt");
                    }
                    return null;
                });
    }

    private static Future<List<String>> calculateTileListForVMLExport(Export j, String schema, SQLQuery exportQuery) throws SQLException {
        if(j.getProcessingList() != null)
            return Future.succeededFuture(j.getProcessingList());

        SQLQuery q = buildVMLCalculateQuery(j, schema, exportQuery);
        logger.info("[{}] Calculate VML-Export {}: {}", j.getId(), j.getTargetSpaceId(), q.text());

        return getClient(j.getTargetConnector())
                .preparedQuery(q.text())
                .execute(new ArrayTuple(q.parameters()))
                .map(row -> {
                    Row res = row.iterator().next();
                    if (res != null) {
                      String[] sArr = res.getArrayOfStrings("tilelist");
                      if( sArr != null && sArr.length > 0)
                        return Arrays.stream(sArr).collect(Collectors.toList());
                    }
                    return null;
                });
    }

    private static Future<Export.ExportStatistic> exportTypeDownload(String clientId, SQLQuery q, Export j , String s3Path){
        logger.info("[{}] Execute S3-Export {}->{} {}", j.getId(), j.getTargetSpaceId(), s3Path, q.text());

        return getClient(clientId)
                .preparedQuery(q.text())
                .execute(new ArrayTuple(q.parameters()))
                .map(row -> {
                    Row res = row.iterator().next();
                    if (res != null) {
                        return new Export.ExportStatistic()
                                .withRowsUploaded(res.getLong("rows_uploaded"))
                                .withFilesUploaded(res.getLong("files_uploaded"))
                                .withBytesUploaded(res.getLong("bytes_uploaded"));
                    }
                    return null;
                });
    }

    private static Future<Export.ExportStatistic> exportTypeVML(String clientId, SQLQuery q, Export j, String s3Path){
        logger.info("[{}] Execute VML-Export {}->{} {}", j.getId(), j.getTargetSpaceId(), s3Path, q.text());

        return getClient(clientId)
                .preparedQuery(q.text())
                .execute(new ArrayTuple(q.parameters()))
                .map(rows -> {
                    Export.ExportStatistic es = new Export.ExportStatistic();

                    rows.forEach(
                            row -> {
                                es.addRows(row.getLong("rows_uploaded"));
                                es.addBytes(row.getLong("bytes_uploaded"));
                                es.addFiles(row.getLong("files_uploaded"));
                            }
                    );

                    return es;
                });
    }

    public static SQLQuery buildS3CalculateQuery(Export job, String schema, SQLQuery query) {
        SQLQuery q = new SQLQuery("select ${schema}.exp_type_download_precalc(" +
                "#{estimated_count}, ${{exportSelectString}}, #{tbl}::regclass) as thread_cnt");

        q.setVariable("schema", schema);
        q.setNamedParameter("estimated_count", job.getEstimatedFeatureCount());
        q.setQueryFragment("exportSelectString", query);
        q.setNamedParameter("tbl", schema+".\""+job.getTargetTable()+"\"");

        return q.substituteAndUseDollarSyntax(q);
    }

    public static SQLQuery buildVMLCalculateQuery(Export job, String schema, SQLQuery query) {
        int exportCalcLevel = 12;

        SQLQuery q = new SQLQuery("select tilelist from ${schema}.exp_type_vml_precalc(" +
                "#{htile}, '', #{mlevel}, ${{exportSelectString}}, #{estimated_count}, #{tbl}::regclass)");

        q.setVariable("schema", schema);
        q.setNamedParameter("htile",true);
        q.setNamedParameter("idk","''");
        q.setNamedParameter("mlevel", exportCalcLevel);
        q.setQueryFragment("exportSelectString", query);
        q.setNamedParameter("estimated_count", job.getEstimatedFeatureCount());
        q.setNamedParameter("tbl", schema+".\""+job.getTargetTable()+"\"");

        return q.substituteAndUseDollarSyntax(q);
    }

    public static SQLQuery buildS3ExportQuery(Export j, String schema,
                                              String s3Bucket, String s3Path, String s3FilePrefix, String s3Region,
                                              SQLQuery customWhereCondition) throws SQLException {

        String propertyFilter = (j.getFilters() == null ? null : j.getFilters().getPropertyFilter());
        Export.SpatialFilter spatialFilter= (j.getFilters() == null ? null : j.getFilters().getSpatialFilter());

        s3Path = s3Path+ "/" +(s3FilePrefix == null ? "" : s3FilePrefix)+"export.csv";
        SQLQuery exportSelectString = generateFilteredExportQuery(schema, j.getTargetSpaceId(), propertyFilter, spatialFilter, j.getTargetVersion(), j.getParams(), j.getCsvFormat(), customWhereCondition);

        SQLQuery q = new SQLQuery("SELECT *,'{iml_s3_export_hint}' as iml_s3_export_hint from aws_s3.query_export_to_s3( "+
                " ${{exportSelectString}},"+
                " aws_commons.create_s3_uri(#{s3Bucket}, #{s3Path}, #{s3Region}),"+
                " options := 'format csv,delimiter '','', encoding ''UTF8'', quote  ''\"'', escape '''''''' ' );"
        );

        q.setQueryFragment("exportSelectString", exportSelectString);
        q.setNamedParameter("s3Bucket",s3Bucket);
        q.setNamedParameter("s3Path",s3Path);
        q.setNamedParameter("s3Region",s3Region);

        return q.substituteAndUseDollarSyntax(q);
    }

    public static SQLQuery buildVMLExportQuery(Export j, String schema,
                                               String s3Bucket, String s3Path, String s3Region,
                                               SQLQuery customWhereCondition, String parentQk) throws SQLException {

        String propertyFilter = (j.getFilters() == null ? null : j.getFilters().getPropertyFilter());
        Export.SpatialFilter spatialFilter= (j.getFilters() == null ? null : j.getFilters().getSpatialFilter());

        int maxTilesPerFile = j.getMaxTilesPerFile() == 0 ? 4096 : j.getMaxTilesPerFile();

        SQLQuery exportSelectString =  generateFilteredExportQuery(schema, j.getTargetSpaceId(), propertyFilter, spatialFilter, j.getTargetVersion(), j.getParams(), j.getCsvFormat(), customWhereCondition);

        SQLQuery q = new SQLQuery(
                "select("+
                        " aws_s3.query_export_to_s3( replace( o.s3sql, 'select *', 'select htiles_convert_qk_to_longk(tile_id)::text as tile_id, tile_content' ) , " +
                        "   #{s3Bucket}, " +
                        "   format('%s/%s/%s-%s.csv',#{s3Path}::text, o.qk, o.bucket, o.nrbuckets) ," +
                        "   #{s3Region}," +
                        "   'format csv')).* ," +
                        "  '{iml_vml_export_hint}' as iml_vml_export_hint " +
                        " from" +
                        "    exp_build_sql_inhabited_txt(true, #{parentQK}, #{targetLevel}, ${{exportSelectString}}, #{maxTilesPerFile}::int )o"
        );

        q.setQueryFragment("exportSelectString", exportSelectString);

        q.setNamedParameter("s3Bucket",s3Bucket);
        q.setNamedParameter("s3Path",s3Path);
        q.setNamedParameter("s3Region",s3Region);
        q.setNamedParameter("targetLevel", j.getTargetLevel());
        q.setNamedParameter("parentQK", parentQk);
        q.setNamedParameter("maxTilesPerFile", maxTilesPerFile);

        return q.substituteAndUseDollarSyntax(q);
    }

    private static SQLQuery generateFilteredExportQuery(String schema, String spaceId, String propertyFilter, Export.SpatialFilter spatialFilter,
                                                        String targetVersion, Map params, CSVFormat csvFormat, SQLQuery customWhereCondition) throws SQLException {
        SQLQuery geoFragment;

        if (spatialFilter != null && spatialFilter.isClipped()) {
            geoFragment = new SQLQuery("ST_Intersection(ST_MakeValid(geo), ST_Buffer(ST_GeomFromText(#{wktGeometry})::geography, #{radius})::geometry) as geo");
            geoFragment.setNamedParameter("wktGeometry", WKTHelper.geometryToWKB(spatialFilter.getGeometry()));
            geoFragment.setNamedParameter("radius", spatialFilter.getRadius());
        } else
            geoFragment = new SQLQuery("geo");

        GetFeaturesByGeometryEvent event = new GetFeaturesByGeometryEvent();
        event.setSpace(spaceId);
        event.setParams(params);

        if(params != null && params.get("enableHashedSpaceId") != null)
            event.setConnectorParams(new HashMap<String, Object>(){{put("enableHashedSpaceId",params.get("enableHashedSpaceId"));}});

        if(params != null && params.get("versionsToKeep") != null)
            event.setVersionsToKeep((int)params.get("versionsToKeep"));

        if(targetVersion != null) {
            event.setRef(targetVersion);
        }

        if(propertyFilter != null) {
            PropertiesQuery propertyQueryLists = HApiParam.Query.parsePropertiesQuery(propertyFilter, "", false);
            event.setPropertiesQuery(propertyQueryLists);
        }

        if(spatialFilter != null){
            event.setGeometry(spatialFilter.getGeometry());
            event.setRadius(spatialFilter.getRadius());
            event.setClip(spatialFilter.isClipped());
        }

        DatabaseHandler dbHandler = new PSQLXyzConnector();
        PSQLConfig config = new PSQLConfig(event, schema);
        dbHandler.setConfig(config);

        SQLQuery sqlQuery;

        try {
            if(spatialFilter == null)
                sqlQuery = new SearchForFeatures(event, dbHandler)._buildQuery(event);
            else
                sqlQuery = new GetFeaturesByGeometry(event, dbHandler)._buildQuery(event);
        } catch (Exception e) {
            throw new SQLException(e);
        }

        /** Override geoFragment */
        sqlQuery.setQueryFragment("geo", geoFragment);
        /** Remove Limit */
        sqlQuery.setQueryFragment("limit", "");

        if(customWhereCondition != null)
            sqlQuery.setQueryFragment("customClause", customWhereCondition);

        if (csvFormat.equals(CSVFormat.GEOJSON)) {
            SQLQuery geoJson = new SQLQuery(
                    "select jsonb_set( jsondata,'{geometry}',st_asgeojson(geo,8)::jsonb ) " +
                    "from ( ${{contentQuery}}) X"
            );
            geoJson.setQueryFragment("contentQuery",sqlQuery);
            geoJson.substitute();
            return queryToText(geoJson);
        }else {
            sqlQuery.substitute();
            return queryToText(sqlQuery);
        }
    }

    private static SQLQuery queryToText(SQLQuery q){
        SQLQuery sq = new SQLQuery();
        String s = q.text()
                .replace("?", "%L")
                .replace("'","''");

        String r =  "format('"+s+"'";

        int i = 0;
        for (Object o :q.parameters()) {
            String curVar = "var"+(i++);
            r += ",#{"+curVar+"}";
            sq.setNamedParameter(curVar, o);
        }

        sq.setText(r+")");
        return sq;
    }
}
