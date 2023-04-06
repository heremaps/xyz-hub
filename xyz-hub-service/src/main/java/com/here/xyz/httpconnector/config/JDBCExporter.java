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
import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.impl.ArrayTuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Client for handle Export-Jobs (RDS -> S3)
 */
public class JDBCExporter extends JDBCClients{
    private static final Logger logger = LogManager.getLogger();
    private static final boolean VML_USE_CLIPPING = true;

    public static Future<Export.ExportStatistic> executeExport(Export j, String schema, String s3Bucket, String s3Path, String s3Region){
        return executeExport(j.getTargetConnector(), schema, j.getTargetSpaceId(),
                s3Bucket, s3Path, s3Region,
                j.getCsvFormat(), j.getExportTarget(),
                j.getFilters(), j.getParams(),
                j.getTargetVersion(), j.getTargetLevel(), j.getMaxTilesPerFile());
    }

    public static Future<Export.ExportStatistic> executeExport(String clientID, String schema, String spaceId,
                                                               String s3Bucket, String s3Path, String s3Region,
                                                               CSVFormat csvFormat, Export.ExportTarget target,
                                                               Export.Filters filters, Map params,
                                                               String targetVersion, Integer targetLevel, int maxTilesPerFile){
        SQLQuery q;

        try{
            String propertyFilter = (filters == null ? null : filters.getPropertyFilter());
            Export.SpatialFilter spatialFilter= (filters == null ? null : filters.getSpatialFilter());

            switch (target.getType()){
                case DOWNLOAD:
                    q = buildS3ExportQuery(schema, spaceId, s3Bucket, s3Path, s3Region, csvFormat,
                            propertyFilter, spatialFilter, targetVersion, params);
                    logger.info("Execute S3-Export {}->{} {}", spaceId, s3Path, q.text());

                    return getClient(clientID)
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
                case VML:
                default:
                    q = buildVMLExportQuery(schema, spaceId, s3Bucket, s3Path, s3Region, csvFormat, targetLevel, maxTilesPerFile,
                            propertyFilter, spatialFilter, targetVersion, params);
                    logger.info("Execute VML-Export {}->{} {}", spaceId, s3Path, q.text());

                    return getClient(clientID)
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
        }catch (SQLException e){
            return Future.failedFuture(e);
        }
    }

    public static SQLQuery buildS3ExportQuery(String schema, String spaceId, String s3Bucket, String s3Path, String s3Region, CSVFormat csvFormat,
                                              String propertyFilter, Export.SpatialFilter spatialFilter, String targetVersion, Map params) throws SQLException {
        s3Path = s3Path+"/export.csv";
        SQLQuery exportSelectString = generateFilteredExportQuery(schema, spaceId, propertyFilter, spatialFilter, targetVersion, params, csvFormat);

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

    public static SQLQuery buildVMLExportQuery(String schema, String spaceId, String s3Bucket, String s3Path, String s3Region,  CSVFormat csvFormat,
                                               int targetLevel, int maxTilesPerFile,  String propertyFilter, Export.SpatialFilter spatialFilter,
                                               String targetVersion, Map params) throws SQLException {

        maxTilesPerFile = (maxTilesPerFile == 0 ? 4096 : maxTilesPerFile);

        SQLQuery exportSelectString =  generateFilteredExportQuery(schema, spaceId, propertyFilter, spatialFilter, targetVersion, params, csvFormat);

        SQLQuery q = new SQLQuery(
                "select("+
                        " aws_s3.query_export_to_s3( o.s3sql , " +
                        "   #{s3Bucket}, " +
                        "   format('%s/%s/%s-%s.csv',#{s3Path}, o.qk, o.bucket, o.nrbuckets) ," +
                        "   #{s3Region}," +
                        "   'format csv')).* ," +
                        "  '{iml_vml_export_hint}' as iml_vml_export_hint " +
                        " from" +
                        "    exp_build_sql_inhabited_txt(#{clipped}, '', #{targetLevel}, ${{exportSelectString}}, #{maxTilesPerFile}::int )o"
        );

        q.setQueryFragment("exportSelectString", exportSelectString);

        q.setNamedParameter("s3Bucket",s3Bucket);
        q.setNamedParameter("s3Path",s3Path);
        q.setNamedParameter("s3Region",s3Region);
        q.setNamedParameter("clipped",VML_USE_CLIPPING);
        q.setNamedParameter("targetLevel", targetLevel);
        q.setNamedParameter("maxTilesPerFile", maxTilesPerFile);

        return q.substituteAndUseDollarSyntax(q);
    }

    private static SQLQuery generateFilteredExportQuery(String schema, String spaceId, String propertyFilter, Export.SpatialFilter spatialFilter,
                                                        String targetVersion, Map params, CSVFormat csvFormat) throws SQLException {
        SQLQuery geoFragment;

        switch (csvFormat){
            case GEOJSON:
                if(spatialFilter != null && spatialFilter.isClipped()) {
                    geoFragment = new SQLQuery("ST_AsGeojson(ST_Intersection(ST_MakeValid(geo), ST_Buffer(ST_GeomFromText(#{wktGeometry})::geography, #{radius})::geometry), 8) as geo");
                    geoFragment.setNamedParameter("radius", spatialFilter.getRadius());
                    geoFragment.setNamedParameter("wktGeometry", WKTHelper.geometryToWKB(spatialFilter.getGeometry()));
                }
                else
                    geoFragment = new SQLQuery("ST_AsGeojson(geo,8)::jsonb");
                break;
            case JSON_WKB:
            case TILEID_FC_B64:
            default:
                if(spatialFilter != null && spatialFilter.isClipped()) {
                    geoFragment = new SQLQuery("ST_Intersection(ST_MakeValid(geo), ST_Buffer(ST_GeomFromText(#{wktGeometry})::geography, #{radius})::geometry) as geo");
                    geoFragment.setNamedParameter("wktGeometry",WKTHelper.geometryToWKB(spatialFilter.getGeometry()));
                    geoFragment.setNamedParameter("radius", spatialFilter.getRadius());
                }
                else
                    geoFragment = new SQLQuery("geo");
        }

        GetFeaturesByGeometryEvent event = new GetFeaturesByGeometryEvent();
        event.setSpace(spaceId);
        event.setParams(params);

        if(params != null && params.get("enableHashedSpaceId") != null)
            event.setConnectorParams(new HashMap<String, Object>(){{put("enableHashedSpaceId",params.get("enableHashedSpaceId"));}});

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
            if(spatialFilter == null) {
                sqlQuery = new SearchForFeatures(event, dbHandler)._buildQuery(event);
            }
            else
                sqlQuery = new GetFeaturesByGeometry(event, dbHandler)._buildQuery(event);
        } catch (Exception e) {
            throw new SQLException(e);
        }

        /** Override geoFragment */
        sqlQuery.setQueryFragment("geo", geoFragment);
        /** Remove Limit */
        sqlQuery.setQueryFragment("limit", "");
        sqlQuery.substitute();

        return queryToText(sqlQuery);
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
