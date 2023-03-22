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
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.query.GetFeaturesByGeometry;
import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.impl.ArrayTuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Client for handle Export-Jobs (RDS -> S3)
 */
public class JDBCExporter extends JDBCClients{
    private static final Logger logger = LogManager.getLogger();
    private static final boolean VML_USE_CLIPPING = true;

    public static Future<Export.ExportStatistic> executeExport(Export j, String schema, String s3Bucket, String s3Path, String s3Region){
        return executeExport(j.getTargetConnector(), schema, j.getTargetTable(),
                s3Bucket, s3Path, s3Region,
                j.getCsvFormat(), j.getExportTarget(),
                j.getFilters(),
                j.getTargetLevel(), j.getMaxTilesPerFile());
    }

    public static Future<Export.ExportStatistic> executeExport(String clientID, String schema, String tablename,
                                                               String s3Bucket, String s3Path, String s3Region,
                                                               CSVFormat csvFormat, Export.ExportTarget target,
                                                               Export.Filters filters,
                                                               int targetLevel, int maxTilesPerFile){
        SQLQuery q;

        switch (target.getType()){
            case S3:
                q = buildS3ExportQuery(schema, tablename, s3Bucket, s3Path, s3Region, csvFormat, filters.getPropertyFilter(), filters.getSpatialFilter());
                logger.info("Execute S3-Export {}->{} {}", tablename, s3Path, q.text());

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
                q = buildVMLExportQuery(schema, tablename, s3Bucket, s3Path, s3Region, csvFormat, targetLevel, maxTilesPerFile, filters.getPropertyFilter(), filters.getSpatialFilter());
                logger.info("Execute VML-Export {}->{} {}", tablename, s3Path, q.text());

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
    }

    public static SQLQuery buildS3ExportQuery(String schema, String tablename, String s3Bucket, String s3Path, String s3Region, CSVFormat csvFormat,
                                              String propertyFilter, Export.SpatialFilter spatialFilter){
        s3Path = s3Path+"/export.csv";
        SQLQuery exportSelectString = generateFilteredExportQuery(schema, tablename, propertyFilter, spatialFilter, csvFormat);

        SQLQuery q = new SQLQuery("SELECT * from aws_s3.query_export_to_s3( "+
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

    public static SQLQuery buildVMLExportQuery(String schema, String tablename, String s3Bucket, String s3Path, String s3Region,  CSVFormat csvFormat,
                                               int targetLevel, int maxTilesPerFile,  String propertyFilter, Export.SpatialFilter spatialFilter){

        maxTilesPerFile = (maxTilesPerFile == 0 ? 4096 : maxTilesPerFile);

        SQLQuery exportSelectString =  generateFilteredExportQuery(schema, tablename, propertyFilter, spatialFilter, csvFormat);

        SQLQuery q = new SQLQuery(
                "select("+
                        " aws_s3.query_export_to_s3( o.s3sql , " +
                        "   #{s3Bucket}, " +
                        "   format('%s/%s/%s-%s.csv',#{s3Path}, o.qk, o.bucket, o.nrbuckets) ," +
                        "   #{s3Region}," +
                        "   'format csv')).* ," +
                        "  o.* " +
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

    private static SQLQuery generateFilteredExportQuery(String schema, String tablename, String propertyFilter, Export.SpatialFilter spatialFilter, CSVFormat csvFormat){
        SQLQuery selectFragment;
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

                selectFragment  = new SQLQuery("select jsonb_build_object(" +
                                "  ''type'', ''Feature''," +
                                "  ''properties'', jsondata," +
                                "  ''geometry'',${{geoFragment}}" +
                                ")");
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

                selectFragment  = new SQLQuery("select jsondata, ${{geoFragment}}");

        }

        selectFragment.setQueryFragment("geoFragment",geoFragment);

        SQLQuery spatialFragment = new SQLQuery("");
        SQLQuery whereFragment = new SQLQuery("");

        GetFeaturesByGeometryEvent event = new GetFeaturesByGeometryEvent();

        if(propertyFilter != null) {
            PropertiesQuery propertyQueryLists = HApiParam.Query.parsePropertiesQuery(propertyFilter, "", false);
            event.setPropertiesQuery(propertyQueryLists);

            if(propertyFilter == null){
                logger.info("No valid propertyFilter {}", propertyFilter);
            }else {
                SQLQuery searchFragment = GetFeaturesByGeometry.generateSearchQuery(event);

                whereFragment = new SQLQuery("AND ${{searchFragment}}");
                whereFragment.setQueryFragment("searchFragment",searchFragment);
                whereFragment.replaceFragments();
            }
        }
        if(spatialFilter != null){
            event.setGeometry(spatialFilter.getGeometry());
            event.setRadius(spatialFilter.getRadius());

            SQLQuery geoFilter = GetFeaturesByGeometry.buildSpatialGeoFilter(event);

            spatialFragment = new SQLQuery("AND ST_Intersects(geo, ${{geoFilter}})");
            spatialFragment.setQueryFragment("geoFilter", geoFilter);
            spatialFragment.replaceFragments();
        }

        SQLQuery filterFragment = new SQLQuery("${{selectFragment}} from ${schema}.${table} " +
                "where 1=1 ${{whereFragment}} ${{spatialFragment}}");

        filterFragment.setQueryFragment("selectFragment", selectFragment);
        filterFragment.setQueryFragment("whereFragment", whereFragment);
        filterFragment.setQueryFragment("spatialFragment",  spatialFragment);
        filterFragment.setVariable("table", tablename);
        filterFragment.setVariable("schema", schema);

        filterFragment.replaceFragments();
        filterFragment.substitute();

        return queryToText(filterFragment);
    }

    private static SQLQuery queryToText(SQLQuery q){
        SQLQuery sq = new SQLQuery();
        String s = q.text().replace("?", "%L");

        String r =  "format('"+s+"'";

        int i = 0;
        for (Object o :q.parameters()) {
            String curVar = "var"+(i++);
            r += ",#{"+curVar+"}";
            sq.setNamedParameter(curVar,o);
        }

        sq.setText(r+")");
        return sq;
    }
}
