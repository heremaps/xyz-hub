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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.COMPOSITE_EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.TILEID_FC_B64;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.PARTITIONID_FC_B64;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.JSON_WKB;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.PARTITIONED_JSON_WKB;

import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.httpconnector.rest.HApiParam;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Export.ExportStatistic;
import com.here.xyz.httpconnector.util.jobs.Job.CSVFormat;
import com.here.xyz.hub.rest.ApiParam;
import com.here.xyz.models.geojson.coordinates.WKTHelper;
import com.here.xyz.psql.PSQLXyzConnector;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.config.PSQLConfig;
import com.here.xyz.psql.query.GetFeatures;
import com.here.xyz.psql.query.GetFeaturesByGeometry;
import com.here.xyz.psql.query.SearchForFeatures;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.impl.ArrayTuple;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Client for handle Export-Jobs (RDS -> S3)
 */
public class JDBCExporter extends JDBCClients {
    private static final Logger logger = LogManager.getLogger();

    public static Future<ExportStatistic> executeExport(Export job, String schema, String s3Bucket, String s3Path, String s3Region) {
      return addClientsIfRequired(job.getTargetConnector())
          .compose(v -> {
            try {
              String propertyFilter = (job.getFilters() == null ? null : job.getFilters().getPropertyFilter());
              Export.SpatialFilter spatialFilter = (job.getFilters() == null ? null : job.getFilters().getSpatialFilter());
              SQLQuery exportQuery;

              boolean compositeCalculation =   job.readParamCompositeMode() == Export.CompositeMode.CHANGES
                                            || job.readParamCompositeMode() == Export.CompositeMode.FULL_OPTIMIZED;

              CSVFormat pseudoCsvFormat = (  job.getCsvFormat() != PARTITIONED_JSON_WKB 
                                           ? job.getCsvFormat() 
                                           : ( job.getPartitionKey() == null || "tileid".equalsIgnoreCase(job.getPartitionKey()) ? TILEID_FC_B64 : PARTITIONID_FC_B64 )
                                          );

              switch ( pseudoCsvFormat ) {
                  case PARTITIONID_FC_B64:                      exportQuery = generateFilteredExportQuery(job.getId(), schema, job.getTargetSpaceId(), propertyFilter, spatialFilter,
                              job.getTargetVersion(), job.getParams(), job.getCsvFormat(), null,
                              compositeCalculation , job.getPartitionKey(), job.getOmitOnNull());
                      return calculateThreadCountForDownload(job, schema, exportQuery)
                              .compose(threads -> {
                                  try {
                                      Promise<Export.ExportStatistic> promise = Promise.promise();
                                      List<Future> exportFutures = new ArrayList<>();
                                      int tCount = threads,
                                              maxPartitionPerFile = 500000; /* tbd ? */
                                      if (job.getPartitionKey() == null || "id".equalsIgnoreCase(job.getPartitionKey()))
                                          if (job.getFilters() != null && ((job.getFilters().getPropertyFilter() != null) || (
                                                  job.getFilters().getSpatialFilter() != null)))
                                              tCount = threads;
                                          else // only when export by id and no filter is used
                                              tCount = Math.max(threads, (int) Math.floor(job.getEstimatedFeatureCount() / (long) maxPartitionPerFile));
                                      for (int i = 0; i < tCount; i++) {
                                          String s3Prefix = i + "_";
                                          SQLQuery q2 = buildPartIdVMLExportQuery(job, schema, s3Bucket, s3Path, s3Prefix, s3Region, compositeCalculation,
                                                  tCount > 1 ? new SQLQuery("AND i%% " + tCount + " = " + i) : null);
                                          exportFutures.add(exportTypeVML(job.getTargetConnector(), q2, job, s3Path));
                                      }
                                      return executeParallelExportAndCollectStatistics(job, promise, exportFutures);
                                  }
                                  catch (SQLException e) {
                                      logger.warn("job[{}] ", job.getId(), e);
                                      return Future.failedFuture(e);
                                  }
                              });

                  case TILEID_FC_B64:

                           exportQuery = generateFilteredExportQuery(job.getId(), schema, job.getTargetSpaceId(), propertyFilter, spatialFilter,
                              job.getTargetVersion(), job.getParams(), job.getCsvFormat(), null,
                              compositeCalculation , job.getPartitionKey(), job.getOmitOnNull());
                      /*
                      Is used for incremental exports (tiles) - here we have to export modified tiles.
                      Those tiles we need to calculate separately
                       */
                      final SQLQuery qkQuery = compositeCalculation
                              ? generateFilteredExportQueryForCompositeTileCalculation(job.getId(), schema, job.getTargetSpaceId(),
                              propertyFilter, spatialFilter, job.getTargetVersion(), job.getParams(), job.getCsvFormat())
                              : null;

                      return calculateTileListForVMLExport(job, schema, exportQuery, qkQuery)
                              .compose(tileList -> {
                                  try {
                                       Promise<Export.ExportStatistic> promise = Promise.promise();
                                       List<Future> exportFutures = new ArrayList<>();
                                       job.setProcessingList(tileList);

                                       for (int i = 0; i < tileList.size(); i++) {
                                          /** Build export for each tile of the weighted tile list */
                                          SQLQuery q2 = buildVMLExportQuery(job, schema, s3Bucket, s3Path, s3Region, tileList.get(i), qkQuery);
                                          
                                          exportFutures.add( job.getCsvFormat() != PARTITIONED_JSON_WKB 
                                                             ? exportTypeVML(job.getTargetConnector(), q2, job, s3Path) 
                                                             : exportTypeDownload(job.getTargetConnector(), q2, job, s3Path) );

                                       }

                                       return executeParallelExportAndCollectStatistics(job, promise, exportFutures);
                                      }
                                      catch (SQLException e) {
                                        logger.warn("job[{}] ", job.getId(), e);
                                        return Future.failedFuture(e);
                                      }
                             });

                            default:
                                exportQuery = generateFilteredExportQuery(job.getId(), schema, job.getTargetSpaceId(), propertyFilter, spatialFilter,
                                        job.getTargetVersion(), job.getParams(), job.getCsvFormat(), compositeCalculation);
                                return calculateThreadCountForDownload(job, schema, exportQuery)
                                        .compose(threads -> {
                                            try {
                                                Promise<Export.ExportStatistic> promise = Promise.promise();
                                                List<Future> exportFutures = new ArrayList<>();

                                                for (int i = 0; i < threads; i++) {
                                                    String s3Prefix = i + "_";
                                                    SQLQuery q2 = buildS3ExportQuery(job, schema, s3Bucket, s3Path, s3Prefix, s3Region, compositeCalculation,
                                                            (threads > 1 ? new SQLQuery("AND i%% " + threads + " = " + i) : null));
                                                    exportFutures.add(exportTypeDownload(job.getTargetConnector(), q2, job, s3Path));
                                                }

                                      return executeParallelExportAndCollectStatistics(job, promise, exportFutures);
                                  }
                                  catch (SQLException e) {
                                      logger.warn("job[{}] ", job.getId(), e);
                                      return Future.failedFuture(e);
                                  }
                              });
              }
            }
            catch (Exception e) {
              return Future.failedFuture(e);
            }
          });
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
                                logger.warn("job[{}] Export failed {}: {}", j.getId(), j.getTargetSpaceId(), t.cause());
                                promise.fail(t.cause());
                            }
                        });
        return promise.future();
    }

    private static Future<Integer> calculateThreadCountForDownload(Export j, String schema, SQLQuery exportQuery) throws SQLException {
        //Currently we are ignoring filters and count only all features
        SQLQuery q = buildS3CalculateQuery(j, schema, exportQuery);
        logger.info("job[{}] Calculate S3-Export {}: {}", j.getId(), j.getTargetSpaceId(), q.text());

        return getClient(j.getTargetConnector(), true)
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

    private static Future<List<String>> calculateTileListForVMLExport(Export j, String schema, SQLQuery exportQuery, SQLQuery qkQuery) throws SQLException {
        if(j.getProcessingList() != null)
            return Future.succeededFuture(j.getProcessingList());

        SQLQuery q = buildVMLCalculateQuery(j, schema, exportQuery, qkQuery);
        logger.info("job[{}] Calculate VML-Export {}: {}", j.getId(), j.getTargetSpaceId(), q.text());

        return getClient(j.getTargetConnector(), false)
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
        logger.info("job[{}] Execute S3-Export {}->{} {}", j.getId(), j.getTargetSpaceId(), s3Path, q.text());

        return getClient(clientId, true)
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
        logger.info("job[{}] Execute VML-Export {}->{} {}", j.getId(), j.getTargetSpaceId(), s3Path, q.text());

        return getClient(clientId, true)
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
        SQLQuery q = new SQLQuery("select /* s3_export_hint m499#jobId(" + job.getId() + ") */ ${schema}.exp_type_download_precalc(" +
                "#{estimated_count}, ${{exportSelectString}}, #{tbl}::regclass) as thread_cnt");

        q.setVariable("schema", schema);
        q.setNamedParameter("estimated_count", job.getEstimatedFeatureCount());
        q.setQueryFragment("exportSelectString", query);
        q.setNamedParameter("tbl", schema+".\""+job.getTargetTable()+"\"");

        return q.substituteAndUseDollarSyntax(q);
    }

    public static SQLQuery buildVMLCalculateQuery(Export job, String schema, SQLQuery exportQuery, SQLQuery qkQuery) {
        int exportCalcLevel = job.getTargetLevel() != null ? Math.max( job.getTargetLevel() - 1, 3 ) : 11;

        SQLQuery q = new SQLQuery("select tilelist /* vml_export_hint m499#jobId(" + job.getId() + ") */ from ${schema}.exp_type_vml_precalc(" +
                "#{htile}, '', #{mlevel}, ${{exportSelectString}}, ${{qkQuery}}, #{estimated_count}, #{tbl}::regclass)");

        q.setVariable("schema", schema);
        q.setNamedParameter("htile",true);
        q.setNamedParameter("idk","''");
        q.setNamedParameter("mlevel", exportCalcLevel);
        q.setQueryFragment("exportSelectString", exportQuery);
        q.setQueryFragment("qkQuery", qkQuery == null ?  new SQLQuery("null::text") : qkQuery);
        q.setNamedParameter("estimated_count", job.getEstimatedFeatureCount());
        q.setNamedParameter("tbl", schema+".\""+job.getTargetTable()+"\"");

        return q.substituteAndUseDollarSyntax(q);
    }

    public static SQLQuery buildS3ExportQuery(Export j, String schema,
                                              String s3Bucket, String s3Path, String s3FilePrefix, String s3Region,
                                              boolean isForCompositeContentDetection, SQLQuery customWhereCondition) throws SQLException {

        String propertyFilter = (j.getFilters() == null ? null : j.getFilters().getPropertyFilter());
        Export.SpatialFilter spatialFilter= (j.getFilters() == null ? null : j.getFilters().getSpatialFilter());

        s3Path = s3Path+ "/" +(s3FilePrefix == null ? "" : s3FilePrefix)+"export.csv";
        SQLQuery exportSelectString = generateFilteredExportQuery(j.getId(), schema, j.getTargetSpaceId(), propertyFilter, spatialFilter,
                j.getTargetVersion(), j.getParams(), j.getCsvFormat(), customWhereCondition, isForCompositeContentDetection,
                j.getPartitionKey(), j.getOmitOnNull());

        SQLQuery q = new SQLQuery("SELECT * /* s3_export_hint m499#jobId(" + j.getId() + ") */ from aws_s3.query_export_to_s3( "+
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

    public static SQLQuery buildPartIdVMLExportQuery(Export j, String schema, String s3Bucket, String s3Path, String s3FilePrefix,
        String s3Region, boolean isForCompositeContentDetection, SQLQuery customWhereCondition) throws SQLException {
        //Generic partition
        String propertyFilter = (j.getFilters() == null ? null : j.getFilters().getPropertyFilter());
        Export.SpatialFilter spatialFilter= (j.getFilters() == null ? null : j.getFilters().getSpatialFilter());

        s3Path = s3Path+ "/" +(s3FilePrefix == null ? "" : s3FilePrefix)+"export.csv";

        SQLQuery exportSelectString = generateFilteredExportQuery(j.getId(), schema, j.getTargetSpaceId(), propertyFilter, spatialFilter,
                j.getTargetVersion(), j.getParams(), j.getCsvFormat(), customWhereCondition, isForCompositeContentDetection,
                j.getPartitionKey(), j.getOmitOnNull());

        SQLQuery q = new SQLQuery("SELECT * /* vml_export_hint m499#jobId(" + j.getId() + ") */ from aws_s3.query_export_to_s3( "+
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

    public static SQLQuery buildVMLExportQuery(Export j, String schema, String s3Bucket, String s3Path, String s3Region, String parentQk,
        SQLQuery qkTileQry) throws SQLException {
        //Tiled export
        String propertyFilter = (j.getFilters() == null ? null : j.getFilters().getPropertyFilter());
        Export.SpatialFilter spatialFilter= (j.getFilters() == null ? null : j.getFilters().getSpatialFilter());

        int maxTilesPerFile = j.getMaxTilesPerFile() == 0 ? 4096 : j.getMaxTilesPerFile();

        SQLQuery exportSelectString =  generateFilteredExportQuery(j.getId(), schema, j.getTargetSpaceId(), propertyFilter, spatialFilter,
            j.getTargetVersion(), j.getParams(), j.getCsvFormat());

        /** QkTileQuery gets used if we are exporting in compositeMode. In this case we need to also include empty tiles to our export. */
        boolean includeEmpty = qkTileQry != null,
                bPartJsWkb   = ( j.getCsvFormat() == PARTITIONED_JSON_WKB );

        SQLQuery q = new SQLQuery(
                "select ("+
                        " aws_s3.query_export_to_s3( o.s3sql, " +
                        "   #{s3Bucket}, " +
                        "   format('%s/%s/%s-%s.csv',#{s3Path}::text, o.qk, o.bucket, o.nrbuckets) ," +
                        "   #{s3Region}," +
                        "   options := 'format csv,delimiter '','', encoding ''UTF8'', quote  ''\"'', escape '''''''' ')).* " +
                        "  /* vml_export_hint m499#jobId(" + j.getId() + ") */ " +
                        " from" +
                        "    ${{exp_build_fkt}}(true, #{parentQK}, #{targetLevel}, ${{exportSelectString}}, ${{qkTileQry}}, #{maxTilesPerFile}::int, ${{b64EncodeParam}} #{isClipped}, #{includeEmpty}) o"
        );

        q.setQueryFragment("exp_build_fkt", !bPartJsWkb ? "exp_build_sql_inhabited_txt" : "exp2_build_sql_inhabited_txt" );
        q.setQueryFragment("b64EncodeParam", !bPartJsWkb ? "true," : "" );

        q.setQueryFragment("exportSelectString", exportSelectString);
        q.setQueryFragment("qkTileQry", qkTileQry == null ? new SQLQuery("null::text") : qkTileQry );

        q.setNamedParameter("s3Bucket",s3Bucket);
        q.setNamedParameter("s3Path",s3Path);
        q.setNamedParameter("s3Region",s3Region);
        q.setNamedParameter("targetLevel", j.getTargetLevel());
        q.setNamedParameter("parentQK", parentQk);
        q.setNamedParameter("maxTilesPerFile", maxTilesPerFile);
        q.setNamedParameter("isClipped", j.getClipped() != null && j.getClipped());
        q.setNamedParameter("includeEmpty", includeEmpty);

        return q.substituteAndUseDollarSyntax(q);
    }

    private static SQLQuery generateFilteredExportQuery(String jobId, String schema, String spaceId, String propertyFilter,
        Export.SpatialFilter spatialFilter, String targetVersion, Map params, CSVFormat csvFormat) throws SQLException {
        return generateFilteredExportQuery(jobId, schema, spaceId, propertyFilter, spatialFilter, targetVersion, params, csvFormat, null, false, null, false);
    }

    private static SQLQuery generateFilteredExportQuery(String jobId, String schema, String spaceId, String propertyFilter,
                                                        Export.SpatialFilter spatialFilter, String targetVersion, Map params, CSVFormat csvFormat, boolean isForCompositeContentDetection) throws SQLException {
        return generateFilteredExportQuery(jobId, schema, spaceId, propertyFilter, spatialFilter, targetVersion, params, csvFormat, null, isForCompositeContentDetection, null, false);
    }


    private static SQLQuery generateFilteredExportQueryForCompositeTileCalculation(String jobId, String schema, String spaceId, String propertyFilter,
                                                        Export.SpatialFilter spatialFilter, String targetVersion, Map params, CSVFormat csvFormat) throws SQLException {
        return generateFilteredExportQuery(jobId, schema, spaceId, propertyFilter, spatialFilter, targetVersion, params, csvFormat, null, true, null, false);
    }

    private static SQLQuery generateFilteredExportQuery(String jobId, String schema, String spaceId, String propertyFilter,
        Export.SpatialFilter spatialFilter, String targetVersion, Map params, CSVFormat csvFormat, SQLQuery customWhereCondition, boolean isForCompositeContentDetection, String partitionKey, Boolean omitOnNull )
        throws SQLException {
        
        csvFormat = (( csvFormat == PARTITIONED_JSON_WKB && ( partitionKey == null || "tileid".equalsIgnoreCase(partitionKey)) ) ? TILEID_FC_B64 : csvFormat );
        //TODO: Re-use existing QR rather than the following duplicated code
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

        if (params != null && params.get("enableHashedSpaceId") != null)
            event.setConnectorParams(new HashMap<>(){{put("enableHashedSpaceId", params.get("enableHashedSpaceId"));}});

        if (params != null && params.get("versionsToKeep") != null)
            event.setVersionsToKeep((int)params.get("versionsToKeep"));

        if (params != null && params.get("context") != null) {
            ContextAwareEvent.SpaceContext context = ContextAwareEvent.SpaceContext.of((String) params.get("context"));

            //TODO: Remove the following hack and perform the switch to super within connector (GetFeatures QR) instead
            if (context == SUPER) {
                //switch to super space
                Map<String,Object> ext = (Map<String, Object>) params.get("extends");
                if (ext != null) {
                    String superSpace = (String) ext.get("spaceId");
                    if (superSpace != null)
                        event.setSpace(superSpace);

                }
                context = ContextAwareEvent.SpaceContext.DEFAULT;
            }
            event.setContext(context);
        }

        if (targetVersion != null)
            event.setRef(targetVersion);

        if (propertyFilter != null) {
            PropertiesQuery propertyQueryLists = HApiParam.Query.parsePropertiesQuery(propertyFilter, "", false);
            event.setPropertiesQuery(propertyQueryLists);
        }

        if (spatialFilter != null) {
            event.setGeometry(spatialFilter.getGeometry());
            event.setRadius(spatialFilter.getRadius());
            event.setClip(spatialFilter.isClipped());
        }

        PSQLXyzConnector dbHandler = new PSQLXyzConnector(false);
        dbHandler.setConfig(new PSQLConfig(event, schema));

        boolean partitionByPropertyValue = ((csvFormat == PARTITIONID_FC_B64 || csvFormat == PARTITIONED_JSON_WKB) && partitionKey != null && !"id".equalsIgnoreCase(partitionKey)),
                partitionByFeatureId     = ((csvFormat == PARTITIONID_FC_B64 || csvFormat == PARTITIONED_JSON_WKB) && !partitionByPropertyValue ),
                downloadAsJsonWkb        = ( csvFormat == JSON_WKB );
                
        SpaceContext ctxStashed = event.getContext();        

        if (isForCompositeContentDetection)
            event.setContext( (partitionByFeatureId || downloadAsJsonWkb) ? EXTENSION : COMPOSITE_EXTENSION);

        SQLQuery sqlQuery,
                 sqlQueryContentByPropertyValue = null;

        try {
          GetFeatures queryRunner;

          if (spatialFilter == null)
            queryRunner = new SearchForFeatures(event);
          else
            queryRunner = new GetFeaturesByGeometry(event);

          queryRunner.setDbHandler(dbHandler);
          sqlQuery = queryRunner._buildQuery(event);

          if( partitionByPropertyValue && isForCompositeContentDetection )
          { event.setContext(ctxStashed);
            sqlQueryContentByPropertyValue = queryRunner._buildQuery(event);
          }

        }
        catch (Exception e) {
          throw new SQLException(e);
        }

        //Override geoFragment
        sqlQuery.setQueryFragment("geo", geoFragment);
        //Remove Limit
        sqlQuery.setQueryFragment("limit", "");

        if( sqlQueryContentByPropertyValue != null )
        {
         sqlQueryContentByPropertyValue.setQueryFragment("geo", geoFragment);
         sqlQueryContentByPropertyValue.setQueryFragment("limit", "");
        }

        if (customWhereCondition != null && csvFormat != PARTITIONID_FC_B64 )
            addCustomWhereClause(sqlQuery, customWhereCondition);

        switch (csvFormat) {
          case GEOJSON :
          {
            SQLQuery geoJson = new SQLQuery("select jsondata || jsonb_build_object('geometry', ST_AsGeoJSON(geo, 8)::jsonb) "
                + "from (${{contentQuery}}) X")
                .withQueryFragment("contentQuery", sqlQuery)
                .substitute();
            return queryToText(geoJson);
          }
         
         case PARTITIONED_JSON_WKB :
         case PARTITIONID_FC_B64   :
         {
            String partQry = 
                         csvFormat == PARTITIONID_FC_B64
                            ? ( isForCompositeContentDetection
                              ? "select jsondata->>'id' as id, " 
                              + " case not coalesce((jsondata#>'{properties,@ns:com:here:xyz,deleted}')::boolean,false) "
                              + "  when true then replace( encode(convert_to(jsonb_build_object( 'type','FeatureCollection','features', jsonb_build_array( jsondata || jsonb_build_object( 'geometry', ST_AsGeoJSON(geo,8)::jsonb ) ) )::text,'UTF8'),'base64') ,chr(10),'') "
                              + "  else null::text "
                              + " end as data "
                              + "from ( ${{contentQuery}}) X"
                              :  "select jsondata->>'id' as id, " 
                              + " replace( encode(convert_to(jsonb_build_object( 'type','FeatureCollection','features', jsonb_build_array( jsondata || jsonb_build_object( 'geometry', ST_AsGeoJSON(geo,8)::jsonb ) ) )::text,'UTF8'),'base64') ,chr(10),'') as data "
                              + "from ( ${{contentQuery}}) X" )
                       /* PARTITIONED_JSON_WKB */
                            : ( isForCompositeContentDetection
                              ? "select jsondata->>'id' as id, " 
                              + " case not coalesce((jsondata#>'{properties,@ns:com:here:xyz,deleted}')::boolean,false) when true then jsondata else null::jsonb end as jsondata," 
                              + " geo "
                              + "from ( ${{contentQuery}}) X"
                              : "select jsondata->>'id' as id, jsondata, geo" 
                              + " replace( encode(convert_to(jsonb_build_object( 'type','FeatureCollection','features', jsonb_build_array( jsondata || jsonb_build_object( 'geometry', ST_AsGeoJSON(geo,8)::jsonb ) ) )::text,'UTF8'),'base64') ,chr(10),'') as data "
                              + "from ( ${{contentQuery}}) X" );

           if( partitionByPropertyValue )
           {  
              String converted = ApiParam.getConvertedKey(partitionKey);
              partitionKey =  String.join("'->'",(converted != null ? converted : partitionKey).split("\\."));
              partQry = String.format(
                 " with  "
                +" plist as  "
                +" ( select o.* from "
                +"   ( select ( dense_rank() over (order by key) )::integer as i, key  "
                +"     from "
                +"     ( select coalesce( key, '\"CSVNULL\"'::jsonb) as key "
                +"       from ( select distinct jsondata->'%1$s' as key from ( ${{contentQuery}} ) X "+ (( omitOnNull == null || !omitOnNull ) ? "" : " where not jsondata->'%1$s' isnull " ) +" ) oo "
                +"     ) d1"
                +"   ) o "
                +"   where 1 = 1 " + ( customWhereCondition != null ? "${{customWhereClause}}" :"" )
                +" ), "
                +" iidata as  "
                +" ( select l.key, (( row_number() over ( partition by l.key ) )/ 20000000)::integer as chunk, r.jsondata, r.geo from ( ${{%2$s}} ) r right join plist l on ( coalesce( r.jsondata->'%1$s', '\"CSVNULL\"'::jsonb) = l.key )  "
                +" ), "
                + ( csvFormat == PARTITIONID_FC_B64
                   ? " iiidata as  "
                    +" ( select coalesce( ('[]'::jsonb || key)->>0, 'CSVNULL' ) as id, (count(1) over ()) as nrbuckets, count(1) as nrfeatures, replace( encode(convert_to(('{\"type\":\"FeatureCollection\",\"features\":[' || coalesce( string_agg( (jsondata || jsonb_build_object('geometry',st_asgeojson(geo,8)::jsonb))::text, ',' ), null::text ) || ']}'),'UTF8'),'base64') ,chr(10),'') as data "
                    +"   from iidata group by 1, chunk order by 1, 3 desc   "
                    +" )   "
                    +" select id, data from iiidata "
                   : " iiidata as  "
                    +" ( select coalesce( ('[]'::jsonb || key)->>0, 'CSVNULL' ) as id, jsondata, geo "
                    +"   from iidata "
                    +" )   "
                    +" select id, jsondata, geo from iiidata "
                  ), partitionKey, sqlQueryContentByPropertyValue != null ? "contentQueryReal" : "contentQuery" );
           }

                SQLQuery geoJson = new SQLQuery(partQry)
                        .withQueryFragment("customWhereCondition", "");

           if (  partitionByFeatureId && customWhereCondition != null )
            addCustomWhereClause(sqlQuery, customWhereCondition);

           if ( partitionByPropertyValue && customWhereCondition != null )
             geoJson.withQueryFragment("customWhereClause", customWhereCondition);

           geoJson.setQueryFragment("contentQuery", sqlQuery);

           if( sqlQueryContentByPropertyValue != null )
            geoJson.setQueryFragment("contentQueryReal", sqlQueryContentByPropertyValue);

           geoJson.substitute();
           return queryToText(geoJson);
         }

            default:
            {
                sqlQuery
                        .withQueryFragment("id", "")
                        .substitute();
                return queryToText(sqlQuery);
            }
        }

    }

    private static void addCustomWhereClause(SQLQuery query, SQLQuery customWhereClause) {
        SQLQuery filterWhereClause = query.getQueryFragment("filterWhereClause");
        SQLQuery customizedWhereClause = new SQLQuery("${{innerFilterWhereClause}} ${{customWhereClause}}")
            .withQueryFragment("innerFilterWhereClause", filterWhereClause)
            .withQueryFragment("customWhereClause", customWhereClause);
        query.setQueryFragment("filterWhereClause", customizedWhereClause);
    }

    //FIXME: The following works only for very specific kind of queries
    private static SQLQuery queryToText(SQLQuery q) {
        String queryText = q.text()
            .replace("?", "%L")
            .replace("'","''");

        int i = 0;
        String replacement = "";
        Map<String, Object> newParams = new HashMap<>();
        for (Object paramValue : q.parameters()) {
            String paramName = "var" + i++;
            replacement += ",(#{"+paramName+"})";
            newParams.put(paramName, paramValue);
        }

      SQLQuery sq = new SQLQuery(String.format("format('%s'%s)", queryText, replacement))
          .withNamedParameters(newParams);
      return sq;
    }
}

