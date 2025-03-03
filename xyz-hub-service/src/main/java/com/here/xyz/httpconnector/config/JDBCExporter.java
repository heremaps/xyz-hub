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

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.COMPOSITE_EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.config.query.ExportSpace;
import com.here.xyz.httpconnector.config.query.ExportSpaceByGeometry;
import com.here.xyz.httpconnector.config.query.ExportSpaceByProperties;
import com.here.xyz.httpconnector.task.JdbcBasedHandler;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Export.ExportStatistic;
import com.here.xyz.httpconnector.util.jobs.Job.CSVFormat;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.GEOJSON;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.JSON_WKB;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.PARTITIONED_JSON_WKB;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.PARTITIONID_FC_B64;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.TILEID_FC_B64;
import com.here.xyz.httpconnector.util.web.LegacyHubWebClient;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.jobs.datasets.DatasetDescription.Space;
import com.here.xyz.jobs.datasets.filters.Filters;
import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.models.geojson.coordinates.WKTHelper;
import com.here.xyz.models.hub.Ref;
import static com.here.xyz.models.hub.Ref.HEAD;
import com.here.xyz.util.Hasher;
import com.here.xyz.util.db.JdbcClient;
import com.here.xyz.util.db.SQLQuery;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import java.sql.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Client for handle Export-Jobs (RDS -> S3)
 */
public class JDBCExporter extends JdbcBasedHandler {
    private static final Logger logger = LogManager.getLogger();
    private static final JDBCExporter instance = new JDBCExporter();

    private JDBCExporter() {
      super(CService.configuration.JOB_DB_POOL_SIZE_PER_CLIENT);
    }

    public static JDBCExporter getInstance() {
      return instance;
    }

    static private final int PSEUDO_NEXT_VERSION = 0;
    //Space-Copy Begin
    private SQLQuery buildCopySpaceQuery(JdbcClient client, Export job,
        Connector targetSpaceConnector, boolean targetVersioningEnabled) throws SQLException {
      boolean enableHashedSpaceId = targetSpaceConnector.params.containsKey("enableHashedSpaceId")
                                    ? (boolean) targetSpaceConnector.params.get("enableHashedSpaceId")
                                    : false;
      String targetSpaceId = job.getTarget().getKey();
      final String tableName = enableHashedSpaceId ? Hasher.getHash(targetSpaceId) : targetSpaceId;
      return new SQLQuery(
          """
            WITH ins_data as /* space_copy_hint m499#jobId(${{jobId}}) */
            (INSERT INTO ${schema}.${table} (jsondata, operation, author, geo, id, version, next_version )
            SELECT idata.jsondata, CASE WHEN idata.operation in ('I', 'U') THEN (CASE WHEN edata.id isnull THEN 'I' ELSE 'U' END) ELSE idata.operation END AS operation, idata.author, idata.geo, idata.id,
                   (SELECT nextval('${schema}.${versionSequenceName}')) AS version,
                   CASE WHEN edata.id isnull THEN max_bigint() ELSE ${{pseudoNextVersion}} END as next_version
            FROM
              (${{contentQuery}} ) idata
              LEFT JOIN ${schema}.${table} edata ON (idata.id = edata.id AND edata.next_version = max_bigint())
              RETURNING id, version, (coalesce(pg_column_size(jsondata),0) + coalesce(pg_column_size(geo),0))::bigint as bytes_size
            ),
            upd_data as
            (UPDATE ${schema}.${table}
               SET next_version = (SELECT version FROM ins_data LIMIT 1)
             WHERE ${{targetVersioningEnabled}}
                AND next_version = max_bigint()
                AND id IN (SELECT id FROM ins_data)
                AND version < (SELECT version FROM ins_data LIMIT 1)
              RETURNING id, version
            ),
            del_data AS
            (DELETE FROM ${schema}.${table}
              WHERE not ${{targetVersioningEnabled}}
                AND id IN (SELECT id FROM ins_data)
                AND version < (SELECT version FROM ins_data LIMIT 1)
              RETURNING id, version
            )
            SELECT count(1) AS rows_uploaded, sum(bytes_size)::BIGINT AS bytes_uploaded, 0::BIGINT AS files_uploaded,
                  (SELECT count(1) FROM upd_data) AS version_updated,
                  (SELECT count(1) FROM del_data) AS version_deleted
            FROM ins_data l
          """
          ).withVariable("schema", getDbSettings(job.getTargetConnector()).getSchema())
           .withVariable("table", tableName)
           .withQueryFragment("jobId", job.getId())
           .withQueryFragment("targetVersioningEnabled", "" + targetVersioningEnabled)
           .withVariable("versionSequenceName", tableName + "_version_seq")
           .withQueryFragment("pseudoNextVersion", PSEUDO_NEXT_VERSION + "" )
           .withQueryFragment("contentQuery", buildCopyContentQuery(client, job, enableHashedSpaceId));
    }

    private SQLQuery buildCopySpaceNextVersionUpdate(JdbcClient client, Export job, Connector targetSpaceConnector) throws SQLException {
      boolean enableHashedSpaceId = targetSpaceConnector.params.containsKey("enableHashedSpaceId")
                                    ? (boolean) targetSpaceConnector.params.get("enableHashedSpaceId")
                                    : false;
      String targetSpaceId = job.getTarget().getKey();
      final String tableName = enableHashedSpaceId ? Hasher.getHash(targetSpaceId) : targetSpaceId;
      /* adjust next_version to max_bigint(), except in case of concurency set it to concurrent inserted version */
      //TODO: case of extern concurency && same id in source & target && non-versiond layer a duplicate id can occure with next_version = concurrent_inserted.version
      return new SQLQuery(
          """
            UPDATE /* space_copy_hint m499#jobId(${{jobId}}) */ 
             ${schema}.${table} t
             set next_version = coalesce(( select version from ${schema}.${table} i where i.id = t.id and i.next_version = max_bigint() ), max_bigint())
            where 
             next_version = ${{pseudoNextVersion}}
          """
          ).withVariable("schema", getDbSettings(job.getTargetConnector()).getSchema())
           .withVariable("table", tableName)
           .withQueryFragment("jobId", job.getId())
           .withQueryFragment("pseudoNextVersion", PSEUDO_NEXT_VERSION + "" );
    }

  private static SQLQuery buildCopyContentQuery(JdbcClient client, Export job, boolean enableHashedSpaceId) throws SQLException {
    String propertyFilter = null;
    SpatialFilter spatialFilter = null;
////// get filters from source space
    if( job.getSource() != null && job.getSource() instanceof Space )
    { Filters f = ((Space) job.getSource()).getFilters();
      propertyFilter = ( f == null ? null : f.getPropertyFilterAsString() );
      spatialFilter = ( f == null ? null : f.getSpatialFilter() );
    }
////// if filters not provided by source space the get filter from job (legacy behaviour)
    if( propertyFilter == null )
     propertyFilter = (job.getFilters() == null ? null : job.getFilters().getPropertyFilterAsString());

    if( spatialFilter == null )
     spatialFilter = (job.getFilters() == null ? null : job.getFilters().getSpatialFilter());
//////

    GetFeaturesByGeometryEvent event = new GetFeaturesByGeometryEvent()
        .withSpace(job.getSource().getKey())
        .withParams(job.getParams())
        .withContext(EXTENSION)
        .withConnectorParams(Collections.singletonMap("enableHashedSpaceId", enableHashedSpaceId));

      if( event.getParams() != null && event.getParams().get("versionsToKeep") != null )
       event.setVersionsToKeep((int) event.getParams().get("versionsToKeep") ); // -> forcing "...AND next_version = maxBigInt..." in query

      event.setRef( job.getTargetVersion() == null ? new Ref(HEAD) : new Ref(job.getTargetVersion()) );

      if (propertyFilter != null) {
          PropertiesQuery propertyQueryLists = PropertiesQuery.fromString(propertyFilter, "", false);
          event.setPropertiesQuery(propertyQueryLists);
      }

      if (spatialFilter != null) {
          event.setGeometry(spatialFilter.getGeometry());
          event.setRadius(spatialFilter.getRadius());
          event.setClip(spatialFilter.isClipped());
      }

    try {

      return getQueryRunner(client, spatialFilter, event)
          //TODO: Why not selecting the feature id / geo here?
          //FIXME: Do not select operation / author as part of the "property-selection"-fragment
          .withSelectionOverride(new SQLQuery("jsondata, operation, author"))
          .withGeoOverride(buildGeoFragment(spatialFilter))
          .buildQuery(event);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }


  private Future<ExportStatistic> executeCopyQuery(JdbcClient client, SQLQuery q, Export job) {
      logger.info("job[{}] Execute Query Space-Copy {}->{} {}", job.getId(), job.getTargetSpaceId(), "Space-Destination", q.text());
      return client.run(q, rs -> {
        ExportStatistic es = new Export.ExportStatistic();
        if (rs.next())
          return new Export.ExportStatistic()
              .addRows(rs.getLong("rows_uploaded"))
              .addFiles(rs.getLong("files_uploaded"))
              .addBytes(rs.getLong("bytes_uploaded"));
        return es;
      });
    }

  private Future<ExportStatistic> executeCopyQuery(JdbcClient client, SQLQuery q, SQLQuery q2, Export job)
  {
    return executeCopyQuery( client, q, job )
             .compose( e -> {
               return client.write(q2).compose( i -> Future.succeededFuture(e) );
            } );
  }

    public Future<ExportStatistic> executeCopy(Export job) {
      Promise<Export.ExportStatistic> promise = Promise.promise();
      List<Future> exportFutures = new ArrayList<>();

      String spaceId = job.getTarget().getKey();
      return LegacyHubWebClient.getSpace( spaceId )
          .compose(space -> LegacyHubWebClient.getConnectorConfig(space.getStorage().getId())
              .compose(connector -> getClient(connector.id)
                  .compose(client -> {
                    try {
                      boolean targetVersioningEnabled = space.getVersionsToKeep() > 1;

                      SQLQuery copyQuery = buildCopySpaceQuery(client, job, connector, targetVersioningEnabled ),
                               setNextVersionUpdateSql = buildCopySpaceNextVersionUpdate(client, job, connector );

                      exportFutures.add(executeCopyQuery(client, copyQuery, setNextVersionUpdateSql, job));
                      return executeParallelExportAndCollectStatistics(job, promise, exportFutures);
                    }
                    catch (SQLException e) {
                      return Future.failedFuture(e);
                    }
                  }))
              .compose(statistics -> LegacyHubWebClient.updateSpaceConfig(new JsonObject().put("contentUpdatedAt", System.currentTimeMillis()), space.getId())
                    .map(statistics))
          );
    }

    //Space-Copy End

    private boolean isIncrementalExportNonComposite( CSVFormat csvFormat, String targetVersion, boolean compositeCalculation )
    {
      return
         compositeCalculation == false
      && csvFormat == PARTITIONED_JSON_WKB
      && targetVersion != null
      && (new Ref( targetVersion )).isRange();
    }

    public Future<ExportStatistic> executeExport(Export job, String s3Bucket, String s3Path, String s3Region) {
      logger.info("job[{}] Execute Export-legacy csvFormat({}) ParamCompositeMode({}) PartitionKey({})", job.getId(), job.getCsvFormat(), job.readParamCompositeMode(), job.getPartitionKey() );

      return getClient(job.getTargetConnector())
          .compose(client -> {
            String schema = getDbSettings(job.getTargetConnector()).getSchema();
            try {
              String propertyFilter = (job.getFilters() == null ? null : job.getFilters().getPropertyFilterAsString());
              SpatialFilter spatialFilter = (job.getFilters() == null ? null : job.getFilters().getSpatialFilter());
              SQLQuery exportQuery;

              boolean compositeCalculation =   job.readParamCompositeMode() == Export.CompositeMode.CHANGES
                                            || job.readParamCompositeMode() == Export.CompositeMode.FULL_OPTIMIZED;

              CSVFormat pseudoCsvFormat = (  job.getCsvFormat() != PARTITIONED_JSON_WKB
                                           ? job.getCsvFormat()
                                           : ( job.getPartitionKey() == null || "tileid".equalsIgnoreCase(job.getPartitionKey()) ? TILEID_FC_B64 : PARTITIONID_FC_B64 )
                                          );

              switch ( pseudoCsvFormat ) {
                  case PARTITIONID_FC_B64:
                            exportQuery = generateFilteredExportQuery(client, schema, job.getTargetSpaceId(), propertyFilter, spatialFilter,
                                 job.getTargetVersion(), job.getParams(), job.getCsvFormat(), null,
                                 compositeCalculation , job.getPartitionKey(), job.getOmitOnNull(), false);
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
                                          SQLQuery q2 = buildPartIdVMLExportQuery(client, job, schema, s3Bucket, s3Path, s3Prefix, s3Region, compositeCalculation,
                                                  tCount > 1 ? new SQLQuery("AND i%% " + tCount + " = " + i) : null);
                                        String clientId = job.getTargetConnector();
                                        exportFutures.add(executeExportQuery(clientId, q2, job, s3Path));
                                      }
                                      return executeParallelExportAndCollectStatistics(job, promise, exportFutures);
                                  }
                                  catch (SQLException e) {
                                      logger.warn("job[{}] ", job.getId(), e);
                                      return Future.failedFuture(e);
                                  }
                              });

                  case TILEID_FC_B64:

                           exportQuery = generateFilteredExportQuery(client, schema, job.getTargetSpaceId(), propertyFilter, spatialFilter,
                              job.getTargetVersion(), job.getParams(), job.getCsvFormat(), null,
                              compositeCalculation , job.getPartitionKey(), job.getOmitOnNull(), false);
                      /*
                      Is used for incremental exports (tiles) - here we have to export modified tiles.
                      Those tiles we need to calculate separately
                       */
                      boolean isIncrementalExport =   job.isIncrementalMode()
                                                   || isIncrementalExportNonComposite(job.getCsvFormat(), job.getTargetVersion(), compositeCalculation) ;

                      final SQLQuery qkQuery = ( compositeCalculation || isIncrementalExport )
                              ? generateFilteredExportQueryForCompositeTileCalculation(client, schema, job.getTargetSpaceId(),
                              propertyFilter, spatialFilter, job.getTargetVersion(), job.getParams(), job.getCsvFormat(), isIncrementalExport)
                              : null;

                      return calculateTileListForVMLExport(job, schema, exportQuery, qkQuery)
                              .compose(tileList -> {
                                  try {
                                       Promise<Export.ExportStatistic> promise = Promise.promise();
                                       List<Future> exportFutures = new ArrayList<>();
                                       job.setProcessingList(tileList);

                                       for (int i = 0; i < tileList.size(); i++) {
                                          /** Build export for each tile of the weighted tile list */
                                          SQLQuery q2 = buildVMLExportQuery(client, job, schema, s3Bucket, s3Path, s3Region, tileList.get(i), qkQuery);

                                          exportFutures.add(executeExportQuery(job.getTargetConnector(), q2, job, s3Path));

                                       }

                                       return executeParallelExportAndCollectStatistics(job, promise, exportFutures);
                                      }
                                      catch (SQLException e) {
                                        logger.warn("job[{}] ", job.getId(), e);
                                        return Future.failedFuture(e);
                                      }
                             });

                            default:
                                exportQuery = generateFilteredExportQuery(client, schema, job.getTargetSpaceId(), propertyFilter, spatialFilter,
                                        job.getTargetVersion(), job.getParams(), job.getCsvFormat(), compositeCalculation);
                                return calculateThreadCountForDownload(job, schema, exportQuery)
                                        .compose(threads -> {
                                            try {
                                                Promise<Export.ExportStatistic> promise = Promise.promise();
                                                List<Future> exportFutures = new ArrayList<>();

                                                for (int i = 0; i < threads; i++) {
                                                    String s3Prefix = i + "_";
                                                    SQLQuery q2 = buildS3ExportQuery(client, job, schema, s3Bucket, s3Path, s3Prefix, s3Region, compositeCalculation,
                                                            (threads > 1 ? new SQLQuery("AND i%% " + threads + " = " + i) : null));
                                                  String clientId = job.getTargetConnector();
                                                  exportFutures.add(executeExportQuery(clientId, q2, job, s3Path));
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

    private static Future<ExportStatistic> executeParallelExportAndCollectStatistics(Export j, Promise<ExportStatistic> promise, List<Future> exportFutures) {
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

    private Future<Integer> calculateThreadCountForDownload(Export j, String schema, SQLQuery exportQuery) {
      //Currently we are ignoring filters and count only all features
      SQLQuery q = new SQLQuery("SELECT /* s3_export_hint m499#jobId(" + j.getId() + ") */ ${schema}.exp_type_download_precalc("
          + "#{estimated_count}, ${{exportSelectString}}, #{tbl}::regclass) AS thread_cnt")
          .withVariable("schema", schema)
          .withNamedParameter("estimated_count", j.getEstimatedFeatureCount())
          .withQueryFragment("exportSelectString", exportQuery)
          .withNamedParameter("tbl", schema + ".\"" + j.getTargetTable() + "\"");
      logger.info("job[{}] Calculate S3-Export {}: {}", j.getId(), j.getTargetSpaceId(), q.text());

      return getClient(j.getTargetConnector())
          .compose(client -> client.run(q, rs -> rs.next() ? rs.getInt("thread_cnt") : null, true))
          .compose(threadCount -> threadCount == null
              ? Future.failedFuture("Error calculating thread count for export for " + j.getId())
              : Future.succeededFuture(threadCount));
    }

    private Future<List<String>> calculateTileListForVMLExport(Export j, String schema, SQLQuery exportQuery, SQLQuery qkQuery) {
      if (j.getProcessingList() != null)
        return Future.succeededFuture(j.getProcessingList());

      SQLQuery q = new SQLQuery("SELECT tilelist /* vml_export_hint m499#jobId(" + j.getId() + ") */ FROM ${schema}.exp_type_vml_precalc(" +
          "#{htile}, '', #{mlevel}, ${{exportSelectString}}, ${{qkQuery}}, #{estimated_count}, #{tbl}::regclass)")
          .withVariable("schema", schema)
          .withNamedParameter("htile", true)
          .withNamedParameter("idk", "''")
          .withNamedParameter("mlevel", j.getTargetLevel() != null ? Math.max(j.getTargetLevel() - 1, 3) : 11)
          .withQueryFragment("exportSelectString", exportQuery)
          .withQueryFragment("qkQuery", qkQuery == null ? new SQLQuery("null::text") : qkQuery)
          .withNamedParameter("estimated_count", j.getEstimatedFeatureCount())
          .withNamedParameter("tbl", schema + ".\"" + j.getTargetTable() + "\"");
      logger.info("job[{}] Calculate VML-Export {}: {}", j.getId(), j.getTargetSpaceId(), q.text());

      return getClient(j.getTargetConnector())
          .compose(client -> client.run(q, rs -> {
            if (rs.next()) {
              Array sqlTiles = rs.getArray("tilelist");
              if (sqlTiles != null) {
                String[] tiles = (String[]) sqlTiles.getArray();
                if (tiles != null)
                  return Arrays.asList(tiles);
              }
            }
            return null;
          }));
    }

    private Future<Export.ExportStatistic> executeExportQuery(String clientId, SQLQuery q, Export j , String s3Path) {
        logger.info("job[{}] Execute S3-Export {}->{} {}", j.getId(), j.getTargetSpaceId(), s3Path, q.text());
        return getClient(clientId)
          .compose(client -> client.run(q, rs -> {
          ExportStatistic es = new Export.ExportStatistic();
          if (rs.next())
            return new Export.ExportStatistic()
                .addRows(rs.getLong("rows_uploaded"))
                .addFiles(rs.getLong("files_uploaded"))
                .addBytes(rs.getLong("bytes_uploaded"));
          return es;
        }, true));
    }

  public SQLQuery buildS3ExportQuery(JdbcClient client, Export j, String schema,
                                              String s3Bucket, String s3Path, String s3FilePrefix, String s3Region,
                                              boolean isForCompositeContentDetection, SQLQuery customWhereCondition) throws SQLException {

        String propertyFilter = (j.getFilters() == null ? null : j.getFilters().getPropertyFilterAsString());
        SpatialFilter spatialFilter= (j.getFilters() == null ? null : j.getFilters().getSpatialFilter());

        s3Path = s3Path+ "/" +(s3FilePrefix == null ? "" : s3FilePrefix)+"export";
        SQLQuery exportSelectString = generateFilteredExportQuery(client, schema, j.getTargetSpaceId(), propertyFilter, spatialFilter,
                j.getTargetVersion(), j.getParams(), j.getCsvFormat(), customWhereCondition, isForCompositeContentDetection,
                j.getPartitionKey(), j.getOmitOnNull(), false);

        String options = "'format csv,delimiter '','', encoding ''UTF8'', quote  ''\"'', escape '''''''' '";
        if(j.getCsvFormat().equals(GEOJSON)){
            options = " 'FORMAT TEXT, ENCODING ''UTF8'' '";
            s3Path += ".geojson";
        }else
            s3Path += ".csv";

        SQLQuery q = new SQLQuery("SELECT * /* s3_export_hint m499#jobId(" + j.getId() + ") */ from aws_s3.query_export_to_s3( "+
                " ${{exportSelectString}},"+
                " aws_commons.create_s3_uri(#{s3Bucket}, #{s3Path}, #{s3Region}),"+
                " options := "+options+");"
        );

        q.setQueryFragment("exportSelectString", exportSelectString);
        q.setNamedParameter("s3Bucket",s3Bucket);
        q.setNamedParameter("s3Path",s3Path);
        q.setNamedParameter("s3Region",s3Region);

        return q;
    }

    private SQLQuery buildPartIdVMLExportQuery(JdbcClient client, Export j, String schema, String s3Bucket, String s3Path, String s3FilePrefix,
        String s3Region, boolean isForCompositeContentDetection, SQLQuery customWhereCondition) throws SQLException {
        //Generic partition
        String propertyFilter = (j.getFilters() == null ? null : j.getFilters().getPropertyFilterAsString());
        SpatialFilter spatialFilter= (j.getFilters() == null ? null : j.getFilters().getSpatialFilter());

        s3Path = s3Path+ "/" +(s3FilePrefix == null ? "" : s3FilePrefix)+"export.csv";

        SQLQuery exportSelectString = generateFilteredExportQuery(client, schema, j.getTargetSpaceId(), propertyFilter, spatialFilter,
                j.getTargetVersion(), j.getParams(), j.getCsvFormat(), customWhereCondition, isForCompositeContentDetection,
                j.getPartitionKey(), j.getOmitOnNull(), false);

        SQLQuery q = new SQLQuery("SELECT * /* vml_export_hint m499#jobId(" + j.getId() + ") */ from aws_s3.query_export_to_s3( "+
                " ${{exportSelectString}},"+
                " aws_commons.create_s3_uri(#{s3Bucket}, #{s3Path}, #{s3Region}),"+
                " options := 'format csv,delimiter '','', encoding ''UTF8'', quote  ''\"'', escape '''''''' ' );"
        );

        q.setQueryFragment("exportSelectString", exportSelectString);
        q.setNamedParameter("s3Bucket",s3Bucket);
        q.setNamedParameter("s3Path",s3Path);
        q.setNamedParameter("s3Region",s3Region);

        return q;
    }

    private SQLQuery buildVMLExportQuery(JdbcClient client, Export j, String schema, String s3Bucket, String s3Path, String s3Region, String parentQk,
        SQLQuery qkTileQry) throws SQLException {
        //Tiled export
        String propertyFilter = (j.getFilters() == null ? null : j.getFilters().getPropertyFilterAsString());
        SpatialFilter spatialFilter= (j.getFilters() == null ? null : j.getFilters().getSpatialFilter());

        int maxTilesPerFile = j.getMaxTilesPerFile() == 0 ? 4096 : j.getMaxTilesPerFile();

       /* incremental -> for tiled export the exportSelect should be crafted like query by "toVersion" query.*/
        String targetVersion = j.getTargetVersion();
        if (HEAD.equals(targetVersion))
          targetVersion = null;
        if (targetVersion != null)
        { Ref ref = new Ref(targetVersion);
          if( ref.isRange() )
           targetVersion = "" + ref.getEnd().getVersion();
        }
       /* incremental */

        SQLQuery exportSelectString =  generateFilteredExportQuery(client, schema, j.getTargetSpaceId(), propertyFilter, spatialFilter,
            targetVersion, j.getParams(), j.getCsvFormat());

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

        return q;
    }

    private SQLQuery generateFilteredExportQuery(JdbcClient client, String schema, String spaceId, String propertyFilter,
        SpatialFilter spatialFilter, String targetVersion, Map params, CSVFormat csvFormat) throws SQLException {
        return generateFilteredExportQuery(client, schema, spaceId, propertyFilter, spatialFilter, targetVersion, params, csvFormat, null, false, null, false, false);
    }

    private SQLQuery generateFilteredExportQuery(JdbcClient client, String schema, String spaceId, String propertyFilter,
                                                        SpatialFilter spatialFilter, String targetVersion, Map params, CSVFormat csvFormat, boolean isForCompositeContentDetection) throws SQLException {
        return generateFilteredExportQuery(client, schema, spaceId, propertyFilter, spatialFilter, targetVersion, params, csvFormat, null, isForCompositeContentDetection, null, false,false);
    }


    private SQLQuery generateFilteredExportQueryForCompositeTileCalculation(JdbcClient client, String schema, String spaceId, String propertyFilter,
                                                        SpatialFilter spatialFilter, String targetVersion, Map params, CSVFormat csvFormat, boolean isIncrementalExport) throws SQLException {
        return generateFilteredExportQuery(client, schema, spaceId, propertyFilter, spatialFilter, targetVersion, params, csvFormat, null, true, null, false, isIncrementalExport);
    }

    private SQLQuery generateFilteredExportQuery(JdbcClient client, String schema, String spaceId, String propertyFilter,
        SpatialFilter spatialFilter, String targetVersion, Map params, CSVFormat csvFormat, SQLQuery customWhereCondition,
        boolean isForCompositeContentDetection, String partitionKey, Boolean omitOnNull, boolean isIncrementalExport
        )
        throws SQLException {

        if( isIncrementalExport ) // in this case, behave like "isForCompositeContentDetection" for tile calculations
         isForCompositeContentDetection = true;

        csvFormat = (( csvFormat == PARTITIONED_JSON_WKB && ( partitionKey == null || "tileid".equalsIgnoreCase(partitionKey)) ) ? TILEID_FC_B64 : csvFormat );

        GetFeaturesByGeometryEvent event = new GetFeaturesByGeometryEvent();
        event.setSpace(spaceId);
        event.setParams(params);

        if (params != null && params.get("enableHashedSpaceId") != null)
            event.setConnectorParams(new HashMap<>(){{put("enableHashedSpaceId", params.get("enableHashedSpaceId"));}});

        if (params != null && params.get("versionsToKeep") != null)
            event.setVersionsToKeep((int)params.get("versionsToKeep"));

        Map<String,Object> extStashed = null;
        if (params != null && params.get(Export.PARAM_CONTEXT) != null) {
            ContextAwareEvent.SpaceContext context = ContextAwareEvent.SpaceContext.of(params.get(Export.PARAM_CONTEXT).toString());

            //TODO: Remove the following hack and perform the switch to super within connector (GetFeatures QR) instead
            if (context == SUPER) {
                //switch to super space
                Map<String,Object> ext = (Map<String, Object>) params.get("extends");
                if (ext != null) {
                    String superSpace = (String) ext.get("spaceId");
                    if (superSpace != null)
                        event.setSpace(superSpace);

                    if( !ext.containsKey("extends") )
                    { extStashed = ext;
                      params.remove("extends"); // needs to be removed and restored later on s.'DS-587'
                                                    // except in case of L2 extends
                    }
                }
                context = ContextAwareEvent.SpaceContext.DEFAULT;
            }
            event.setContext(context);
        }

        if (targetVersion != null)
            event.setRef(new Ref(targetVersion));

        if (propertyFilter != null) {
            PropertiesQuery propertyQueryLists = PropertiesQuery.fromString(propertyFilter, "", false);
            event.setPropertiesQuery(propertyQueryLists);
        }

        if (spatialFilter != null) {
            event.setGeometry(spatialFilter.getGeometry());
            event.setRadius(spatialFilter.getRadius());
            event.setClip(spatialFilter.isClipped());
        }

        boolean partitionByPropertyValue = ((csvFormat == PARTITIONID_FC_B64 || csvFormat == PARTITIONED_JSON_WKB) && partitionKey != null && !"id".equalsIgnoreCase(partitionKey)),
                partitionByFeatureId     = ((csvFormat == PARTITIONID_FC_B64 || csvFormat == PARTITIONED_JSON_WKB) && !partitionByPropertyValue ),
                downloadAsJsonWkb        = ( csvFormat == JSON_WKB );

        SpaceContext ctxStashed = event.getContext();

        if (isForCompositeContentDetection)
            event.setContext( (partitionByFeatureId || downloadAsJsonWkb) ? EXTENSION : COMPOSITE_EXTENSION);

        SQLQuery contentQuery,
                 contentQueryByPropertyValue = null;

      try {
        final ExportSpace queryRunner = getQueryRunner(client, spatialFilter, event);

        if (customWhereCondition != null && (csvFormat != PARTITIONID_FC_B64 || partitionByFeatureId))
          queryRunner.withCustomWhereClause(customWhereCondition);

        contentQuery = queryRunner
              .withGeoOverride(buildGeoFragment(spatialFilter))
              .buildQuery(event);

          if (partitionByPropertyValue && isForCompositeContentDetection) {
            event.setContext(ctxStashed);
            contentQueryByPropertyValue = getQueryRunner(client, spatialFilter, event)
                .withGeoOverride(buildGeoFragment(spatialFilter))
                .buildQuery(event);
          }
        }
        catch (Exception e) {
          throw new SQLException(e);
        }

        if( extStashed != null ) // restore saved extends -> 'DS-587'
         params.put("extends", extStashed );

        char cFlag = isForCompositeContentDetection ? 'C' : 'P'; // fix/prevent name clash for namedparameter during export

        switch (csvFormat) {
          case GEOJSON :
          {
            SQLQuery geoJson = new SQLQuery("select jsondata || jsonb_build_object('geometry', ST_AsGeoJSON(geo, 8)::jsonb) "
                + "from (${{contentQuery}}) X")
                .withQueryFragment("contentQuery", contentQuery);
            return queryToText(geoJson,"va" + cFlag );
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
                       /*TODO:  "st_geomfromtext(st_astext(geo,8))" conversion is a tmp solution for export of wkb, should be removed on later releases when all geom in db are aligned to 8 digit prec. */
                            : ( isForCompositeContentDetection
                              ? "select jsondata->>'id' as id, "
                              + " case not coalesce((jsondata#>'{properties,@ns:com:here:xyz,deleted}')::boolean,false) when true then jsondata else null::jsonb end as jsondata,"
                              + " st_geomfromtext(st_astext(geo,8),4326) as geo "
                              + "from ( ${{contentQuery}}) X"
                              : "select jsondata->>'id' as id, jsondata, st_geomfromtext(st_astext(geo,8),4326) as geo "
                              + "from ( ${{contentQuery}}) X" );

           if( partitionByPropertyValue )
           {
              String converted = PropertiesQuery.getConvertedKey(partitionKey);
              partitionKey =  String.join("'->'",(converted != null ? converted : partitionKey).split("\\."));
              //TODO: Simplify / structure the following query blob
              partQry =
                 " with  "
                +" plist as  "
                +" ( select o.* from "
                +"   ( select ( dense_rank() over (order by key) )::integer as i, key  "
                +"     from "
                +"     ( select coalesce( key, '\"CSVNULL\"'::jsonb) as key "
                +"       from ( select distinct jsondata->'${{partitionKey}}' as key from ( ${{contentQuery}} ) X "+ (( omitOnNull == null || !omitOnNull ) ? "" : " where not jsondata->'${{partitionKey}}' isnull " ) +" ) oo "
                +"     ) d1"
                +"   ) o "
                +"   where 1 = 1 ${{customWhereCondition}}"
                +" ), "
                +" iidata as  "
                +" ( select l.key, (( row_number() over ( partition by l.key ) )/ 20000000)::integer as chunk, r.jsondata, r.geo from (${{contentQueryReal}}) r right join plist l on ( coalesce( r.jsondata->'${{partitionKey}}', '\"CSVNULL\"'::jsonb) = l.key )  "
                +" ), "
                + ( csvFormat == PARTITIONID_FC_B64 //TODO: Generalize / structure the following two fragments and re-use similar parts
                   ? " iiidata as  "
                    +" ( select coalesce( ('[]'::jsonb || key)->>0, 'CSVNULL' ) as id, (count(1) over ()) as nrbuckets, count(1) as nrfeatures, replace( encode(convert_to(('{\"type\":\"FeatureCollection\",\"features\":[' || coalesce( string_agg( (jsondata || jsonb_build_object('geometry',st_asgeojson(geo,8)::jsonb))::text, ',' ), null::text ) || ']}'),'UTF8'),'base64') ,chr(10),'') as data "
                    +"   from iidata group by 1, chunk order by 1, 3 desc   "
                    +" )   "
                    +" select id, data from iiidata "
                   : " iiidata as  "
                    +" ( select coalesce( ('[]'::jsonb || key)->>0, 'CSVNULL' ) as id, jsondata, geo "
                    +"   from iidata "
                    +" )   "
                    +" select id, jsondata, st_geomfromtext(st_astext(geo,8),4326) as geo from iiidata "
                  );
           }

          SQLQuery geoJson = new SQLQuery(partQry)
              .withQueryFragment("partitionKey", partitionKey)
              .withQueryFragment("contentQueryReal", contentQueryByPropertyValue != null ? contentQueryByPropertyValue : contentQuery)
              .withQueryFragment("customWhereCondition", partitionByPropertyValue && customWhereCondition != null ? customWhereCondition : new SQLQuery(""))
              .withQueryFragment("contentQuery", contentQuery);

           return queryToText(geoJson,"vb" + cFlag);
         }

            default:
            {
              // JSON_WKB, DOWNLOAD
              /*TODO:  "st_geomfromtext(st_astext(geo,8))" conversion is a tmp solution for export of wkb, should be removed on later releases when all geom in db are aligned to 8 digit prec. */
              contentQuery = new SQLQuery("SELECT jsondata, st_geomfromtext(st_astext(geo,8),4326) as geo FROM (${{innerContentQuery}}) contentQuery")
                  .withQueryFragment("innerContentQuery", contentQuery);
                return queryToText(contentQuery,"vc" + cFlag);
            }
        }

    }

  private static SQLQuery buildGeoFragment(SpatialFilter spatialFilter) {
    if (spatialFilter != null && spatialFilter.isClipped()) {
     if( spatialFilter.getRadius() != 0 )
      return new SQLQuery("ST_Intersection(ST_MakeValid(geo), ST_Buffer(st_force3d(ST_GeomFromText(#{wktGeometry}))::geography, #{radius})::geometry) as geo")
          .withNamedParameter("wktGeometry", WKTHelper.geometryToWKT2d(spatialFilter.getGeometry()))
          .withNamedParameter("radius", spatialFilter.getRadius());
     else
      return new SQLQuery("ST_Intersection(ST_MakeValid(geo), st_setsrid(st_force3d( ST_GeomFromText( #{wktGeometry} )),4326 )) as geo")
          .withNamedParameter("wktGeometry", WKTHelper.geometryToWKT2d(spatialFilter.getGeometry()));
    }
    else
        return new SQLQuery("geo");
  }

  private static ExportSpace getQueryRunner(JdbcClient client, SpatialFilter spatialFilter, GetFeaturesByGeometryEvent event)
      throws SQLException, ErrorResponseException {
    ExportSpace queryRunner;
    if (spatialFilter == null)
      queryRunner = new ExportSpaceByProperties(event);
    else
      queryRunner = new ExportSpaceByGeometry(event);

    queryRunner.setDataSourceProvider(client.getDataSourceProvider());
    return queryRunner;
  }

  /**
   * @deprecated This method is deprecated, please do not use it for new implementations
   * @param q
   * @return
   */
    @Deprecated
    private static SQLQuery queryToText(SQLQuery q,String prefixParamName) {
        /*
      FIXME:
       The following works only for very specific kind of queries, do not try to compile dynamic queries in software,
       but inside SQL functions directly and pass the parameters as function-arguments instead
       */
        String queryText = q.substitute().text()
            .replace("?", "%L")
            .replace("'","''");

        int i = 0;
        String formatArgs = "";
        Map<String, Object> newParams = new HashMap<>();
        for (Object paramValue : q.parameters()) {
            String paramName = prefixParamName + i++;
            formatArgs += ",(#{" + paramName + "}::" + getType(paramValue) + ")";
            newParams.put(paramName, paramValue);
        }

      SQLQuery sq = new SQLQuery(String.format("format('%s'%s)", queryText, formatArgs))
          .withNamedParameters(newParams);
      return sq;
    }

  /**
   * @deprecated See above
   * @param o
   * @return
   */
  @Deprecated
    private static String getType(Object o) {
      if (o instanceof String)
        return "TEXT";
      if (o instanceof Long)
        return "BIGINT";
      if (o instanceof Integer)
        return "INT";
      if (o instanceof String[])
        return "TEXT[]";
      throw new RuntimeException("Unknown type: " + o.getClass().getSimpleName());
    }
}

