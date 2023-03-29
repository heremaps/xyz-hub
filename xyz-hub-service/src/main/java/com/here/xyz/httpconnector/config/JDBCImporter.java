/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.jobs.Job.CSVFormat;
import com.here.xyz.httpconnector.util.status.RDSStatus;
import com.here.xyz.httpconnector.util.status.RunningQueryStatistic;
import com.here.xyz.httpconnector.util.status.RunningQueryStatistics;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.query.ModifySpace;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.sqlclient.impl.ArrayTuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Client for handle IMPORT-Jobs (S3 -> RDS)
 */
public class JDBCImporter extends JDBCClients{
    private static final Logger logger = LogManager.getLogger();

    /** Names of default Indices */
    private static final String IDX_NAME_ID = "id";
    private static final String IDX_NAME_GEO = "geo";
    private static final String IDX_NAME_CREATEDAT = "createdAt";
    private static final String IDX_NAME_UPDATEDAT = "updatedAt";
    private static final String IDX_NAME_SERIAL = "serial";
    private static final String IDX_NAME_TAGS = "tags";
    private static final String IDX_NAME_VIZ = "viz";

    private static final String IDX_NAME_ID_NEW = "idnew";
    private static final String IDX_NAME_VERSION = "version";
    private static final String IDX_NAME_ID_VERSION = "idversion";
    private static final String IDX_NAME_OPERATION = "operation";
    private static final String IDX_NAME_AUTHOR = "author";

    /** Temporally used during migration phase */
    private static final Boolean OLD_DATABASE_LAYOUT =  (CService.configuration !=null && CService.configuration.JOB_OLD_DATABASE_LAYOUT != null)
            ? CService.configuration.JOB_OLD_DATABASE_LAYOUT : false;

    /** List of default Indices */
    public static final String[] DEFAULT_IDX_LIST = { IDX_NAME_ID,IDX_NAME_GEO,IDX_NAME_CREATEDAT,IDX_NAME_UPDATEDAT,IDX_NAME_SERIAL,IDX_NAME_TAGS,IDX_NAME_VIZ };

    /** List of version related default Indices */
    public static final String[] DEFAULT_IDX_LIST_EXTENDED = { IDX_NAME_ID,IDX_NAME_GEO,IDX_NAME_CREATEDAT,IDX_NAME_UPDATEDAT,IDX_NAME_SERIAL,IDX_NAME_TAGS,IDX_NAME_VIZ,
            IDX_NAME_ID_NEW,IDX_NAME_VERSION,IDX_NAME_ID_VERSION,IDX_NAME_OPERATION,IDX_NAME_AUTHOR };

    /**
     * Prepare Table for Import
     */
    public static Future<Void> prepareImport(String schema, Import job){

        return increaseVersionSequence(job.getTargetConnector(), schema, job.getTargetTable())
                .compose( version -> {
                    job.setSpaceVersion(version);
                    if(version > 1){
                        /** Abort - if increased version is not 0 => Layer is not empty */
                        return Future.failedFuture("SequenceNot0");
                    }

                    logger.info("Start IDX-List of: {}", job.getTargetTable());
                    return listIndices(job.getTargetConnector(), schema, job.getTargetTable())
                            .compose(idxList -> {
                                logger.info("Start deletion of IDX-List: {}", idxList);
                                return dropIndices(job.getTargetConnector(), schema, job.getTargetTable(), idxList)
                                        .compose(f2 -> {
                                            logger.info("Start creation of Import-Trigger {}", getViewName(job.getTargetTable()));
                                            return createTriggerOnTargetTable(job, schema)
                                                    .compose(f3 -> Future.succeededFuture());
                                        });
                            });
                });
    }

    public static Future<List<String>> listIndices(String clientID, String schema, String tablename){
        SQLQuery q = new SQLQuery("select * from xyz_index_list_all_available(#{schema},#{table});");
        q.setNamedParameter("schema",schema);
        q.setNamedParameter("table",tablename);
        q.substitute();
        q = q.substituteAndUseDollarSyntax(q);

        return getClient(clientID)
                .preparedQuery(q.text())
                .execute(new ArrayTuple(q.parameters()))
                .map(rows -> {
                    List<String> idxList = new ArrayList<>();
                    rows.forEach(row -> idxList.add(row.getString(0)));
                    return idxList;
                });
    }

    public static Future<Void> dropIndices(String clientID, String schema, String tablename, List<String> idxList){
        if(idxList.size() == 0)
            return Future.succeededFuture();

        SQLQuery q = new SQLQuery("");

        for (String idx : idxList) {
            q.append("DROP INDEX IF EXISTS ${schema}.\""+idx+"\" CASCADE;");
        }
        q.setVariable("schema", schema);

        return getClient(clientID).query(q.substitute().text())
                .execute()
                .map(f->null);
    }

    public static Future<String> createTriggerOnTargetTable(Job job, String schema){
        String author = job.getAuthor() == null ? "ANONYMOUS" : job.getAuthor();
        boolean addTablenameToXYZNs = false;

        /** deprecated */
        boolean _enableUUID  = job.getParam("enableUUID") == null ? false : (boolean) job.getParam("enableUUID");

        SQLQuery q = new SQLQuery("CREATE OR REPLACE TRIGGER insertTrigger BEFORE INSERT ON ${schema}.${tablename} \n"
                + "FOR EACH ROW EXECUTE PROCEDURE ${schema}.xyz_import_trigger('{trigger_hrn}',{addTableName},{enableUUID},{spaceVersion},{author},{oldLayout});"
                .replace("{trigger_hrn}","TBD")
                .replace("{addTableName}", Boolean.toString(addTablenameToXYZNs))
                .replace("{enableUUID}", Boolean.toString(_enableUUID))
                .replace("{spaceVersion}", Long.toString( job.getSpaceVersion()))
                .replace("{author}", author)
                .replace("{oldLayout}", Boolean.toString(OLD_DATABASE_LAYOUT))
        );

        q.setVariable("schema", schema);
        q.setVariable("tablename", job.getTargetTable());

        return getClient(job.getTargetConnector())
                .query(q.substitute().text())
                .execute()
                .map(job.getTargetTable());
    }

    public static Future<Long> increaseVersionSequence(String clientID, String schema, String tablename){
        SQLQuery q = new SQLQuery("SELECT nextval('${schema}.${table}');");
        q.setVariable("schema", schema);
        q.setVariable("table", tablename+"_version_seq");

        return getClient(clientID).query(q.substitute().text())
                .execute()
                .map(row -> row.iterator().next().getLong(0));
    }

    public static Future<Void> doesTableExists(String clientID, String schema, String tablename){
        SQLQuery q = new SQLQuery("SELECT 1 FROM ${schema}.${table} limit 1;");
        q.setVariable("schema", schema);
        q.setVariable("table", tablename);

        return getClient(clientID).query(q.substitute().text())
                .execute()
                .map(f->null);
    }

    public static Future<Long> isTableEmpty(String clientID, String schema, String tablename){
        SQLQuery q = new SQLQuery("SELECT count(1) FROM ${schema}.${table};");
        q.setVariable("schema", schema);
        q.setVariable("table", tablename);

        return getClient(clientID).query(q.substitute().text())
                .execute()
                .map(row -> row.iterator().next().getLong(0));
    }

    /**
     * Import Data from S3
     */
    public static Future<String> executeImport(String clientID, String schema, String tablename, String s3Bucket, String s3Path, String s3Region, long curFileSize, CSVFormat csvFormat){
        SQLQuery q = new SQLQuery(("SELECT aws_s3.table_import_from_s3( "
                + "'${schema}.${table}', "
                + "#{columns}, "
                + " 'DELIMITER '','' CSV ENCODING  ''UTF8'' QUOTE  ''\"'' ESCAPE '''''''' ', "
                + " aws_commons.create_s3_uri( "
                + "     #{s3Bucket}, "
                + "     #{s3Path}, "
                + "     #{s3Region}"
                + " )),'{iml_import_hint}' as iml_import_hint")
                    /** Need replace, because it is not possible to read a Parameter as hint */
                    .replace("{iml_import_hint}", s3Path+":"+curFileSize));

        q.setNamedParameter("s3Bucket",s3Bucket);
        q.setNamedParameter("s3Path",s3Path);
        q.setNamedParameter("s3Region",s3Region);
        q.setNamedParameter("columns",csvFormat.equals(CSVFormat.GEOJSON) ? "jsondata" : "jsondata,geo");


        q.setVariable("table", tablename);
        q.setVariable("schema", schema);
        q = q.substituteAndUseDollarSyntax(q);

        logger.info("Execute S3-Import {}->{} {}", tablename, s3Path, q.text());
        return getClient(clientID)
                .preparedQuery(q.text())
                .execute(new ArrayTuple(q.parameters()))
                .map(row -> row.iterator().next().getString(0));
    }


    public static List<Future> generateIndexFutures (Job job, String schema, String tableName, String clientId){
        List<Future> indicesFutures = new ArrayList<>();

        for (String idxName:  (OLD_DATABASE_LAYOUT ? DEFAULT_IDX_LIST : DEFAULT_IDX_LIST_EXTENDED)) {
            /** Create VIZ index only on root table - to workaround potential postgres bug */
            if(idxName.equalsIgnoreCase(IDX_NAME_VIZ) && (tableName.indexOf("_head") != -1 || tableName.indexOf("_p0") != -1))
                continue;

            SQLQuery q = createIdxQuery(idxName, schema, tableName);
            indicesFutures.add(createIndex(clientId, q, idxName)
                    .onSuccess(idx -> {
                        logger.info("IDX creation of '{}' succeeded!", idx);
                        ((Import)job).addIdx(idxName+"@"+tableName);
                    }).onFailure(e -> {
                        logger.warn("IDX creation '{}' failed! ",idxName+"@"+tableName,e);
                        job.setErrorDescription(Import.ERROR_DESCRIPTION_IDX_CREATION_FAILED);
                    }));
        }
        return indicesFutures;
    }
    /**
     * Finalize Import - create Indices and drop temporary resources
     */
    public static Future<Void> finalizeImport(Job job, String schema){
        String clientId = job.getTargetConnector();
        String tableName = job.getTargetTable();

        Promise<Void> p = Promise.promise();

        /** Current workaround to avoid "duplicate key value violates unique constraint"-error, which
         * get caused due to a potential bug inside postgres partitioning. This happens during
         * a parallel index creation where indexNames on partitions are getting created with
         * conflicting names e.g.: "3b32d555c6a2eb07009fbf382564d9e1_p0_expr_expr1_idx"
         * */
        List<Future> indicesFuturesHead = generateIndexFutures(job, schema, tableName+"_head", clientId);

        CompositeFuture.join(indicesFuturesHead).onComplete(
                t-> {
                    List<Future> indicesFuturesP0 = generateIndexFutures(job, schema, tableName+"_p0", clientId);
                    CompositeFuture.join(indicesFuturesP0).onComplete(
                            t2-> {
                                List<Future> indicesFuturesRoot = generateIndexFutures(job, schema, tableName, clientId);
                                CompositeFuture.join(indicesFuturesRoot).onComplete(
                                        t3-> {
                                            deleteImportTrigger(clientId, schema, tableName)
                                                .onComplete(
                                                        f-> {
                                                            markMaintenance(clientId, schema, tableName);
                                                            p.complete();
                                                        }
                                                );
                                        }
                                );
                            }
                    );
                }
        ).onFailure(e -> {
            logger.warn("Table cleanup has failed",e);
            if(job.getErrorDescription() == null)
                job.setErrorDescription(Import.ERROR_DESCRIPTION_TABLE_CLEANUP_FAILED);
            p.fail("Table cleanup has failed");
        });
        return p.future();
    }

    public static Future<Void> deleteView(String clientID, String schema, String tableName){
        SQLQuery q = new SQLQuery("DROP VIEW IF EXISTS ${schema}.${viewname};");

        q.setVariable("schema",schema);
        q.setVariable("viewname", getViewName(tableName));

        logger.info("Delete view {}",getViewName(tableName));

        return getClient(clientID).query(q.substitute().text())
                .execute()
                .map(f -> null);
    }

    public static Future<Void> deleteImportTrigger(String clientID, String schema, String tableName){
        SQLQuery q = new SQLQuery("DROP TRIGGER IF EXISTS insertTrigger ON ${schema}.${tablename};");
        q.setVariable("schema", schema);
        q.setVariable("tablename", tableName);

        return getClient(clientID).query(q.substitute().text())
                .execute()
                .map(f -> null);
    }

    public static Future<Void> markMaintenance(String clientID, String schema, String spaceId){
        SQLQuery q = new SQLQuery("UPDATE "+ModifySpace.IDX_STATUS_TABLE_FQN
                + " SET idx_creation_finished = #{markAs} "
                + " WHERE spaceid=#{spaceId} AND schem=#{schema};");

        q.setNamedParameter("schema", schema);
        q.setNamedParameter("spaceId", spaceId);
        q.setNamedParameter("markAs", false);

        q = q.substituteAndUseDollarSyntax(q);

        logger.info("Mark maintenance for {}",spaceId);
        return getClient(clientID)
                .preparedQuery(q.text())
                .execute(new ArrayTuple(q.parameters()))
                .map(f -> null);
    }

    public static SQLQuery createIdxQuery(String idxName, String schema, String tablename){
        SQLQuery q = null;

        switch (idxName){
            case IDX_NAME_ID:
                q = new SQLQuery("CREATE INDEX IF NOT EXISTS ${idx_name} ON ${schema}.${table} ((jsondata->>'id'));");
                q.setVariable("idx_name", "idx_"+tablename+"_"+IDX_NAME_ID);
                break;
            case IDX_NAME_GEO:
                q = new SQLQuery("CREATE INDEX IF NOT EXISTS ${idx_name} ON ${schema}.${table} USING gist ((geo));");
                q.setVariable("idx_name", "idx_"+tablename+"_"+IDX_NAME_GEO);
                break;
            case IDX_NAME_CREATEDAT:
                q = new SQLQuery("CREATE INDEX IF NOT EXISTS ${idx_name} ON ${schema}.${table} USING btree ((jsondata->'properties'->'@ns:com:here:xyz'->'createdAt'), (jsondata->>'id'));");
                q.setVariable("idx_name", "idx_"+tablename+"_"+IDX_NAME_CREATEDAT);
                break;
            case IDX_NAME_UPDATEDAT:
                q = new SQLQuery("CREATE INDEX IF NOT EXISTS ${idx_name} ON ${schema}.${table} USING btree ((jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt'), (jsondata->>'id'));");
                q.setVariable("idx_name", "idx_"+tablename+"_"+IDX_NAME_UPDATEDAT);
                break;
            case IDX_NAME_SERIAL:
                q = new SQLQuery("CREATE INDEX IF NOT EXISTS ${idx_name} ON ${schema}.${table} USING btree ((i));");
                q.setVariable("idx_name", "idx_"+tablename+"_"+IDX_NAME_SERIAL);
                break;
            case IDX_NAME_TAGS:
                q = new SQLQuery("CREATE INDEX IF NOT EXISTS ${idx_name} ON ${schema}.${table} USING gin ((jsondata->'properties'->'@ns:com:here:xyz'->'tags') jsonb_ops);");
                q.setVariable("idx_name", "idx_"+tablename+"_"+IDX_NAME_TAGS);
                break;
            case IDX_NAME_VIZ:
                q = new SQLQuery("CREATE INDEX IF NOT EXISTS ${idx_name} ON ${schema}.${table} USING btree (left( md5(''||i),5));");
                q.setVariable("idx_name", "idx_"+tablename+"_"+IDX_NAME_VIZ);
                break;

            case IDX_NAME_ID_NEW:
                q = new SQLQuery("CREATE INDEX IF NOT EXISTS ${idx_name} ON ${schema}.${table} USING btree (id);");
                q.setVariable("idx_name", "idx_"+tablename+"_"+IDX_NAME_ID_NEW);
                break;
            case IDX_NAME_VERSION:
                q = new SQLQuery("CREATE INDEX IF NOT EXISTS ${idx_name} ON ${schema}.${table} USING btree (version);");
                q.setVariable("idx_name", "idx_"+tablename+"_"+IDX_NAME_VERSION);
                break;
            case IDX_NAME_ID_VERSION:
                q = new SQLQuery("CREATE INDEX IF NOT EXISTS ${idx_name} ON ${schema}.${table} USING btree (id,version);");
                q.setVariable("idx_name", "idx_"+tablename+"_"+IDX_NAME_ID_VERSION);
                break;
            case IDX_NAME_AUTHOR:
                q = new SQLQuery("CREATE INDEX IF NOT EXISTS ${idx_name} ON ${schema}.${table} USING btree (author);");
                q.setVariable("idx_name", "idx_"+tablename+"_"+IDX_NAME_AUTHOR);
                break;
            case IDX_NAME_OPERATION:
                q = new SQLQuery("CREATE INDEX IF NOT EXISTS ${idx_name} ON ${schema}.${table} USING btree (operation);");
                q.setVariable("idx_name", "idx_"+tablename+"_"+IDX_NAME_OPERATION);
                break;
        }

        if(q != null){
            q.setVariable("schema", schema);
            q.setVariable("table", tablename);
            q.substitute();
        }
        return q;
    }

    public static Future<String> createIndex(String clientID, SQLQuery q, String idxName){
        return getClient(clientID).query(q.text())
                .execute()
                .map(idxName);
    }

    public static List<Future> createIndices(String clientID, String schema, String tablename){
        List<Future> futures = new ArrayList<>();

        for (String idxName: (OLD_DATABASE_LAYOUT ? DEFAULT_IDX_LIST :DEFAULT_IDX_LIST_EXTENDED)) {
            SQLQuery q = createIdxQuery(idxName, schema, tablename);
            futures.add(createIndex(clientID, q, idxName));
        }

        return futures;
    }

    public static Future<RunningQueryStatistics> collectRunningQueries(String clientID){
        SQLQuery q = new SQLQuery(
                "select '!ignore!' as ignore, datname,pid,state,backend_type,query_start,state_change,application_name,query from pg_stat_activity "
                        +"WHERE 1=1 "
                        +"AND application_name=#{applicationName}"
                        +"AND datname='postgres' "
                        +"AND state='active' "
                        +"AND backend_type='client backend'"
                        +"AND POSITION('!ignore!' in query) = 0"
        );

        q.setNamedParameter("applicationName", getApplicationName(clientID));
        q = q.substituteAndUseDollarSyntax(q);

        return getClient(clientID)
                .preparedQuery(q.text())
                .execute(new ArrayTuple(q.parameters()))
                .onFailure(f -> logger.warn(f))
                .map(rows -> {
                    RunningQueryStatistics statistics = new RunningQueryStatistics();
                    rows.forEach(
                            row -> statistics.addRunningQueryStatistic(new RunningQueryStatistic(
                                    row.getInteger("pid"),
                                    row.getString("state"),
                                    row.getLocalDateTime("query_start"),
                                    row.getLocalDateTime("state_change"),
                                    row.getString("query"),
                                    row.getString("application_name")
                            ))
                    );

                    return statistics;
                });
    }

    public static Future<RDSStatus> getRDSStatus(String clientId) {
        Promise<RDSStatus> p = Promise.promise();
        /** Collection current metrics from Cloudwatch */

        if(JDBCImporter.getClient(clientId) == null){
            logger.info("DB-Client not Ready!");
            p.complete(null);
        }else{
            JDBCImporter.collectRunningQueries(clientId)
                    .onSuccess(runningQueryStatistics -> {
                        /** Collect metrics from Cloudwatch */
                        JSONObject avg5MinRDSMetrics = CService.jobCWClient.getAvg5MinRDSMetrics(CService.rdsLookupDatabaseIdentifier.get(clientId));
                        p.complete(new RDSStatus(clientId, avg5MinRDSMetrics, runningQueryStatistics));
                    }).onFailure(f -> {
                        logger.warn("Cant get RDS-Resources {}",f);
                        p.fail(f);
                    } );
        }

        return p.future();
    }
}
