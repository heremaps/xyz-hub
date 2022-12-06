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
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.sqlclient.Tuple;
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

    /** List of Default Indices */
    public static final String[] DEFAULT_IDX_LIST = {IDX_NAME_ID,IDX_NAME_GEO,IDX_NAME_CREATEDAT,IDX_NAME_UPDATEDAT,IDX_NAME_SERIAL,IDX_NAME_TAGS,IDX_NAME_VIZ};

    /**
     * Prepare Table for Import
     */
    public static Future<Void> prepareImport(String clientID, String schema, String tablename, boolean enableUUID, CSVFormat csvFormat){

       return doesTableExists(clientID, schema, tablename)
               .compose( f1 -> {
                   logger.info("Start IDX-List of: {}", tablename);
                   return listIndices(clientID, schema, tablename)
                           .compose(idxList -> {
                               logger.info("Start deletion of IDX-List: {}", idxList);
                               return dropIndices(clientID, schema, tablename, idxList)
                                       .compose(f2 -> {
                                           logger.info("Start creation of Import-View {}", getViewName(tablename));
                                           return createTriggerOnTargetTable(clientID, schema, tablename, enableUUID, csvFormat)
                                                   .compose(f3 -> Future.succeededFuture());
                                       });
                           });
               });
    }

    public static Future<List<String>> listIndices(String clientID, String schema, String tablename){
        return getClient(clientID)
                .preparedQuery("select * from xyz_index_list_all_available($1,$2);")
                .execute(Tuple.of(schema,tablename))
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
            q.append("DROP INDEX IF EXISTS ${schema}.\""+idx+"\";");
        }
        q.setVariable("schema", schema);

        return getClient(clientID).query(q.substitute().text())
                .execute()
                .map(f->null);
    }

    public static Future<String> createViewAndTrigger(String clientID, String schema, String tablename){
        String viewName = getViewName(tablename);
        SQLQuery q = new SQLQuery("${{create_view}} ${{create_trigger}}");
        SQLQuery q1 = new SQLQuery("CREATE OR REPLACE VIEW ${schema}.${viewname} AS \n"
                + "SELECT \n"
                + "     t.jsondata as data, \n"
                + "     t.geo as geo \n"
                + "FROM ${schema}.${tablename} t;");
        SQLQuery q2 = new SQLQuery("CREATE TRIGGER insertTrigger INSTEAD OF INSERT ON ${schema}.${viewname} \n"
                + "FOR EACH ROW EXECUTE PROCEDURE xyz_viewTrigger_wkbv2('{trigger_hrn}','{trigger_schema}','{trigger_table}')"
                .replace("{trigger_hrn}","TBD")
                .replace("{trigger_schema}",schema)
                .replace("{trigger_table}",tablename));

        q.setQueryFragment("create_view",q1);
        q.setQueryFragment("create_trigger",q2);

        q.setVariable("viewname", viewName);
        q.setVariable("schema", schema);
        q.setVariable("tablename", tablename);
        q.setVariable("hrn", "TBD");

        logger.info("Create view and trigger for {}", tablename);

        return getClient(clientID).query(q.substitute().text())
                .execute()
                .map(viewName);
    }

    public static Future<String> createTriggerOnTargetTable(String clientID, String schema, String tablename, boolean enableUUID, CSVFormat csvFormat){
        boolean addTablenameToXYZNs = false;

        SQLQuery q = new SQLQuery("${{drop_trigger}} ${{create_trigger}}");
        SQLQuery q1 = new SQLQuery("DROP TRIGGER IF EXISTS insertTrigger ON ${schema}.${tablename};");
        SQLQuery q2 = new SQLQuery("CREATE TRIGGER insertTrigger BEFORE INSERT ON ${schema}.${tablename} \n"
                + "FOR EACH ROW EXECUTE PROCEDURE ${schema}.xyz_import_trigger('{trigger_hrn}',{addTableName},{enableUUID});"
                .replace("{trigger_hrn}","TBD")
                .replace("{enableUUID}", Boolean.toString(enableUUID))
                .replace("{addTableName}", Boolean.toString(addTablenameToXYZNs))
        );

        q.setQueryFragment("drop_trigger",q1);
        q.setQueryFragment("create_trigger",q2);

        q.setVariable("schema", schema);
        q.setVariable("tablename", tablename);
        q.setVariable("hrn", "TBD");

        return getClient(clientID).query(q.substitute().text())
                .execute()
                .map(tablename);
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
    public static Future<String> executeImportIntoView(String clientID, String schema, String tablename, String s3Bucket, String s3Path, String s3Region, long curFileSize, CSVFormat csvFormat){
        tablename = getViewName(tablename);
        return executeImport(clientID, schema, tablename, s3Bucket, s3Path, s3Region, curFileSize, csvFormat);
    }

    public static Future<String> executeImport(String clientID, String schema, String tablename, String s3Bucket, String s3Path, String s3Region, long curFileSize, CSVFormat csvFormat){

        SQLQuery q = new SQLQuery(("SELECT aws_s3.table_import_from_s3( \n"
                + "'${schema}.${table}', \n"
                + "'{columns}', \n"
                + " 'DELIMITER '','' CSV ENCODING  ''UTF8'' QUOTE  ''\"'' ESCAPE '''''''' ', \n"
                + " aws_commons.create_s3_uri( \n"
                + "     '{s3Bucket}', \n"
                + "     '{s3Path}', \n"
                + "     '{s3Region}' \n"
                + " )),'{s3Path}:{partSize}' as iml_import_hint")
                .replace("{partSize}",Long.toString(curFileSize))
                .replace("{s3Bucket}",s3Bucket)
                .replace("{s3Path}",s3Path)
                .replace("{s3Region}",s3Region)
                .replace("{columns}", csvFormat.equals(CSVFormat.GEOJSON) ? "jsondata" : "jsondata,geo"));

        q.setVariable("columns", tablename);
        q.setVariable("table", tablename);
        q.setVariable("schema", schema);
        q.substitute();

        logger.info("Execute S3-Import {}->{} {}", tablename, s3Path, q.text());
        return getClient(clientID).query(q.text())
                .execute()
                .map(row -> row.iterator().next().getString(0));
    }

    /**
     * Finalize Import - create Indices and drop temporary resources
     */
    public static Future<Void> finalizeImport(Job job, String schema){
        String clientId = job.getTargetConnector();
        String tableName = job.getTargetTable();

        Promise<Void> p = Promise.promise();

        List<Future> indicesFutures = new ArrayList<>();

        for (String idxName: DEFAULT_IDX_LIST) {
            SQLQuery q = createIdxQuery(idxName, schema, tableName);
            indicesFutures.add(createIndex(clientId, q, idxName)
                    .onSuccess(idx -> {
                        logger.info("IDX creation of '{}' succeeded!", idx);
                        ((Import)job).addIdx(idxName);
                        //TODO: Add Manual Indices!
                    }).onFailure(e -> {
                        logger.warn("IDX creation '{}' failed! ",idxName,e);
                        job.setErrorDescription(Import.ERROR_DESCRIPTION_IDX_CREATION_FAILED);
                    }));
        }

        CompositeFuture.join(indicesFutures).onComplete(
                t-> {
                    deleteImportTrigger(clientId, schema, tableName);
                    p.complete();
                }
        ).onFailure(e -> {
            logger.warn("Table cleanup has failed",e);
            if(job.getErrorDescription() == null)
                job.setErrorDescription(Import.ERROR_DESCRIPTION_TABLE_CLEANUP_FAILED);
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

    public static SQLQuery createIdxQuery(String idxName, String schema, String tablename){
        SQLQuery q = null;

        switch (idxName){
            case IDX_NAME_ID:
                q = new SQLQuery("CREATE UNIQUE INDEX IF NOT EXISTS ${idx_name} ON ${schema}.${table} ((jsondata->>'id'));");
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

        for (String idxName: DEFAULT_IDX_LIST) {
            SQLQuery q = createIdxQuery(idxName, schema, tablename);
            futures.add(createIndex(clientID, q, idxName));
        }
        return futures;
    }

    public static Future<RunningQueryStatistics> collectRunningQueries(String clientID){
        SQLQuery q = new SQLQuery(
                "select '!ignore!' as ignore, datname,pid,state,backend_type,query_start,state_change,application_name,query from pg_stat_activity "
                        +"WHERE 1=1 "
                         +"AND application_name='"+getApplicationName(clientID)+"'"
                        +"AND datname='postgres' "
                        +"AND state='active' "
                        +"AND backend_type='client backend'"
                        +"AND POSITION('!ignore!' in query) = 0"
        );

        return getClient(clientID).query(q.substitute().text())
                .execute()
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
