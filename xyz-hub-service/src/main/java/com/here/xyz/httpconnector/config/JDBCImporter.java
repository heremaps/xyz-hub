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

import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.GEOJSON;
import static com.here.xyz.httpconnector.util.jobs.Job.ERROR_TYPE_ABORTED;
import static com.here.xyz.httpconnector.util.jobs.Job.ERROR_TYPE_FINALIZATION_FAILED;
import static com.here.xyz.psql.query.ModifySpace.IDX_STATUS_TABLE;
import static com.here.xyz.psql.query.ModifySpace.XYZ_CONFIG_SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildLoadSpaceTableIndicesQuery;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildSpaceTableDropIndexQueries;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildSpaceTableIndexQueries;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.task.JdbcBasedHandler;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.jobs.Job.CSVFormat;
import com.here.xyz.util.db.SQLQuery;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Client for handle IMPORT-Jobs (S3 -> RDS)
 */
public class JDBCImporter extends JdbcBasedHandler {
    private static final Logger logger = LogManager.getLogger();
    private static final JDBCImporter instance = new JDBCImporter();

    private JDBCImporter() {
        super(CService.configuration.JOB_DB_POOL_SIZE_PER_CLIENT);
    }

    public static JDBCImporter getInstance() {
        return instance;
    }

    /**
     * Prepare Table for Import
     */
    public Future<Void> prepareImport(Import job){
        return increaseVersionSequence(job.getTargetConnector(), job.getTargetTable())
            .compose(version -> {
                job.setSpaceVersion(version);
                if (version > 1)
                    //Abort - if increased version is not 0 => Layer is not empty
                    return Future.failedFuture("SequenceNot0");

                return listIndices(job.getTargetConnector(), job.getTargetTable())
                    .compose(idxList -> dropIndices(job.getTargetConnector(), idxList))
                    .compose(v -> createTriggerOnTargetTable(job));
            });
    }

    private  Future<List<String>> listIndices(String clientID, String tableName){
        return getClient(clientID).compose(client -> {
            SQLQuery q = buildLoadSpaceTableIndicesQuery(getDbSettings(clientID).getSchema(), tableName);
            return client.run(q, rs -> {
                List<String> idxList = new ArrayList<>();
                while(rs.next())
                    idxList.add(rs.getString(1));
                return idxList;
            }, true);
        });
    }

    private  Future<Void> dropIndices(String clientID, List<String> indexNames) {
        if (indexNames.size() == 0)
            return Future.succeededFuture();

        String schema = getDbSettings(clientID).getSchema();

        return getClient(clientID).compose(client -> {
            List<SQLQuery> dropQueries = buildSpaceTableDropIndexQueries(schema, indexNames);
            SQLQuery q = SQLQuery.join(dropQueries, ";");
            return client.write(q).mapEmpty();
        });
    }

    private Future<Void> createTriggerOnTargetTable(Job job) {
        return getClient(job.getTargetConnector()).compose(client -> {
            SQLQuery q = new SQLQuery("CREATE OR REPLACE TRIGGER insertTrigger BEFORE INSERT ON ${schema}.${table} \n"
                + "FOR EACH ROW EXECUTE PROCEDURE ${schema}.xyz_import_trigger('${{triggerHrn}}', false, false, ${{spaceVersion}}, "
                + "'${{author}}');")
                .withQueryFragment("triggerHrn", "TBD")
                .withQueryFragment("spaceVersion", "" + job.getSpaceVersion())
                .withQueryFragment("author", job.getAuthor() == null ? "ANONYMOUS" : job.getAuthor())
                .withVariable("schema", getDbSettings(job.getTargetConnector()).getSchema())
                .withVariable("table", job.getTargetTable());

            return client.write(q).mapEmpty();
        });
    }

    //TODO: Use existing impl of GetNextVersion QR instead!!
    @Deprecated
    private Future<Long> increaseVersionSequence(String clientID, String tableName) {
        return getClient(clientID).compose(client -> {
            SQLQuery q = new SQLQuery("SELECT nextval('${schema}.${sequence}');")
                .withVariable("schema", getDbSettings(clientID).getSchema())
                .withVariable("sequence", tableName + "_version_seq");

            return client.run(q, rs -> {
                rs.next();
                return rs.getLong(1);
            });
        });
    }

    /**
     * Import Data from S3
     */
    public Future<String> executeImport(Import job, String tableName, String s3Bucket, String s3Path, String s3Region, long curFileSize, CSVFormat csvFormat) {
        return getClient(job.getTargetConnector()).compose(client -> {
            SQLQuery q = new SQLQuery(("SELECT ${{importHint}} aws_s3.table_import_from_s3( "
                + "'${schema}.${table}', "
                + "#{columns}, "
                + " 'DELIMITER '','' CSV ENCODING  ''UTF8'' QUOTE  ''\"'' ESCAPE '''''''' ', "
                + " aws_commons.create_s3_uri( "
                + "     #{s3Bucket}, "
                + "     #{s3Path}, "
                + "     #{s3Region}"
                + " ))"))
                .withQueryFragment("importHint", buildImportHint(s3Path, curFileSize, job.getId()))
                .withNamedParameter("s3Bucket", s3Bucket)
                .withNamedParameter("s3Path", s3Path)
                .withNamedParameter("s3Region", s3Region)
                .withNamedParameter("columns", csvFormat == GEOJSON ? "jsondata" : "jsondata,geo")
                .withVariable("table", tableName)
                .withVariable("schema", getDbSettings(job.getTargetConnector()).getSchema());

            logger.info("job[{}] Execute S3-Import {}->{} {}", job.getId(), tableName, s3Path, q.text());
            return client.run(q, rs -> {
                rs.next();
                return rs.getString(1);
            });
        });
    }

    private static SQLQuery buildImportHint(String jobId) {
        return buildImportHint(null, 0, jobId);
    }

    private static SQLQuery buildImportHint(String s3Path, long curFileSize, String jobId) {
        String queryTypeIdentifier = "import_hint";
        SQLQuery importHint = new SQLQuery("/* ${{queryTypeIdentifier}} ${{s3Part}} m499#jobId(${{jobId}}) */")
            .withQueryFragment("queryTypeIdentifier", queryTypeIdentifier)
            .withQueryFragment("jobId", jobId)
            .withQueryFragment("s3Part", "");

        if (s3Path != null)
            importHint.withQueryFragment("s3Part", new SQLQuery("${{s3Path}}:${{curFileSize}}")
                .withQueryFragment("s3Path", s3Path)
                .withQueryFragment("curFileSize", "" + curFileSize));

        return importHint;
    }

    private Future<Void> createIndices(Import job, String schema, String tableName, String clientId) {
        /*
        FIXME:
         Let DatabaseMaintainer create the default indices in future. It can put the index creation tasks into a queue and execute as many in
         parallel as possible (depending on the DB load)
         */
        List<SQLQuery> indexCreationQueries = buildSpaceTableIndexQueries(schema, tableName, buildImportHint(job.getId()));

        //Run all index creations in sequence
        Future<Void> resultingSequentialFuture = Future.succeededFuture();
        for (SQLQuery q : indexCreationQueries)
            resultingSequentialFuture = resultingSequentialFuture.compose(v -> getClient(clientId)
                .compose(client -> client.write(q).<Void>mapEmpty())
                .onSuccess(nv -> {
                    logger.info("job[{}] IDX creation of succeeded! Index creation query: {}", job.getId(), q);
                    job.addIdx(q.toString());
                })
                .onFailure(e -> {
                    logger.warn("job[{}] IDX creation failed! Index creation query: {}", job.getId(), q, e);
                    job.setErrorDescription(Import.ERROR_DESCRIPTION_IDX_CREATION_FAILED);
                }));

        return resultingSequentialFuture;
    }

    /**
     * Finalize Import - create Indices and drop temporary resources
     */
    public Future<Void> finalizeImport(Import job) {
        return getClient(job.getTargetConnector()).compose(client -> {
            String schema = getDbSettings(job.getTargetConnector()).getSchema();

            return createIndices(job, schema, job.getTargetTable(), job.getTargetConnector())
                .compose(cf -> deleteImportTrigger(job.getTargetConnector(), schema, job.getTargetTable()))
                .compose(v -> triggerAnalyse(job.getTargetConnector(), schema, job.getTargetTable()))
                .compose(v -> markMaintenance(job.getTargetConnector(), schema, job.getTargetTable()))
                .recover(t -> { //TODO: Do not rely on the error message of the exception, check other info instead (e.g. exception type / error code)
                    if (t.getMessage() != null && t.getMessage().equalsIgnoreCase("Fail to read any response from the server, "
                        + "the underlying connection might get lost unexpectedly."))
                        return Future.failedFuture(ERROR_TYPE_ABORTED);
                    else
                        return Future.failedFuture(ERROR_TYPE_FINALIZATION_FAILED);
                });
        });
    }

    private Future<Void> triggerAnalyse(String clientID, String schema, String tableName) {
        SQLQuery q = new SQLQuery("ANALYSE ${schema}.${table}")
            .withVariable("schema", schema)
            .withVariable("table", tableName);

        return getClient(clientID).compose(client -> client.write(q).mapEmpty());
    }

    private Future<Void> deleteImportTrigger(String clientID, String schema, String tableName) {
        SQLQuery q = new SQLQuery("DROP TRIGGER IF EXISTS insertTrigger ON ${schema}.${table}")
            .withVariable("schema", schema)
            .withVariable("table", tableName);

        return getClient(clientID).compose(client -> client.write(q).mapEmpty());
    }

    private Future<Void> markMaintenance(String clientID, String schema, String spaceId) {
        SQLQuery q = new SQLQuery("UPDATE ${schema}.${table} "
                + "SET idx_creation_finished = #{markAs} "
                + "WHERE spaceid = #{spaceId} AND schem = #{schema}")
            .withVariable("schema", XYZ_CONFIG_SCHEMA)
            .withVariable("table", IDX_STATUS_TABLE)
            .withNamedParameter("schema", schema)
            .withNamedParameter("spaceId", spaceId)
            .withNamedParameter("markAs", false);

        logger.info("Mark maintenance for {}", spaceId);
        return getClient(clientID).compose(client -> client.write(q).mapEmpty());
    }
}
