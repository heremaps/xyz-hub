/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.hub.config.JDBCConfig;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

import java.util.ArrayList;
import java.util.List;

/**
 * A client for reading and editing job definitions.
 */
public class JDBCJobConfigClient extends JobConfigClient {
    private static final Logger logger = LogManager.getLogger();
    public static final String JOB_TABLE = JDBCConfig.SCHEMA + ".xyz_job";
    private static JDBCJobConfigClient instance;
    private final SqlClient client;
    private static boolean initialized = false;

    private JDBCJobConfigClient() {
        JDBCClients.addConfigClient();
        this.client = JDBCClients.getClient(JDBCClients.CONFIG_CLIENT_ID);
    }

    public static JDBCJobConfigClient getInstance() {
        if (instance == null) {
            instance = new JDBCJobConfigClient();
        }
        return instance;
    }

    @Override
    protected Future<Job> getJob(Marker marker, String jobId) {
        return client.preparedQuery("SELECT * FROM " + JOB_TABLE + " WHERE ID=$1")
                .execute(Tuple.of(jobId))
                .map(rows -> {
                    if (rows.rowCount() == 0)
                        return null;
                    JsonObject config = rows.iterator().next().getJsonObject("config");
                    return Json.decodeValue(config.toString(), Job.class);
                });
    }

    @Override
    protected Future<List<Job>> getJobs(Marker marker, Job.Type type, Job.Status status, String targetSpaceId) {
        final List<Job> result = new ArrayList<>();
        List<Object> tuples = new ArrayList<>();

        String q = "SELECT * FROM " + JOB_TABLE + " WHERE 1 = 1 ";

        if (type != null) {
            tuples.add(type.toString());
            q += " AND config->>'type' = $" + tuples.size();
        }

        if (status != null) {
            tuples.add(status.toString());
            q += " AND config->>'status' = $" + tuples.size();
        }

        if (targetSpaceId != null) {
            tuples.add(targetSpaceId);
            q += " AND config->>'targetSpaceId' = $" + tuples.size();
        }
        return client.preparedQuery(q)
                .execute(tuples.size() > 0 ? Tuple.from(tuples) : Tuple.tuple())
                .map(rows -> {
                    for (Row row : rows) {
                        result.add(Json.decodeValue(row.getJsonObject("config").toString(), Job.class));
                    }
                    return result;
                });
    }

    @Override
    protected Future<String> getImportJobsOnTargetSpace(Marker marker, String targetSpaceId) {
        List<Object> tuples = new ArrayList<>();

        String q = "SELECT * FROM " + JOB_TABLE + " WHERE 1 = 1 AND config->>'status' NOT IN ('waiting','failed','finalized') ";

        if (targetSpaceId != null) {
            tuples.add(targetSpaceId);
            q += " AND config->>'targetSpaceId' = $1";
        }

        return client.preparedQuery(q)
                .execute(Tuple.of(targetSpaceId))
                .map(rows -> {
                    if (rows.rowCount() == 0) {
                        return null;
                    }
                    return rows.iterator().next().getString("id");
                });
    }

    @Override
    protected Future<Job> storeJob(Marker marker, Job job, boolean isUpdate) {
        return client.preparedQuery("INSERT INTO " + JOB_TABLE + " (id, jobtype, config) VALUES ($1, $2, $3) " +
                        "ON CONFLICT (id) DO " +
                        "UPDATE SET id = $1, jobtype = $2, config = $3")
                .execute(Tuple.of(job.getId(), job.getClass().getSimpleName(), new JsonObject(Json.encode(job))))
                .map(rows -> {
                    if (rows.rowCount() == 0) {
                        return null;
                    }
                    return job;
                });
    }

    @Override
    protected Future<Job> deleteJob(Marker marker, Job job) {
        return client.preparedQuery("DELETE FROM " + JOB_TABLE + " WHERE id = $1 ")
                .execute(Tuple.of(job.getId()))
                .map(rows -> {
                    if (rows.rowCount() == 0) {
                        return null;
                    }
                    return job;
                });
    }

    @Override
    public void init(Handler<AsyncResult<Void>> onReady) {
        if (initialized) {
            onReady.handle(Future.succeededFuture());
            return;
        }
        initialized = true;

        String q = "SELECT " +
                "to_regclass as table_exists," +
                "(SELECT schema_name as schema_exists FROM information_schema.schemata WHERE schema_name='xyz_config') " +
                "from to_regclass($1)";

        client.preparedQuery(q)
                .execute(Tuple.of(JOB_TABLE))
                .onSuccess(rows -> {
                    if (rows.rowCount() == 0) {
                        onReady.handle(Future.failedFuture(""));
                        return;
                    }
                    String tableExists = rows.iterator().next().getString("table_exists");
                    String schemaExists = rows.iterator().next().getString("schema_exists");

                    if (schemaExists == null) {
                        logger.error("Config-Schema is missing!");
                        onReady.handle(Future.failedFuture("Config-Schema is missing!"));
                        return;
                    }
                    if (tableExists == null) {
                        logger.info("Job Table is missing - create Table!");
                        client.preparedQuery("CREATE TABLE IF NOT EXISTS "+JOB_TABLE+" (id VARCHAR(255) primary key, jobtype VARCHAR (255), config JSONB)")
                                .execute()
                                .onSuccess( f -> onReady.handle(Future.succeededFuture()))
                                .onFailure( f -> {
                                    logger.error("Cant create JOB-Config Table!", f);
                                    onReady.handle(Future.failedFuture(f.getCause()));
                                });
                    }else {
                        /** Schema are Table are in place */
                        onReady.handle(Future.succeededFuture());
                    }
                })
                .onFailure(f -> {
                    logger.error("Initialization Error", f);
                    onReady.handle(Future.failedFuture(f.getCause()));
                });
    }
}
