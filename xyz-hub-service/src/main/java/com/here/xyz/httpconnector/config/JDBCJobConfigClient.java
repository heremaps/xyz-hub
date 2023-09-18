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

import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.hub.config.jdbc.JDBCConfig;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

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
          if (rows.rowCount() == 0) {
            return null;
          }
          JsonObject config = rows.iterator().next().getJsonObject("config");
          return Json.decodeValue(config.toString(), Job.class);
        });
  }

  @Override
  protected Future<List<Job>> getJobs(Marker marker, String type, Job.Status status, String targetSpaceId) {
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
    protected Future<List<Job>> getJobs(Marker marker, Job.Status status, String key, DatasetDirection direction) {
        final List<Job> result = new ArrayList<>();
        List<Object> tuples = new ArrayList<>();

        String q = "SELECT * FROM " + JOB_TABLE + " WHERE 1 = 1 ";

        if (status != null) {
            tuples.add(status.toString());
            q += " AND config->>'status' = $" + tuples.size();
        }

        if (key != null) {
            tuples.add(key);
            if(direction == DatasetDirection.SOURCE)
                q += " AND config->>'_sourceKey' = $" + tuples.size();
            else if(direction == DatasetDirection.TARGET)
                q += " AND config->>'_targetKey' = $" + tuples.size();
            else
                q += " AND (config->>'_sourceKey' = $" + tuples.size() + " OR config->>'_targetKey' = $" + tuples.size() + ")";
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
  protected Future<String> findRunningJobOnSpace(Marker marker, String targetSpaceId, String type) {
    String q = "SELECT * FROM " + JOB_TABLE + " WHERE 1 = 1 AND config->>'status' NOT IN ('waiting','failed','finalized')" +
        " AND config->>'targetSpaceId' = $1" +
        " AND jobtype = $2";

    return client.preparedQuery(q)
        .execute(Tuple.of(targetSpaceId, type))
        .map(rows -> {
          if (rows.rowCount() == 0) {
            return null;
          }
          return rows.iterator().next().getString("id");
        });
  }

  @Override
  protected Future<Job> storeJob(Marker marker, Job job, boolean isUpdate) {
    JsonObject json = new JsonObject(Json.encode(job));
        if(job.getSource() != null)
            json.put("_sourceKey", job.getSource().getKey());
        if(job.getTarget() != null)
            json.put("_targetKey", job.getTarget().getKey());return client.preparedQuery("INSERT INTO " + JOB_TABLE + " (id, jobtype, config) VALUES ($1, $2, $3) " +
            "ON CONFLICT (id) DO " +
            "UPDATE SET id = $1, jobtype = $2, config = $3")
        .execute(Tuple.of(job.getId(), job.getClass().getSimpleName(), json))
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
  public Future<Void> init() {
    if (initialized) {
      return Future.succeededFuture();
    }
    initialized = true;

    String q = "SELECT " +
        "to_regclass as table_exists," +
        "(SELECT schema_name as schema_exists FROM information_schema.schemata WHERE schema_name='xyz_config') " +
        "from to_regclass($1)";

    return client.preparedQuery(q)
        .execute(Tuple.of(JOB_TABLE))
        .flatMap(rows -> {
          if (rows.rowCount() == 0) {
            return Future.failedFuture("");
          }

          String tableExists = rows.iterator().next().getString("table_exists");
          String schemaExists = rows.iterator().next().getString("schema_exists");

          if (schemaExists == null) {
            logger.error("Config-Schema is missing!");
            return Future.failedFuture("Config-Schema is missing!");
          }

          if (tableExists != null) {
            return Future.succeededFuture();
          }
          logger.info("Job Table is missing - create Table!");

          return client.preparedQuery(
                  "CREATE TABLE IF NOT EXISTS " + JOB_TABLE + " (id VARCHAR(255) primary key, jobtype VARCHAR (255), config JSONB)")
              .execute()
              .onFailure(f -> {
                logger.error("Cant create JOB-Config Table!", f);
              });
        })
        .onFailure(t -> logger.error("Initialization Error", t))
        .mapEmpty();
  }
}
