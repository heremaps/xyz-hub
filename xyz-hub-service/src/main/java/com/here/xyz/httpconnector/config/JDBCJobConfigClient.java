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

import static com.here.xyz.hub.config.jdbc.JDBCConfigClient.configListParser;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.hub.config.jdbc.JDBCConfigClient;
import com.here.xyz.util.db.SQLQuery;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.sql.SQLException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.postgresql.util.PGobject;

/**
 * A client for reading and editing job definitions.
 */
public class JDBCJobConfigClient extends JobConfigClient {
  private static final String JOB_API_SCHEMA = "xyz_job_api";
  private static final String JOBS_TABLE = "xyz_jobs";
  private static final Logger logger = LogManager.getLogger();
  private static JDBCJobConfigClient instance;
  private static volatile boolean initialized = false;
  private final JDBCConfigClient client = new JDBCConfigClient(JOB_API_SCHEMA, JOBS_TABLE, CService.configuration);

  public static JDBCJobConfigClient getInstance() {
    if (instance == null)
      instance = new JDBCJobConfigClient();
    return instance;
  }

  private static PGobject toJsonb(String json) throws SQLException {
    final PGobject jsonbObject = new PGobject();
    jsonbObject.setType("jsonb");
    jsonbObject.setValue(json);
    return jsonbObject;
  }

  @Override
  protected Future<Job> getJob(Marker marker, String jobId) {
    return client.run(
        client.getQuery("SELECT * FROM ${schema}.${table} WHERE ID=#{id}").withNamedParameter("id", jobId),
        rs -> rs.next() ? Json.decodeValue(rs.getString("config"), Job.class) : null
    );
  }

  @Override
  protected Future<List<Job>> getJobs(Marker marker, String type, Job.Status status, String targetSpaceId) {
    SQLQuery q = client.getQuery("SELECT * FROM ${schema}.${table} WHERE TRUE ${{type}} ${{status}} ${{targetSpaceId}}")
        .withQueryFragment("type", "")
        .withQueryFragment("status", "")
        .withQueryFragment("targetSpaceId", "");

    if (type != null)
      q.setQueryFragment("type", new SQLQuery("AND config->>'type' = #{type}").withNamedParameter("type", type));

    if (status != null)
      q.setQueryFragment("status", new SQLQuery("AND config->>'status' = #{status}")
          .withNamedParameter("status", status.toString()));

    if (targetSpaceId != null)
      q.setQueryFragment("targetSpaceId", new SQLQuery("AND config->>'targetSpaceId' = #{targetSpaceId}")
          .withNamedParameter("targetSpaceId", targetSpaceId));

    return client.run(q, configListParser(Job.class));
  }

  @Override
  protected Future<List<Job>> getJobs(Marker marker, Job.Status status, String key, DatasetDirection direction) {
    SQLQuery q = client.getQuery("SELECT * FROM ${schema}.${table} WHERE TRUE ${{status}} ${{direction}}")
        .withQueryFragment("status", "")
        .withQueryFragment("direction", "");

    if (status != null)
      q.setQueryFragment("status", new SQLQuery(" AND config->>'status' = #{status}")
          .withNamedParameter("status", status.toString()));

    if (key != null) {
      SQLQuery directionCondition;
      if (direction == DatasetDirection.SOURCE)
        directionCondition = new SQLQuery("AND config->>'_sourceKey' = #{key}");
      else if (direction == DatasetDirection.TARGET)
        directionCondition = new SQLQuery("AND config->>'_targetKey' = #{key}");
      else
        directionCondition = new SQLQuery("AND (config->>'_sourceKey' = #{key} OR config->>'_targetKey' = #{key})");
      q.setQueryFragment("direction", directionCondition);
    }
    return client.run(q, configListParser(Job.class));
  }

  @Override
  protected Future<String> findRunningJobOnSpace(Marker marker, String targetSpaceId, String type) {
    SQLQuery q = client.getQuery("SELECT * FROM ${schema}.${table} "
        + "WHERE TRUE AND config->>'status' NOT IN ('waiting', 'failed', 'finalized') "
        + "AND config->>'targetSpaceId' = #{targetSpaceId} "
        + "AND jobtype = #{type}")
        .withNamedParameter("targetSpaceId", targetSpaceId)
        .withNamedParameter("type", type);

    return client.run(q, rs -> rs.next() ? rs.getString("id") : null);
  }

  @Override
  protected Future<Job> storeJob(Marker marker, Job job, boolean isUpdate) {
    JsonObject json = new JsonObject(Json.encode(job));
    if (job.getSource() != null)
      json.put("_sourceKey", job.getSource().getKey());
    if (job.getTarget() != null)
      json.put("_targetKey", job.getTarget().getKey());

    SQLQuery q;
    try {
      q = client.getQuery("INSERT INTO ${schema}.${table} (id, jobtype, config) VALUES ($1, $2, $3) " +
          "ON CONFLICT (id) DO " +
          "UPDATE SET id = #{id}, jobtype = #{type}, config = #{config}")
          .withNamedParameter("id", job.getId())
          .withNamedParameter("type", job.getClass().getSimpleName())
          .withNamedParameter("config", toJsonb(json.toString()));
    }
    catch (SQLException e) {
      return Future.failedFuture(e);
    }

    return client.write(q)
        .map(rowCount -> rowCount > 0 ? job : null);
  }

  @Override
  protected Future<Job> deleteJob(Marker marker, Job job) {
    return client.write(client.getQuery("DELETE FROM ${schema}.${table} WHERE id = #{id}").withNamedParameter("id", job.getId()))
        .map(rowCount -> rowCount > 0 ? job : null);
  }

  @Override
  public Future<Void> init() {
    if (initialized)
      return Future.succeededFuture();

    initialized = true;
    return client.init()
        .compose(v -> initTable());
  }

  private Future<Void> initTable() {
    return client.write(client.getQuery("CREATE TABLE IF NOT EXISTS ${schema}.${table} "
        + "(id TEXT primary key, jobtype TEXT, config JSONB)"))
        .onFailure(e -> logger.error("Can not create table {}!", JOBS_TABLE, e))
        .mapEmpty();
  }
}
