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

package com.here.xyz.httpconnector.task;

import static com.here.xyz.httpconnector.CService.APPLICATION_NAME_PREFIX;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.config.AwsCWClient;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.status.RDSStatus;
import com.here.xyz.httpconnector.util.status.RunningQueryStatistic;
import com.here.xyz.httpconnector.util.status.RunningQueryStatistics;
import com.here.xyz.util.db.JdbcClient;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import io.vertx.core.Future;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StatusHandler extends JdbcBasedHandler {
  private static final Logger logger = LogManager.getLogger();
  private static final StatusHandler instance = new StatusHandler();

  private StatusHandler() {
    super(CService.configuration.JOB_DB_POOL_SIZE_PER_STATUS_CLIENT);
  }

  public static StatusHandler getInstance() {
    return instance;
  }

  public Future<RDSStatus> getRDSStatus(String connectorId) {
    return getClient(connectorId)
        .compose(client -> getRDSStatus(client, getDbSettings(connectorId)));
  }

  private Future<RDSStatus> getRDSStatus(JdbcClient client, DatabaseSettings dbSettings) {
    //Collect running queries from db
    return collectRunningQueries(client, dbSettings)
        .map(runningQueryStatistics -> {
          RDSStatus rdsStatus = new RDSStatus(dbSettings.getId());
          rdsStatus.addRdsMetrics(runningQueryStatistics);
          return rdsStatus;
        })
        .compose(rdsStatus -> {
          String dbClusterIdentifier = getDBClusterIdentifier(dbSettings);
          if (dbClusterIdentifier == null)
            logger.warn("{} - configured ECPS does not use a clusterConfig! {}", dbSettings.getId(), dbSettings.getHost());
          else {
            //Collect cloudwatch metrics for db
            rdsStatus.addCloudWatchDBWriterMetrics(CService.jobCWClient.getAvg5MinRDSMetrics(dbClusterIdentifier, AwsCWClient.Role.WRITER));
            if (dbSettings.getReplicaHost() != null)
              rdsStatus.addCloudWatchDBReaderMetrics(CService.jobCWClient.getAvg5MinRDSMetrics(dbClusterIdentifier, AwsCWClient.Role.READER));
          }
          return Future.succeededFuture(rdsStatus);
        })
        .onFailure(e -> logger.warn("Can not get RDS-Resources! ", e));
  }

  private String getDBClusterIdentifier(DatabaseSettings databaseSettings) {
      if (databaseSettings.getHost() == null || databaseSettings.getHost().indexOf(".cluster-") == -1)
          return null;

      //DbClusterIdentifier.ClusterIdentifier.Region.rds.amazonaws.com
      return databaseSettings.getHost().split("\\.")[0];
  }

  //TODO: Move abort job methods into JDBCBasedJob class
  public Future<Void> abortJob(Job job) {
    return getClient(job.getTargetConnector())
        .compose(client -> abortJob(job, client, getDbSettings(job.getTargetConnector())));

  }

  private static Future<Void> abortJob(Job job, JdbcClient client, DatabaseSettings dbSettings) {
    SQLQuery q = buildAbortsJobQuery(job);
    logger.info("job[{}] Abort Job {}: {}", job.getId(), job.getTargetSpaceId(), q.text());

    //Abort queries on writer & reader in parallel
    return Future.all(
        abortJobQueries(job, client, dbSettings, q, false),
        abortJobQueries(job, client, dbSettings, q, true)
    ).mapEmpty();
  }

  private static Future<Void> abortJobQueries(Job job, JdbcClient client, DatabaseSettings dbSettings, SQLQuery abortQuery,
      boolean useReader) {
    if (useReader && !client.hasReader())
      return Future.succeededFuture();
    return client.run(abortQuery, useReader)
        .onSuccess(v -> logger.info("job[{}] Aborted job on {} {}@{}", job.getId(), useReader ? "reader" : "writer",
            job.getTargetSpaceId(), dbSettings.getHost()));
  }

  private static SQLQuery buildAbortsJobQuery(Job job) {
      return new SQLQuery("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                      + "WHERE state = 'active' "
                      + "AND strpos(query, 'pg_terminate_backend') = 0 "
                      + "AND strpos(query, #{queryIdentifier}) > 0 "
                      + "AND strpos(query, #{jobIdentifier}) > 0 ")
          .withNamedParameter("queryIdentifier", job.getQueryIdentifier())
          .withNamedParameter("jobIdentifier", getJobIdentifier(job));
  }

  private static String getJobIdentifier(Job job) {
      return "m499#jobId(" + job.getId() + ")";
  }

  @FunctionalInterface
  private interface ResultExtractor {
    void runWithPotentialException() throws SQLException;
  }

  private static void forEachResult(ResultSet rs, ResultExtractor resultExtractor) throws SQLException {
      while (rs.next())
          resultExtractor.runWithPotentialException();
  }

  private static Future<RunningQueryStatistics> collectRunningQueries(JdbcClient client, DatabaseSettings dbSettings) {
    SQLQuery q = new SQLQuery("SELECT '!ignore!' as ignore, datname, pid, state, backend_type, query_start, state_change, "
        + "application_name, query FROM pg_stat_activity WHERE "
        + "application_name LIKE #{applicationName} "
        + "AND datname = #{db} "
        + "AND state = 'active' "
        + "AND backend_type = 'client backend' "
        + "AND POSITION('!ignore!' in query) = 0"
    )
        .withNamedParameter("applicationName", APPLICATION_NAME_PREFIX + "%")
        .withNamedParameter("db", dbSettings.getDb());

    return client.run(q, rs -> {
          RunningQueryStatistics statistics = new RunningQueryStatistics();
          forEachResult(rs, () -> statistics.addRunningQueryStatistic(new RunningQueryStatistic(
              rs.getInt("pid"),
              rs.getString("state"),
              rs.getTimestamp("query_start").toLocalDateTime(),
              rs.getTimestamp("state_change").toLocalDateTime(),
              rs.getString("query"),
              rs.getString("application_name")
          )));
          return statistics;
        })
        .onFailure(e -> logger.warn(e));
  }
}
