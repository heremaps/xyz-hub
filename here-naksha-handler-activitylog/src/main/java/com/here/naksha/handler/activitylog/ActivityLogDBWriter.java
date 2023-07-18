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
package com.here.naksha.handler.activitylog;

import static com.here.naksha.lib.core.NakshaLogger.currentLogger;
import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.lib.psql.PsqlDataSource;
import com.here.naksha.lib.psql.SQL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class ActivityLogDBWriter {

  public static void fromActicityLogDBToFeature(
      final @NotNull PsqlDataSource dataSourceLocalHost,
      final @NotNull PsqlDataSource dataSourceActivityLog,
      final @NotNull String tableName,
      final int pageSize) {
    assert pageSize >= 1;
    long startTime = System.currentTimeMillis();
    currentLogger()
        .atInfo("Activity Log table is {} and page size is {}")
        .add(tableName)
        .add(pageSize)
        .log();
    String schema = dataSourceActivityLog.getSchema();
    List<String> featureList = new ArrayList<>();
    List<String> geoList = new ArrayList<>();
    List<Integer> iList = new ArrayList<>();
    long maxFirstActivityTable;
    try (final Connection conn = dataSourceActivityLog.getConnection()) {
      try (final PreparedStatement stmt = conn.prepareStatement(new SQL("SELECT i FROM ")
          .escape(schema)
          .append(".")
          .escape(tableName)
          .append(" ORDER BY i DESC LIMIT 1;")
          .toString())) {
        final ResultSet result = stmt.executeQuery();
        if (result != null && result.next()) {
          maxFirstActivityTable = result.getInt(1);
        } else {
          throw new SQLException("Empty result-set returned from activity-log table.");
        }
      }
      currentLogger()
          .atInfo("Maximum first value present in activity-log table is: {}")
          .add(maxFirstActivityTable)
          .log();
      // Assigning this value for testing purpose
      maxFirstActivityTable = 100;
      long maxFirstFeaturesTable = 0;
      try (final Connection connLocalHost = dataSourceLocalHost.getConnection()) {
        final String queryPreRequisite = new SQL("CREATE SCHEMA IF NOT EXISTS ")
            .escape(schema)
            .append("; CREATE TABLE IF NOT EXISTS ")
            .escape(schema)
            .append(".Features_Original_Format")
            .append(
                """
(
jsondata      jsonb,"
geo           varchar,"
i             int8 PRIMARY KEY"
);""")
            .append("CREATE SEQUENCE IF NOT EXISTS ")
            .escape(schema)
            .append(".Features_Original_Format_i_seq")
            .append(" AS int8 OWNED BY ")
            .escape(schema)
            .append(".Features_Original_Format.i;")
            .toString();
        sqlExecute(connLocalHost, queryPreRequisite);
        try (final PreparedStatement stmt =
            connLocalHost.prepareStatement("SELECT MAX(i) FROM activity.Features_Original_Format;")) {
          final ResultSet result = stmt.executeQuery();
          if (result.next()) {
            maxFirstFeaturesTable = result.getInt(1);
          }
        }
        currentLogger()
            .atInfo("Maximum first value present in features table is: {}")
            .add(maxFirstFeaturesTable)
            .log();
        long batchNumber = maxFirstFeaturesTable + pageSize;
        while (batchNumber < (maxFirstActivityTable + pageSize)) {
          try (final PreparedStatement stmt = conn.prepareStatement(new SQL("SELECT jsondata,geo,i FROM ")
              .escape(schema)
              .append(".")
              .escape(tableName)
              .append(" WHERE i >= ? ORDER BY i DESC LIMIT ?")
              .toString())) {
            stmt.setLong(1, pageSize * batchNumber);
            stmt.setInt(2, pageSize);
            final ResultSet result = stmt.executeQuery();
            if (result != null) {
              while (result.next()) {
                final XyzFeature activityLogFeature =
                    JsonSerializable.deserialize(result.getString(1), XyzFeature.class);
                assert activityLogFeature != null;
                geoList.add(result.getString(2));
                iList.add(result.getInt(3));
                ActivityLogHandler.fromActivityLogFormat(activityLogFeature);
                featureList.add(activityLogFeature.serialize());
              }
            }
          }
          if (featureList.size() != 0) {
            final String sqlBulkInsertQuery =
                sqlQueryInsertConvertedFeatures(featureList, schema, geoList, iList);
            currentLogger().info("Inserting " + iList.size() + " records with ids " + iList);
            sqlExecute(connLocalHost, sqlBulkInsertQuery);
          }
          connLocalHost.commit();
          batchNumber += pageSize;
          geoList.clear();
          iList.clear();
          featureList.clear();
        }
        currentLogger()
            .atInfo("Total time to process: {}")
            .add(System.currentTimeMillis() - startTime)
            .log();
      }
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  // TODO: Change to prepared statement to prevent possible SQL injections.
  public static String sqlQueryInsertConvertedFeatures(
      List<String> featureList, String schema, List<String> geoList, List<Integer> iList) {
    String firstPart = "INSERT INTO " + schema + ".\"Features_Original_Format\"(jsondata,geo,i) VALUES ";
    for (int iterator = 1; iterator < featureList.size() + 1; iterator++) {
      firstPart += "("
          + "\'"
          + featureList.get(iterator - 1)
          + "\', "
          + "'"
          + geoList.get(iterator - 1)
          + "', "
          + iList.get(iterator - 1)
          + ")";
      if (iterator != featureList.size()) {
        firstPart += ",";
      }
    }
    return firstPart;
  }

  public static void sqlExecute(Connection conn, String sqlQuery) {
    try (final PreparedStatement stmt = conn.prepareStatement(sqlQuery)) {
      stmt.execute();
    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }
  }
}
