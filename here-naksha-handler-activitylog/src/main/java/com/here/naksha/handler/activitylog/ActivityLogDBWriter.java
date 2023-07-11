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

import static com.here.naksha.lib.core.NakshaContext.currentLogger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.lib.psql.PsqlDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ActivityLogDBWriter {
  public static void fromActicityLogDBToFeature(
      PsqlDataSource dataSourceLocalHost, PsqlDataSource dataSourceActivityLog, String tableName, Integer limit) {
    long startTime = System.currentTimeMillis();
    currentLogger().info("Activity Log table is " + tableName + " and value of limit is " + limit);
    String schema = dataSourceActivityLog.getSchema();
    List<String> featureList = new ArrayList<>();
    List<String> geoList = new ArrayList<>();
    List<Integer> iList = new ArrayList<>();
    int intMaxIFeaturesTable = 0;
    int intMaxIActivityTable = 0;
    try (Connection conn = dataSourceActivityLog.getConnection()) {
      String SQLMaxIActivityTable = sqlQueryMaxIActivity(schema, tableName);
      try (final PreparedStatement stmt = conn.prepareStatement(SQLMaxIActivityTable)) {
        final ResultSet result = stmt.executeQuery();
        if (result != null && result.next()) {
          intMaxIActivityTable = result.getInt(1);
        } else {
          currentLogger().error("Make sure that source table Activity Log is not empty.");
        }
      } catch (SQLException throwables) {
        currentLogger().info("Unable to get max I value from an activity table.");
        throwables.printStackTrace();
      }
      currentLogger().info("Maximum I value present in Activity table is " + intMaxIActivityTable);
      // Assigning this value for testing purpose
      intMaxIActivityTable = 100;
      try (Connection connLocalHost = dataSourceLocalHost.getConnection()) {
        String queryPreRequisite = sqlQueryPreRequisites(schema);
        sqlExecute(connLocalHost, queryPreRequisite);
        String SQLMaxIFeaturesTable = sqlQuerySelectMaxIFeatures();
        try (final PreparedStatement stmt = connLocalHost.prepareStatement(SQLMaxIFeaturesTable)) {
          final ResultSet result = stmt.executeQuery();
          if (result != null && result.next()) {
            intMaxIFeaturesTable = result.getInt(1);
          }
        } catch (SQLException throwables) {
          currentLogger().info("Unable to get max I value from an features table.");
          throwables.printStackTrace();
        }
        currentLogger().info("Maximum I value present in Features table is " + intMaxIFeaturesTable);
        Integer batchNumber = intMaxIFeaturesTable + limit;
        while (batchNumber < (intMaxIActivityTable + limit)) {
          String SQLSelectActiLog = sqlQuerySelectFromActivityLog(schema, tableName, batchNumber, limit);
          try (final PreparedStatement stmt = conn.prepareStatement(SQLSelectActiLog)) {
            final ResultSet result = stmt.executeQuery();
            if (result != null) {
              while (result.next()) {
                try {
                  XyzFeature activityLogFeature =
                      JsonSerializable.deserialize(result.getString(1), XyzFeature.class);
                  geoList.add(result.getString(2));
                  iList.add(result.getInt(3));
                  ActivityLogHandler.fromActivityLogFormat(activityLogFeature);
                  featureList.add(activityLogFeature.serialize());
                } catch (JsonProcessingException e) {
                  currentLogger().info("Error while processing/converting activity log json.");
                  e.printStackTrace();
                }
              }
            }
          } catch (SQLException throwables) {
            currentLogger().info("Error while selecting activity log records from activity table.");
            throwables.printStackTrace();
          }
          if (featureList.size() != 0) {
            String sqlBulkInsertQuery =
                sqlQueryInsertConvertedFeatures(featureList, schema, geoList, iList);
            currentLogger().info("Inserting " + iList.size() + " records with ids " + iList);
            sqlExecute(connLocalHost, sqlBulkInsertQuery);
          }
          connLocalHost.commit();
          batchNumber += limit;
          geoList.clear();
          iList.clear();
          featureList.clear();
        }
        currentLogger().info("Total Time to process : " + (System.currentTimeMillis() - startTime));
      } catch (SQLException throwables) {
        currentLogger().info("Error while connecting to destination database.");
        throwables.printStackTrace();
      }
    } catch (SQLException throwables) {
      currentLogger().info("Error while connecting to source database.");
      throwables.printStackTrace();
    }
  }

  public static String sqlQuerySelectFromActivityLog(String schema, String tableName, int batchNumber, int limit) {
    String SQLSelectActiLog = "SELECT jsondata,geo,i FROM "
        + schema
        + ".\""
        + tableName
        + "\" Where i>"
        + (batchNumber - limit)
        + " And i<="
        + batchNumber
        + " limit "
        + limit
        + ";";
    return SQLSelectActiLog;
  }

  public static String sqlQuerySelectMaxIFeatures() {
    String SQLMaxIFeaturesTable = "select i from activity.\"Features_Original_Format\" order by i desc limit 1;";
    return SQLMaxIFeaturesTable;
  }

  public static String sqlQueryMaxIActivity(String schema, String tableName) {
    String SQLMaxIActivityTable = "select i from " + schema + ".\"" + tableName + "\"" + "order by i desc limit 1;";
    return SQLMaxIActivityTable;
  }

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

  public static String sqlQueryPreRequisites(String schema) {
    String sqlQuery = "CREATE SCHEMA IF NOT EXISTS "
        + schema
        + ";"
        + "CREATE TABLE IF NOT EXISTS "
        + schema
        + ".\"Features_Original_Format\""
        + "(\n"
        + "    jsondata      jsonb,"
        + "    geo           varchar,"
        + "    i             int8 PRIMARY KEY"
        + ");"
        + "CREATE SEQUENCE IF NOT EXISTS "
        + schema
        + "."
        + "Features_Original_Format_i_seq"
        + " AS int8 OWNED BY "
        + schema
        + ".\"Features_Original_Format\".i;";
    return sqlQuery;
  }

  public static void sqlExecute(Connection conn, String sqlQuery) {
    try (final PreparedStatement stmt = conn.prepareStatement(sqlQuery)) {
      stmt.execute();
    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }
  }
}
