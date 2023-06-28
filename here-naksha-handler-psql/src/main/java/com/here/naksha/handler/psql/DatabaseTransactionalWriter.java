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
package com.here.naksha.handler.psql;

import static com.here.naksha.lib.core.NakshaContext.currentLogger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import com.here.naksha.lib.core.models.geojson.implementation.FeatureCollection;
import com.vividsolutions.jts.geom.Geometry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.postgresql.util.PGobject;

public class DatabaseTransactionalWriter extends DatabaseWriter {

  private static final int TYPE_INSERT = 1;
  private static final int TYPE_UPDATE = 2;
  private static final int TYPE_DELETE = 3;

  public static FeatureCollection insertFeatures(
      @NotNull PsqlHandler processor,
      FeatureCollection collection,
      List<FeatureCollection.ModificationFailure> fails,
      List<Feature> inserts,
      Connection connection,
      Integer version,
      boolean forExtendedSpace)
      throws SQLException, JsonProcessingException {

    final String schema = processor.spaceSchema();
    final String table = processor.spaceTable();
    final PreparedStatement insertStmt = createInsertStatement(connection, schema, table, forExtendedSpace);
    final PreparedStatement insertWithoutGeometryStmt =
        createInsertWithoutGeometryStatement(connection, schema, table, forExtendedSpace);

    // parameters for without-geometry scenario
    final List<String> insertWithoutGeometryIdList = new ArrayList<>();
    final List<PGobject> insertWithoutGeoJsonbObjectList = new ArrayList<>();
    final List<Feature> featureWithoutGeoList = new ArrayList<>();
    // parameters for with-geometry scenario
    final List<String> insertIdList = new ArrayList<>();
    final List<PGobject> insertJsonbObjectList = new ArrayList<>();
    final List<Geometry> insertGeometryList = new ArrayList<>();
    final List<Feature> featureList = new ArrayList<>();

    for (int i = 0; i < inserts.size(); i++) {
      final Feature feature = inserts.get(i);

      final PGobject jsonbObject = featureToPGobject(feature, version);

      if (feature.getGeometry() == null) {
        /*if (forExtendedSpace)
        insertWithoutGeometryStmt.setBoolean(2, getDeletedFlagFromFeature(feature));*/
        insertWithoutGeoJsonbObjectList.add(jsonbObject);
        insertWithoutGeometryIdList.add(feature.getId());
        featureWithoutGeoList.add(feature);
      } else {
        insertJsonbObjectList.add(jsonbObject);
        Geometry jtsGeometry = feature.getGeometry().getJTSGeometry();
        // Avoid NAN values
        assure3d(jtsGeometry.getCoordinates());
        insertGeometryList.add(jtsGeometry);
        /*if (forExtendedSpace)
        insertStmt.setBoolean(3, getDeletedFlagFromFeature(feature));*/
        insertIdList.add(feature.getId());
        featureList.add(feature);
      }
      collection.getFeatures().add(feature);
    }

    if (insertWithoutGeometryIdList.size() > 0) {
      insertWithoutGeometryStmt.setArray(
          1, connection.createArrayOf("jsonb", insertWithoutGeoJsonbObjectList.toArray()));
    }
    if (insertIdList.size() > 0) {
      insertStmt.setArray(1, connection.createArrayOf("jsonb", insertJsonbObjectList.toArray()));
      insertStmt.setArray(2, connection.createArrayOf("geometry", insertGeometryList.toArray()));
    }

    executeBatchesAndCheckOnFailures(
        processor,
        insertIdList,
        insertWithoutGeometryIdList,
        insertStmt,
        insertWithoutGeometryStmt,
        featureList,
        featureWithoutGeoList,
        fails,
        false,
        TYPE_INSERT,
        table);

    if (fails.size() > 0) {
      logException(null, processor, LOG_EXCEPTION_INSERT, table);
      throw new SQLException(INSERT_ERROR_GENERAL);
    }

    return collection;
  }

  public static FeatureCollection updateFeatures(
      @NotNull PsqlHandler processor,
      FeatureCollection collection,
      List<FeatureCollection.ModificationFailure> fails,
      List<Feature> updates,
      Connection connection,
      boolean handleUUID,
      boolean enableNowait,
      Integer version,
      boolean forExtendedSpace)
      throws SQLException, JsonProcessingException {
    final String schema = processor.spaceSchema();
    final String table = processor.spaceTable();
    final PreparedStatement updateStmt =
        createUpdateStatement(connection, schema, table, handleUUID, forExtendedSpace);
    final PreparedStatement updateWithoutGeometryStmt =
        createUpdateWithoutGeometryStatement(connection, schema, table, handleUUID, forExtendedSpace);

    // parameters for without-geometry scenario
    final List<String> updateWithoutGeometryIdList = new ArrayList<>();
    final List<PGobject> updateWithoutGeoJsonbObjectList = new ArrayList<>();
    final List<String> updateWithoutGeoUuidList = handleUUID ? new ArrayList<>() : null;
    final List<Feature> featureWithoutGeoList = new ArrayList<>();
    // parameters for with-geometry scenario
    final List<String> updateIdList = new ArrayList<>();
    final List<PGobject> updateJsonbObjectList = new ArrayList<>();
    final List<String> updateUuidList = handleUUID ? new ArrayList<>() : null;
    final List<Geometry> updateGeometryList = new ArrayList<>();
    final List<Feature> featureList = new ArrayList<>();

    for (int i = 0; i < updates.size(); i++) {
      final Feature feature = updates.get(i);
      final String puuid = feature.getProperties().getXyzNamespace().getPuuid();

      final PGobject jsonbObject = featureToPGobject(feature, version);

      if (feature.getGeometry() == null) {
        if (handleUUID) {
          updateWithoutGeoUuidList.add(puuid);
        }
        updateWithoutGeoJsonbObjectList.add(jsonbObject);
        /*if (forExtendedSpace)
        updateWithoutGeometryStmt.setBoolean(++paramIdx, getDeletedFlagFromFeature(feature));*/
        updateWithoutGeometryIdList.add(feature.getId());
        featureWithoutGeoList.add(feature);
      } else {
        if (handleUUID) {
          updateUuidList.add(puuid);
        }
        updateJsonbObjectList.add(jsonbObject);
        Geometry jtsGeometry = feature.getGeometry().getJTSGeometry();
        // Avoid NAN values
        assure3d(jtsGeometry.getCoordinates());
        updateGeometryList.add(jtsGeometry);
        /*if (forExtendedSpace)
        updateStmt.setBoolean(++paramIdx, getDeletedFlagFromFeature(feature));*/

        updateIdList.add(feature.getId());
        featureList.add(feature);
      }
      collection.getFeatures().add(feature);
    }

    if (updateWithoutGeometryIdList.size() > 0) {
      updateWithoutGeometryStmt.setArray(
          1, connection.createArrayOf("text", updateWithoutGeometryIdList.toArray()));
      updateWithoutGeometryStmt.setArray(
          2, handleUUID ? connection.createArrayOf("text", updateWithoutGeoUuidList.toArray()) : null);
      updateWithoutGeometryStmt.setArray(
          3, connection.createArrayOf("jsonb", updateWithoutGeoJsonbObjectList.toArray()));
      updateWithoutGeometryStmt.setBoolean(4, enableNowait);
    }
    if (updateIdList.size() > 0) {
      updateStmt.setArray(1, connection.createArrayOf("text", updateIdList.toArray()));
      updateStmt.setArray(2, handleUUID ? connection.createArrayOf("text", updateUuidList.toArray()) : null);
      updateStmt.setArray(3, connection.createArrayOf("jsonb", updateJsonbObjectList.toArray()));
      updateStmt.setArray(4, connection.createArrayOf("geometry", updateGeometryList.toArray()));
      updateStmt.setBoolean(5, enableNowait);
    }

    executeBatchesAndCheckOnFailures(
        processor,
        updateIdList,
        updateWithoutGeometryIdList,
        updateStmt,
        updateWithoutGeometryStmt,
        featureList,
        featureWithoutGeoList,
        fails,
        handleUUID,
        TYPE_UPDATE,
        table);

    if (fails.size() > 0) {
      logException(null, processor, LOG_EXCEPTION_UPDATE, table);
      throw new SQLException(UPDATE_ERROR_GENERAL);
    }

    return collection;
  }

  protected static void deleteFeatures(
      @NotNull PsqlHandler processor,
      List<FeatureCollection.ModificationFailure> fails,
      Map<String, String> deletes,
      Connection connection,
      boolean handleUUID,
      Integer version)
      throws SQLException {
    final String schema = processor.spaceSchema();
    final String table = processor.spaceTable();
    final PreparedStatement batchDeleteStmt = deleteStmtSQLStatement(connection, schema, table, handleUUID);
    final PreparedStatement batchDeleteStmtWithoutUUID = deleteStmtSQLStatement(connection, schema, table, false);

    /**
     * If versioning is enabled than we are going to perform an update instead of an delete. The
     * trigger will finally delete the row.
     */
    final PreparedStatement batchDeleteStmtVersioned =
        versionedDeleteStmtSQLStatement(connection, schema, table, handleUUID);
    final PreparedStatement batchDeleteStmtVersionedWithoutUUID =
        versionedDeleteStmtSQLStatement(connection, schema, table, false);

    Set<String> idsToDelete = deletes.keySet();

    // parameters for without-uuid scenario
    final List<String> deleteIdListWithoutUUID = new ArrayList<>();
    // parameters for versioned-without-uuid scenario
    final List<Integer> versionListWithoutUUID = new ArrayList<>();
    // parameters for with-uuid scenario
    List<String> deleteIdList = new ArrayList<>();
    final List<String> deleteUuidList = handleUUID ? new ArrayList<>() : null;
    // parameters for versioned-with-uuid scenario
    final List<Integer> versionList = new ArrayList<>();
    final List<String> versionedUuidList = new ArrayList<>();

    for (String deleteId : idsToDelete) {
      final String puuid = deletes.get(deleteId);

      if (version == null) {
        if (handleUUID && puuid == null) {
          deleteIdListWithoutUUID.add(deleteId);
        } else {
          deleteIdList.add(deleteId);
          if (handleUUID) {
            deleteUuidList.add(puuid);
          }
        }
      } else {
        if (handleUUID && puuid == null) {
          versionListWithoutUUID.add(version);
          deleteIdListWithoutUUID.add(deleteId);
        } else {
          versionList.add(version);
          deleteIdList.add(deleteId);
          if (handleUUID) {
            versionedUuidList.add(puuid);
          }
        }
      }
    }
    if (version != null) {
      if (deleteIdListWithoutUUID.size() > 0) {
        batchDeleteStmtVersionedWithoutUUID.setArray(
            1, connection.createArrayOf("bigint", versionListWithoutUUID.toArray()));
        batchDeleteStmtVersionedWithoutUUID.setArray(
            2, connection.createArrayOf("text", deleteIdListWithoutUUID.toArray()));
      }
      if (deleteIdList.size() > 0) {
        batchDeleteStmtVersioned.setArray(1, connection.createArrayOf("bigint", versionList.toArray()));
        batchDeleteStmtVersioned.setArray(2, connection.createArrayOf("text", deleteIdList.toArray()));
        batchDeleteStmtVersioned.setArray(
            3, handleUUID ? connection.createArrayOf("text", versionedUuidList.toArray()) : null);
      }
      executeBatchesAndCheckOnFailures(
          processor,
          deleteIdList,
          deleteIdListWithoutUUID,
          batchDeleteStmtVersioned,
          batchDeleteStmtVersionedWithoutUUID,
          null,
          null,
          fails,
          handleUUID,
          TYPE_DELETE,
          table);

    } else {
      executeBatchesAndCheckOnFailures(
          processor,
          deleteIdList,
          deleteIdListWithoutUUID,
          batchDeleteStmt,
          batchDeleteStmtWithoutUUID,
          null,
          null,
          fails,
          handleUUID,
          TYPE_DELETE,
          table);
    }

    if (fails.size() > 0) {
      logException(null, processor, LOG_EXCEPTION_DELETE, table);
      throw new SQLException(DELETE_ERROR_GENERAL);
    }
  }

  private static void executeBatchesAndCheckOnFailures(
      @NotNull PsqlHandler processor,
      List<String> idList,
      List<String> idList2,
      PreparedStatement batchStmt,
      PreparedStatement batchStmt2,
      final List<Feature> featureList,
      final List<Feature> featureWithoutGeoList,
      List<FeatureCollection.ModificationFailure> fails,
      boolean handleUUID,
      int type,
      final String table)
      throws SQLException {

    try {
      final long startTS = System.currentTimeMillis();
      if (idList.size() > 0) {
        currentLogger().debug("batch execution [{}]: {} ", type, batchStmt);

        batchStmt.setQueryTimeout((int) processor.calculateTimeout());
        batchStmt.execute();
        ResultSet rs = batchStmt.getResultSet();
        fillFeatureListAndFailList(rs, featureList, fails, idList, handleUUID, type);
        if (rs != null) rs.close();
      }

      if (idList2.size() > 0) {
        currentLogger().debug("batch2 execution [{}]: {} ", type, batchStmt2);

        batchStmt2.setQueryTimeout((int) processor.calculateTimeout());
        batchStmt2.execute();
        ResultSet rs = batchStmt2.getResultSet();
        fillFeatureListAndFailList(rs, featureWithoutGeoList, fails, idList2, handleUUID, type);
        if (rs != null) rs.close();
      }
      final long duration = System.currentTimeMillis() - startTS;
      currentLogger()
          .info(
              "Transactional DB Operation Stats [format => eventType,table,opType,timeTakenMs] - {} {} {} {}",
              "DBOperationStats",
              table,
              type,
              duration);
    } finally {
      batchStmt.close();
      batchStmt2.close();
    }
  }

  private static void fillFeatureListAndFailList(
      final ResultSet rs,
      final List<Feature> featureList,
      List<FeatureCollection.ModificationFailure> fails,
      List<String> idList,
      boolean handleUUID,
      int type)
      throws SQLException {
    // Function populates:
    //      - xyz namespace as obtained from DB into feature "collection"
    //      - creates "fails" list including features for which UPDATE got failed
    if (rs == null || !rs.next()) {
      throw new SQLException("No result out of batch operation.");
    }
    final Boolean[] successArr = (Boolean[]) rs.getArray("success").getArray();
    final String[] xyzNsArr = (String[]) rs.getArray("xyz_ns").getArray();
    final String[] errMsgArr = (String[]) rs.getArray("err_msg").getArray();

    for (int i = 0, max = successArr.length; i < max; i++) {
      if (!successArr[i]) {
        String message = TRANSACTION_ERROR_GENERAL;
        switch (type) {
          case TYPE_INSERT:
            message = INSERT_ERROR_GENERAL;
            break;
          case TYPE_UPDATE:
            message = handleUUID ? UPDATE_ERROR_UUID : UPDATE_ERROR_NOT_EXISTS;
            break;
          case TYPE_DELETE:
            message = handleUUID ? DELETE_ERROR_UUID : DELETE_ERROR_NOT_EXISTS;
            break;
        }
        final String featureId = idList.get(i);
        currentLogger()
            .warn(
                "DB operation type [{}] failed for id [{}], with error [{}]",
                type,
                featureId,
                errMsgArr[i]);
        fails.add(new FeatureCollection.ModificationFailure()
            .withId(featureId)
            .withMessage(message));
      } else {
        if (featureList != null) {
          saveXyzNamespaceInFeature(featureList.get(i), xyzNsArr[i]);
        }
      }
    }
  }
}
