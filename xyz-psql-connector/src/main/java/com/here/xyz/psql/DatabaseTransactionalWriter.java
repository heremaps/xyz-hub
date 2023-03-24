/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.psql;

import com.here.xyz.connectors.AbstractConnectorHandler.TraceItem;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PGobject;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DatabaseTransactionalWriter extends  DatabaseWriter{
    private static final Logger logger = LogManager.getLogger();

    private static final int TYPE_INSERT = 1;
    private static final int TYPE_UPDATE = 2;
    private static final int TYPE_DELETE = 3;

    public static FeatureCollection insertFeatures(DatabaseHandler dbh, String schema, String table, TraceItem traceItem,
                FeatureCollection collection, List<FeatureCollection.ModificationFailure> fails,
                List<Feature> inserts, Connection connection, Integer version, boolean forExtendedSpace)
            throws SQLException, JsonProcessingException {

        final PreparedStatement insertStmt = createInsertStatement(connection, schema, table, forExtendedSpace);
        final PreparedStatement insertWithoutGeometryStmt = createInsertWithoutGeometryStatement(connection, schema, table, forExtendedSpace);

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

            final PGobject jsonbObject= featureToPGobject(feature, version);

            if (feature.getGeometry() == null) {
                /*if (forExtendedSpace)
                    insertWithoutGeometryStmt.setBoolean(2, getDeletedFlagFromFeature(feature));*/
                insertWithoutGeoJsonbObjectList.add(jsonbObject);
                insertWithoutGeometryIdList.add(feature.getId());
                featureWithoutGeoList.add(feature);
            } else {
                insertJsonbObjectList.add(jsonbObject);
                Geometry jtsGeometry = feature.getGeometry().getJTSGeometry();
                //Avoid NAN values
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
            insertWithoutGeometryStmt.setArray(1, connection.createArrayOf("jsonb", insertWithoutGeoJsonbObjectList.toArray()));
        }
        if (insertIdList.size() > 0) {
            insertStmt.setArray(1, connection.createArrayOf("jsonb", insertJsonbObjectList.toArray()));
            insertStmt.setArray(2, connection.createArrayOf("geometry", insertGeometryList.toArray()));
        }

        executeBatchesAndCheckOnFailures(dbh, insertIdList, insertWithoutGeometryIdList,
                insertStmt, insertWithoutGeometryStmt, featureList, featureWithoutGeoList, fails, false, TYPE_INSERT, traceItem);

        return collection;
    }

    public static FeatureCollection updateFeatures(DatabaseHandler dbh, String schema, String table, TraceItem traceItem, FeatureCollection collection,
                                                   List<FeatureCollection.ModificationFailure> fails, List<Feature> updates,
                                                   Connection connection, boolean handleUUID, Integer version, boolean forExtendedSpace)
            throws SQLException, JsonProcessingException {

        final PreparedStatement updateStmt = createUpdateStatement(connection, schema, table, handleUUID, forExtendedSpace);
        final PreparedStatement updateWithoutGeometryStmt = createUpdateWithoutGeometryStatement(connection,schema,table,handleUUID, forExtendedSpace);

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

            if (feature.getId() == null) {
                throw new NullPointerException("id");
            }

            final PGobject jsonbObject= featureToPGobject(feature, version);

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
                //Avoid NAN values
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
            updateWithoutGeometryStmt.setArray(1, connection.createArrayOf("text", updateWithoutGeometryIdList.toArray()));
            updateWithoutGeometryStmt.setArray(2, handleUUID ? connection.createArrayOf("text", updateWithoutGeoUuidList.toArray()) : null);
            updateWithoutGeometryStmt.setArray(3, connection.createArrayOf("jsonb", updateWithoutGeoJsonbObjectList.toArray()));
        }
        if (updateIdList.size() > 0) {
            updateStmt.setArray(1, connection.createArrayOf("text", updateIdList.toArray()));
            updateStmt.setArray(2, handleUUID ? connection.createArrayOf("text", updateUuidList.toArray()) : null);
            updateStmt.setArray(3, connection.createArrayOf("jsonb", updateJsonbObjectList.toArray()));
            updateStmt.setArray(4, connection.createArrayOf("geometry", updateGeometryList.toArray()));
        }

        executeBatchesAndCheckOnFailures(dbh, updateIdList, updateWithoutGeometryIdList,
                updateStmt, updateWithoutGeometryStmt, featureList, featureWithoutGeoList, fails, handleUUID, TYPE_UPDATE, traceItem);

        if(fails.size() > 0) {
            logException(null, traceItem, LOG_EXCEPTION_UPDATE, table);
            throw new SQLException(UPDATE_ERROR_GENERAL);
        }

        return collection;
    }

    protected static void deleteFeatures(DatabaseHandler dbh, String schema, String table, TraceItem traceItem,
                                         List<FeatureCollection.ModificationFailure> fails, Map<String, String> deletes,
                                         Connection connection, boolean handleUUID, Integer version)
            throws SQLException, JsonProcessingException {

        final PreparedStatement batchDeleteStmt = deleteStmtSQLStatement(connection,schema,table,handleUUID);
        final PreparedStatement batchDeleteStmtWithoutUUID = deleteStmtSQLStatement(connection,schema,table,false);

        /** If versioning is enabled than we are going to perform an update instead of an delete. The trigger will finally delete the row.*/
        final PreparedStatement batchDeleteStmtVersioned =  versionedDeleteStmtSQLStatement(connection,schema,table,handleUUID);
        final PreparedStatement batchDeleteStmtVersionedWithoutUUID =  versionedDeleteStmtSQLStatement(connection,schema,table,false);

        Set<String> idsToDelete = deletes.keySet();

        // parameters for without-uuid scenario
        final List<String> deleteIdListWithoutUUID = new ArrayList<>();
        // parameters for versioned-without-uuid scenario
        final List<Integer> versionListWithoutUUID = new ArrayList<>();
        // parameters for with-uuid scenario
        final List<String> deleteIdList = new ArrayList<>();
        final List<String> deleteUuidList = handleUUID ? new ArrayList<>() : null;
        // parameters for versioned-with-uuid scenario
        final List<Integer> versionList = new ArrayList<>();
        final List<String> versionedUuidList = new ArrayList<>();

        for (String deleteId : idsToDelete) {
            final String puuid = deletes.get(deleteId);

            if(version == null){
                if(handleUUID && puuid == null){
                    deleteIdListWithoutUUID.add(deleteId);
                }
                else {
                    deleteIdList.add(deleteId);
                    if (handleUUID) {
                        deleteUuidList.add(puuid);
                    }
                }
            }else{
                if(handleUUID && puuid == null){
                    versionListWithoutUUID.add(version);
                    deleteIdListWithoutUUID.add(deleteId);
                }
                else {
                    versionList.add(version);
                    deleteIdList.add(deleteId);
                    if (handleUUID) {
                        versionedUuidList.add(puuid);
                    }
                }
            }
        }
        if(version != null){
            if (deleteIdListWithoutUUID.size() > 0) {
                batchDeleteStmtVersionedWithoutUUID.setArray(1, connection.createArrayOf("bigint", versionListWithoutUUID.toArray()));
                batchDeleteStmtVersionedWithoutUUID.setArray(2, connection.createArrayOf("text", deleteIdListWithoutUUID.toArray()));
            }
            if (deleteIdList.size() > 0) {
                batchDeleteStmtVersioned.setArray(1, connection.createArrayOf("bigint", versionList.toArray()));
                batchDeleteStmtVersioned.setArray(2, connection.createArrayOf("text", deleteIdList.toArray()));
                batchDeleteStmtVersioned.setArray(3, handleUUID ? connection.createArrayOf("text", versionedUuidList.toArray()) : null);
            }
            executeBatchesAndCheckOnFailures(dbh, deleteIdList, deleteIdListWithoutUUID,
                    batchDeleteStmtVersioned, batchDeleteStmtVersionedWithoutUUID, null, null, fails, handleUUID, TYPE_DELETE, traceItem);

        }else{
            if (deleteIdListWithoutUUID.size() > 0) {
                batchDeleteStmtWithoutUUID.setArray(1, connection.createArrayOf("text", deleteIdListWithoutUUID.toArray()));
            }
            if (deleteIdList.size() > 0) {
                batchDeleteStmt.setArray(1, connection.createArrayOf("text", deleteIdList.toArray()));
                batchDeleteStmt.setArray(2, handleUUID ? connection.createArrayOf("text", deleteUuidList.toArray()) : null);
            }
            executeBatchesAndCheckOnFailures(dbh, deleteIdList, deleteIdListWithoutUUID,
                batchDeleteStmt, batchDeleteStmtWithoutUUID, null, null, fails, handleUUID, TYPE_DELETE, traceItem);
        }

        if(fails.size() > 0) {
            logException(null, traceItem, LOG_EXCEPTION_DELETE, table);
            throw new SQLException(DELETE_ERROR_GENERAL);
        }
    }

    private static void executeBatchesAndCheckOnFailures(DatabaseHandler dbh, List<String> idList, List<String> idList2,
                                 PreparedStatement batchStmt, PreparedStatement batchStmt2,
                                 final List<Feature> featureList, final List<Feature> featureWithoutGeoList,
                                 List<FeatureCollection.ModificationFailure> fails,
                                 boolean handleUUID, int type, TraceItem traceItem) throws SQLException, JsonProcessingException {

        try {
            if (idList.size() > 0) {
                logger.debug("{} batch execution [{}]: {} ", traceItem, type, batchStmt);

                batchStmt.setQueryTimeout(dbh.calculateTimeout());
                batchStmt.execute();
                ResultSet rs = batchStmt.getResultSet();
                fillFeatureListAndFailList(rs, featureList, fails, idList, handleUUID, type);
                if (rs!=null) rs.close();
            }

            if (idList2.size() > 0) {
                logger.debug("{} batch2 execution [{}]: {} ", traceItem, type, batchStmt2);

                batchStmt2.setQueryTimeout(dbh.calculateTimeout());
                batchStmt2.execute();
                ResultSet rs = batchStmt2.getResultSet();
                fillFeatureListAndFailList(rs, featureWithoutGeoList, fails, idList2, handleUUID, type);
                if (rs!=null) rs.close();
            }
        }finally {
            batchStmt.close();
            batchStmt2.close();
        }
    }

    private static void fillFeatureListAndFailList(final ResultSet rs, final List<Feature> featureList,
                        List<FeatureCollection.ModificationFailure> fails, List<String> idList,
                        boolean handleUUID, int type) throws SQLException, JsonProcessingException {
        // Function populates:
        //      - xyz namespace as obtained from DB into feature "collection"
        //      - creates "fails" list including features for which UPDATE got failed
        if (rs == null || !rs.next()) {
            throw new SQLException("No result out of batch operation.");
        }
        final Boolean[] successArr = (Boolean[])rs.getArray("success").getArray();
        final String[] xyzNsArr = (String[])rs.getArray("xyz_ns").getArray();

        for (int i=0, max=successArr.length; i<max; i++) {
            if (!successArr[i]) {
                String message = TRANSACTION_ERROR_GENERAL;
                switch (type){
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
                fails.add(new FeatureCollection.ModificationFailure().withId(idList.get(i)).withMessage(message));
            }
            else {
                if (featureList!=null) {
                    saveXyzNamespaceInFeature(featureList.get(i), xyzNsArr[i]);
                }
            }
        }
    }
}
