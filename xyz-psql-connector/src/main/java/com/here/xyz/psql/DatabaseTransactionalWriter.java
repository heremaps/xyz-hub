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
import com.here.xyz.models.txn.TransactionData;
import com.here.xyz.models.txn.TransactionLog;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PGobject;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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

        List<String> insertIdList = new ArrayList<>();
        List<String> insertWithoutGeometryIdList = new ArrayList<>();

        for (int i = 0; i < inserts.size(); i++) {
            final Feature feature = inserts.get(i);

            final PGobject jsonbObject= featureToPGobject(feature, version);

            if (feature.getGeometry() == null) {
                insertWithoutGeometryStmt.setObject(1, jsonbObject);
                if (forExtendedSpace)
                    insertWithoutGeometryStmt.setBoolean(2, getDeletedFlagFromFeature(feature));
                insertWithoutGeometryStmt.addBatch();
                insertWithoutGeometryIdList.add(feature.getId());
            } else {
                insertStmt.setObject(1, jsonbObject);

                final WKBWriter wkbWriter = new WKBWriter(3);
                Geometry jtsGeometry = feature.getGeometry().getJTSGeometry();
                //Avoid NAN values
                assure3d(jtsGeometry.getCoordinates());
                insertStmt.setBytes(2, wkbWriter.write(jtsGeometry));
                if (forExtendedSpace)
                    insertStmt.setBoolean(3, getDeletedFlagFromFeature(feature));

                insertStmt.addBatch();
                insertIdList.add(feature.getId());
            }
            collection.getFeatures().add(feature);
        }

        executeBatchesAndCheckOnFailures(dbh, insertIdList, insertWithoutGeometryIdList,
                insertStmt, insertWithoutGeometryStmt, fails, false, TYPE_INSERT, traceItem);

        return collection;
    }

    public static FeatureCollection updateFeatures(DatabaseHandler dbh, String schema, String table, TraceItem traceItem, FeatureCollection collection,
                                                   List<FeatureCollection.ModificationFailure> fails, List<Feature> updates,
                                                   Connection connection, boolean handleUUID, Integer version, boolean forExtendedSpace)
            throws SQLException, JsonProcessingException {

        final PreparedStatement updateStmt = createUpdateStatement(connection, schema, table, handleUUID, forExtendedSpace);
        final PreparedStatement updateWithoutGeometryStmt = createUpdateWithoutGeometryStatement(connection,schema,table,handleUUID, forExtendedSpace);

        List<String> updateIdList = new ArrayList<>();
        List<String> updateWithoutGeometryIdList = new ArrayList<>();

        for (int i = 0; i < updates.size(); i++) {
            final Feature feature = updates.get(i);
            final String puuid = feature.getProperties().getXyzNamespace().getPuuid();

            if (feature.getId() == null) {
                throw new NullPointerException("id");
            }

            final PGobject jsonbObject= featureToPGobject(feature, version);

            int paramIdx = 0;
            if (feature.getGeometry() == null) {
                updateWithoutGeometryStmt.setObject(++paramIdx, jsonbObject);
                if (forExtendedSpace)
                    updateWithoutGeometryStmt.setBoolean(++paramIdx, getDeletedFlagFromFeature(feature));
                updateWithoutGeometryStmt.setString(++paramIdx, feature.getId());
                if (handleUUID)
                    updateWithoutGeometryStmt.setString(++paramIdx, puuid);
                updateWithoutGeometryStmt.addBatch();

                updateWithoutGeometryIdList.add(feature.getId());
            } else {
                updateStmt.setObject(++paramIdx, jsonbObject);
                final WKBWriter wkbWriter = new WKBWriter(3);
                Geometry jtsGeometry = feature.getGeometry().getJTSGeometry();
                //Avoid NAN values
                assure3d(jtsGeometry.getCoordinates());
                updateStmt.setBytes(++paramIdx, wkbWriter.write(jtsGeometry));
                if (forExtendedSpace)
                    updateStmt.setBoolean(++paramIdx, getDeletedFlagFromFeature(feature));
                updateStmt.setString(++paramIdx, feature.getId());
                if (handleUUID)
                    updateStmt.setString(++paramIdx, puuid);
                updateStmt.addBatch();

                updateIdList.add(feature.getId());
            }
            collection.getFeatures().add(feature);
        }

        executeBatchesAndCheckOnFailures(dbh, updateIdList, updateWithoutGeometryIdList,
                updateStmt, updateWithoutGeometryStmt, fails, handleUUID, TYPE_UPDATE, traceItem);

        if(fails.size() > 0) {
            logException(null, traceItem, LOG_EXCEPTION_UPDATE, table);
            throw new SQLException(UPDATE_ERROR_GENERAL);
        }

        return collection;
    }

    protected static void deleteFeatures(DatabaseHandler dbh, String schema, String table, TraceItem traceItem,
                                         List<FeatureCollection.ModificationFailure> fails, Map<String, String> deletes,
                                         Connection connection, boolean handleUUID, Integer version)
            throws SQLException {

        final PreparedStatement batchDeleteStmt = deleteStmtSQLStatement(connection,schema,table,handleUUID);
        final PreparedStatement batchDeleteStmtWithoutUUID = deleteStmtSQLStatement(connection,schema,table,false);

        /** If versioning is enabled than we are going to perform an update instead of an delete. The trigger will finally delete the row.*/
        final PreparedStatement batchDeleteStmtVersioned =  versionedDeleteStmtSQLStatement(connection,schema,table,handleUUID);
        final PreparedStatement batchDeleteStmtVersionedWithoutUUID =  versionedDeleteStmtSQLStatement(connection,schema,table,false);

        Set<String> idsToDelete = deletes.keySet();

        List<String> deleteIdList = new ArrayList<>();
        List<String> deleteIdListWithoutUUID = new ArrayList<>();

        for (String deleteId : idsToDelete) {
            final String puuid = deletes.get(deleteId);

            if(version == null){
                if(handleUUID && puuid == null){
                    batchDeleteStmtWithoutUUID.setString(1, deleteId);
                    batchDeleteStmtWithoutUUID.addBatch();
                    deleteIdListWithoutUUID.add(deleteId);
                }
                else {
                    batchDeleteStmt.setString(1, deleteId);
                    if (handleUUID) {
                        batchDeleteStmt.setString(2, puuid);
                    }
                    deleteIdList.add(deleteId);
                    batchDeleteStmt.addBatch();
                }
            }else{
                if(handleUUID && puuid == null){
                    batchDeleteStmtVersionedWithoutUUID.setLong(1, version);
                    batchDeleteStmtVersionedWithoutUUID.setString(2, deleteId);
                    deleteIdListWithoutUUID.add(deleteId);
                    batchDeleteStmtVersionedWithoutUUID.addBatch();
                }
                else {
                    batchDeleteStmtVersioned.setLong(1, version);
                    batchDeleteStmtVersioned.setString(2, deleteId);
                    if (handleUUID) {
                        batchDeleteStmtVersioned.setString(3, puuid);
                    }
                    deleteIdList.add(deleteId);
                    batchDeleteStmtVersioned.addBatch();
                }
            }
        }
        if(version != null){
            executeBatchesAndCheckOnFailures(dbh, deleteIdList, deleteIdListWithoutUUID,
                    batchDeleteStmtVersioned, batchDeleteStmtVersionedWithoutUUID, fails, handleUUID, TYPE_DELETE, traceItem);

        }else{
            executeBatchesAndCheckOnFailures(dbh, deleteIdList, deleteIdListWithoutUUID,
                batchDeleteStmt, batchDeleteStmtWithoutUUID, fails, handleUUID, TYPE_DELETE, traceItem);
        }

        if(fails.size() > 0) {
            logException(null, traceItem, LOG_EXCEPTION_DELETE, table);
            throw new SQLException(DELETE_ERROR_GENERAL);
        }
    }

    public static void insertTransactionLog(DatabaseHandler dbh, String schema, String table,
                                             TraceItem traceItem, TransactionLog txnLog, Connection connection) throws SQLException, JsonProcessingException {

        final PreparedStatement insertStmt = createInsertStatementForTxnLog(connection, schema, table);

        if (txnLog.getUuids() == null || txnLog.getUuids().isEmpty())
            return;

        // Bind prepared statement and add to batch
        insertStmt.setString(1, txnLog.getSpace_id());
        insertStmt.setArray(2, connection.createArrayOf("text", txnLog.getUuids().toArray()));
        insertStmt.addBatch();

        executeBatchForTransactionDetails(dbh, insertStmt, TYPE_INSERT, traceItem);
    }

    public static void insertTransactionData(DatabaseHandler dbh, String schema, String table,
                      TraceItem traceItem, TransactionLog txnLog, Connection connection) throws SQLException, JsonProcessingException {

        final PreparedStatement insertStmt = createInsertStatementForTxnData(connection, schema, table);

        if (txnLog.getTxnDataList() == null || txnLog.getTxnDataList().isEmpty())
            return;

        for (final TransactionData txnData : txnLog.getTxnDataList()) {
            final PGobject jsonObject = new PGobject();
            jsonObject.setType("jsonb");
            jsonObject.setValue(txnData.getJsondata());
            // Bind prepared statement and add to batch
            insertStmt.setString(1, txnData.getUuid());
            insertStmt.setString(2, txnData.getOperation());
            insertStmt.setObject(3, jsonObject);
            insertStmt.addBatch();
        }

        executeBatchForTransactionDetails(dbh, insertStmt, TYPE_INSERT, traceItem);
    }

    private static void executeBatchForTransactionDetails(DatabaseHandler dbh, PreparedStatement batchStmt,
                               int type, TraceItem traceItem) throws SQLException {

        logger.debug("{} batch execution [{}]: {} ", traceItem, type, batchStmt);

        try {
            batchStmt.setQueryTimeout(dbh.calculateTimeout());
            final int[] batchStmtResult = batchStmt.executeBatch();
            for (int i= 0; i < batchStmtResult.length; i++) {
                if(batchStmtResult[i] == 0 ) {
                    throw new SQLException("Exception populating transaction log entries");
                }
            }
        } catch (Exception ex) {
            logger.error("{} Exception populating transaction log entries. ", traceItem, ex);
            throw new SQLException("Exception populating transaction log entries");
        }
        finally {
            batchStmt.close();
        }
    }

    private static void executeBatchesAndCheckOnFailures(DatabaseHandler dbh, List<String> idList, List<String> idList2,
                                                         PreparedStatement batchStmt, PreparedStatement batchStmt2,
                                                         List<FeatureCollection.ModificationFailure> fails,
                                                         boolean handleUUID, int type, TraceItem traceItem) throws SQLException {
        int[] batchStmtResult;
        int[] batchStmtResult2;

        try {
            if (idList.size() > 0) {
                logger.debug("{} batch execution [{}]: {} ", traceItem, type, batchStmt);

                batchStmt.setQueryTimeout(dbh.calculateTimeout());
                batchStmtResult = batchStmt.executeBatch();
                fillFailList(batchStmtResult, fails, idList, handleUUID, type);
            }

            if (idList2.size() > 0) {
                logger.debug("{} batch2 execution [{}]: {} ", traceItem, type, batchStmt2);

                batchStmt2.setQueryTimeout(dbh.calculateTimeout());
                batchStmtResult2 = batchStmt2.executeBatch();
                fillFailList(batchStmtResult2, fails, idList2, handleUUID, type);
            }
        }finally {
            batchStmt.close();
            batchStmt2.close();
        }
    }

    private static void fillFailList(int[] batchResult, List<FeatureCollection.ModificationFailure> fails,  List<String> idList, boolean handleUUID, int type){
        for (int i= 0; i < batchResult.length; i++) {
            if(batchResult[i] == 0 ) {
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
        }
    }
}
