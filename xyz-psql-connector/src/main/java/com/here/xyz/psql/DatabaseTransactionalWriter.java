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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.connectors.AbstractConnectorHandler.TraceItem;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DatabaseTransactionalWriter extends  DatabaseWriter{
    private static final Logger logger = LogManager.getLogger();

    private static final int TYPE_INSERT = 1;
    private static final int TYPE_UPDATE = 2;
    private static final int TYPE_DELETE = 3;

    public static FeatureCollection insertFeatures(DatabaseHandler dbh, String schema, String table, TraceItem traceItem,
                FeatureCollection collection, List<FeatureCollection.ModificationFailure> fails,
                List<Feature> inserts, Connection connection, Integer version)
            throws SQLException, JsonProcessingException {
        SQLQuery insertQuery = SQLQueryBuilder.buildInsertStmtQuery(schema, table);
        List<String> insertIdList = new ArrayList<>();

        for (final Feature feature : inserts) {
            fillInsertQueryFromFeature(insertQuery, feature, version);
            PreparedStatement ps = insertQuery.prepareStatement(connection);

            ps.addBatch();
            insertIdList.add(feature.getId());
            collection.getFeatures().add(feature);
        }

        executeBatchesAndCheckOnFailures(dbh, insertIdList, insertQuery.prepareStatement(connection), fails, false, TYPE_INSERT,
            traceItem);

        return collection;
    }

    public static FeatureCollection updateFeatures(DatabaseHandler dbh, String schema, String table, TraceItem traceItem, FeatureCollection collection,
                                                   List<FeatureCollection.ModificationFailure> fails, List<Feature> updates,
                                                   Connection connection, boolean handleUUID, Integer version, boolean forExtendedSpace)
            throws SQLException, JsonProcessingException {

        SQLQuery updateQuery = SQLQueryBuilder.buildUpdateStmtQuery(schema, table, handleUUID);
        List<String> updateIdList = new ArrayList<>();

        for (Feature feature : updates) {
            if (feature.getId() == null)
                throw new NullPointerException("id");

            fillUpdateQueryFromFeature(updateQuery, feature, handleUUID, version);
            PreparedStatement ps = updateQuery.prepareStatement(connection);

            ps.addBatch();
            updateIdList.add(feature.getId());
            collection.getFeatures().add(feature);
        }

        executeBatchesAndCheckOnFailures(dbh, updateIdList, updateQuery.prepareStatement(connection), fails, handleUUID, TYPE_UPDATE,
            traceItem);

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

    private static void executeBatchesAndCheckOnFailures(DatabaseHandler dbh, List<String> idList, PreparedStatement batchStmt,
        List<FeatureCollection.ModificationFailure> fails,
        boolean handleUUID, int type, TraceItem traceItem) throws SQLException {
        executeBatchesAndCheckOnFailures(dbh, idList, Collections.emptyList(), batchStmt, null, fails, handleUUID, type, traceItem);
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
            if (batchStmt != null)
                batchStmt.close();
            if (batchStmt2 != null)
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
