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
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.io.WKBWriter;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DatabaseWriter {

    protected static final int TYPE_INSERT = 1;
    protected static final int TYPE_UPDATE = 2;
    protected static final int TYPE_DELETE = 3;
    protected static final Logger logger = LogManager.getLogger();

    public static final String UPDATE_ERROR_GENERAL = "Update has failed";
    public static final String UPDATE_ERROR_NOT_EXISTS = UPDATE_ERROR_GENERAL+" - Object does not exist";
    public static final String UPDATE_ERROR_UUID = UPDATE_ERROR_GENERAL+" - Object does not exist or UUID mismatch";
    public static final String UPDATE_ERROR_ID_MISSING = UPDATE_ERROR_GENERAL+" - Feature Id is missing";
    public static final String UPDATE_ERROR_PUUID_MISSING = UPDATE_ERROR_GENERAL+" -  Feature puuid is missing";

    public static final String DELETE_ERROR_GENERAL = "Delete has failed";
    public static final String DELETE_ERROR_NOT_EXISTS = DELETE_ERROR_GENERAL+" - Object does not exist";
    public static final String DELETE_ERROR_UUID = DELETE_ERROR_GENERAL+" - Object does not exist or UUID mismatch";

    public static final String INSERT_ERROR_GENERAL = "Insert has failed";

    protected static final String TRANSACTION_ERROR_GENERAL = "Transaction has failed";

    public static final String LOG_EXCEPTION_INSERT = "insert";
    public static final String LOG_EXCEPTION_UPDATE = "update";
    public static final String LOG_EXCEPTION_DELETE = "delete";

    protected static PGobject featureToPGobject(final Feature feature, Integer version) throws SQLException {
        final Geometry geometry = feature.getGeometry();
        feature.setGeometry(null); // Do not serialize the geometry in the JSON object

        final String json;

        try {
            if(version != null)
                feature.getProperties().getXyzNamespace().setVersion(version);
            json = feature.serialize();
        } finally {
            feature.setGeometry(geometry);
        }

        final PGobject jsonbObject = new PGobject();
        jsonbObject.setType("jsonb");
        jsonbObject.setValue(json);
        return jsonbObject;
    }

    protected static void fillUpdateQueryFromFeature(SQLQuery query, Feature feature, boolean handleUUID) throws SQLException {
        fillUpdateQueryFromFeature(query, feature, handleUUID, null);
    }

    protected static void fillUpdateQueryFromFeature(SQLQuery query, Feature feature, boolean handleUUID, Integer version) throws SQLException {
        fillInsertQueryFromFeature(query, feature, version);
        if (handleUUID)
            query.setNamedParameter("puuid", feature.getProperties().getXyzNamespace().getPuuid());
    }

    protected static void fillInsertQueryFromFeature(SQLQuery query, Feature feature, Integer version) throws SQLException {
        query
            .withNamedParameter("id", feature.getId())
            .withNamedParameter("rev", feature.getProperties().getXyzNamespace().getRev())
            .withNamedParameter("operation", getDeletedFlagFromFeature(feature) ? 'D' : 'I')
            .withNamedParameter("jsondata", featureToPGobject(feature, version));

        Geometry geo = feature.getGeometry();
        if (geo != null) {
            //Avoid NaN values
            assure3d(geo.getJTSGeometry().getCoordinates());
            query.setNamedParameter("geo", new WKBWriter(3).write(geo.getJTSGeometry()));
        }
        else
            query.setNamedParameter("geo", null);

    }

    protected static boolean getDeletedFlagFromFeature(Feature f) {
        return f.getProperties() == null ? false :
            f.getProperties().getXyzNamespace() == null ? false :
            f.getProperties().getXyzNamespace().isDeleted();
    }

    protected static PreparedStatement createStatement(Connection connection, String statement) throws SQLException {
        final PreparedStatement preparedStatement = connection.prepareStatement(statement);
        return preparedStatement;
    }

    protected static PreparedStatement deleteStmtSQLStatement(Connection connection, String schema, String table, boolean handleUUID)
            throws SQLException {
        return createStatement(connection, SQLQueryBuilder.deleteStmtSQL(schema,table,handleUUID));
    }

    protected static PreparedStatement versionedDeleteStmtSQLStatement(Connection connection, String schema, String table, boolean handleUUID)
            throws SQLException {
        return createStatement(connection, SQLQueryBuilder.versionedDeleteStmtSQL(schema,table,handleUUID));
    }

    protected static FeatureCollection insertFeatures(DatabaseHandler dbh, ModifyFeaturesEvent event, TraceItem traceItem, FeatureCollection collection,
                                                      List<FeatureCollection.ModificationFailure> fails,
                                                      List<Feature> inserts, Connection connection, Integer version)
            throws SQLException, JsonProcessingException {
        String schema = dbh.config.getDatabaseSettings().getSchema();
        String table = dbh.config.readTableFromEvent(event);
        boolean transactional = event.getTransaction();

        List<String> insertIdList = transactional ? new ArrayList<>() : null;
        connection.setAutoCommit(!transactional);
        SQLQuery insertQuery = SQLQueryBuilder.buildInsertStmtQuery(schema, table);

        try {
            for (final Feature feature : inserts) {
                try {
                    fillInsertQueryFromFeature(insertQuery, feature, version);
                    PreparedStatement ps = insertQuery.prepareStatement(connection);

                    if (transactional) {
                        ps.addBatch();
                        insertIdList.add(feature.getId());
                    }
                    else
                        ps.setQueryTimeout(dbh.calculateTimeout());

                    if (transactional || ps.executeUpdate() != 0)
                        collection.getFeatures().add(feature);
                    else
                        fails.add(new FeatureCollection.ModificationFailure().withId(feature.getId()).withMessage(INSERT_ERROR_GENERAL));
                }
                catch (Exception e) {
                    if (transactional)
                        throw e;

                    if (e instanceof SQLException && "42P01".equalsIgnoreCase(((SQLException) e).getSQLState()))
                        throw (SQLException) e;

                    fails.add(new FeatureCollection.ModificationFailure().withId(feature.getId()).withMessage(INSERT_ERROR_GENERAL));
                    logException(e, traceItem, LOG_EXCEPTION_INSERT, table);
                }
            }

            if (transactional)
                executeBatchesAndCheckOnFailures(dbh, insertIdList, insertQuery.prepareStatement(connection), fails, false, TYPE_INSERT,
                    traceItem);
        }
        finally {
            insertQuery.closeStatement();
        }

        return collection;
    }

    protected static FeatureCollection updateFeatures(DatabaseHandler dbh, ModifyFeaturesEvent event, TraceItem traceItem, FeatureCollection collection,
                                                      List<FeatureCollection.ModificationFailure> fails,
                                                      List<Feature> updates, Connection connection,
                                                      boolean handleUUID, Integer version)
            throws SQLException, JsonProcessingException {
        connection.setAutoCommit(!event.getTransaction());
        if (event.getTransaction())
            return DatabaseTransactionalWriter.updateFeatures(dbh, event, traceItem, collection, fails, updates, connection,handleUUID, version);
        return DatabaseStreamWriter.updateFeatures(dbh, event, traceItem, collection, fails, updates, connection, handleUUID);
    }

    protected static void deleteFeatures(DatabaseHandler dbh, ModifyFeaturesEvent event, TraceItem traceItem,
                                                      List<FeatureCollection.ModificationFailure> fails,
                                                      Map<String, String> deletes, Connection connection,
                                                      boolean handleUUID, Integer version)
            throws SQLException {
        connection.setAutoCommit(!event.getTransaction());
        if (event.getTransaction()) {
            DatabaseTransactionalWriter.deleteFeatures(dbh, event, traceItem, fails, deletes, connection ,handleUUID, version);
            return;
        }
        DatabaseStreamWriter.deleteFeatures(dbh, event, traceItem, fails, deletes, connection, handleUUID);
    }

    private static void assure3d(Coordinate[] coords){
        for (Coordinate coord : coords){
            if(Double.valueOf(coord.z).isNaN())
                coord.z= 0;
        }
    }

    protected static void logException(Exception e, TraceItem traceItem, String action, String table){
        if(e != null && e.getMessage() != null && e.getMessage().contains("does not exist")) {
            /* If table not yet exist */
            logger.info("{} Failed to perform {} - table {} does not exists {}", traceItem, action, table, e);
        }
        else
            logger.info("{} Failed to perform {} on table {} {}", traceItem, action, table, e);
    }

    protected static void executeBatchesAndCheckOnFailures(DatabaseHandler dbh, List<String> idList, PreparedStatement batchStmt,
        List<FeatureCollection.ModificationFailure> fails,
        boolean handleUUID, int type, TraceItem traceItem) throws SQLException {
        DatabaseWriter.executeBatchesAndCheckOnFailures(dbh, idList, Collections.emptyList(), batchStmt, null, fails, handleUUID, type, traceItem);
    }

    protected static void executeBatchesAndCheckOnFailures(DatabaseHandler dbh, List<String> idList, List<String> idList2,
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
                DatabaseWriter.fillFailList(batchStmtResult, fails, idList, handleUUID, type);
            }

            if (idList2.size() > 0) {
                logger.debug("{} batch2 execution [{}]: {} ", traceItem, type, batchStmt2);

                batchStmt2.setQueryTimeout(dbh.calculateTimeout());
                batchStmtResult2 = batchStmt2.executeBatch();
                DatabaseWriter.fillFailList(batchStmtResult2, fails, idList2, handleUUID, type);
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
