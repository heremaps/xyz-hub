/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.io.WKBWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PGobject;

public class DatabaseWriter {

    private static final int TYPE_INSERT = 1;
    private static final int TYPE_UPDATE = 2;
    private static final int TYPE_DELETE = 3;
    private static final Logger logger = LogManager.getLogger();

    private static final String UPDATE_ERROR_GENERAL = "Update has failed";
    public static final String UPDATE_ERROR_NOT_EXISTS = UPDATE_ERROR_GENERAL+" - Object does not exist";
    public static final String UPDATE_ERROR_UUID = UPDATE_ERROR_GENERAL+" - Object does not exist or UUID mismatch";
    private static final String UPDATE_ERROR_ID_MISSING = UPDATE_ERROR_GENERAL+" - Feature Id is missing";
    private static final String UPDATE_ERROR_PUUID_MISSING = UPDATE_ERROR_GENERAL+" -  Feature puuid is missing";

    private static final String DELETE_ERROR_GENERAL = "Delete has failed";
    public static final String DELETE_ERROR_NOT_EXISTS = DELETE_ERROR_GENERAL+" - Object does not exist";
    public static final String DELETE_ERROR_UUID = DELETE_ERROR_GENERAL+" - Object does not exist or UUID mismatch";

    public static final String INSERT_ERROR_GENERAL = "Insert has failed";

    protected static final String TRANSACTION_ERROR_GENERAL = "Transaction has failed";

    private static final String LOG_EXCEPTION_INSERT = "insert";
    private static final String LOG_EXCEPTION_UPDATE = "update";
    private static final String LOG_EXCEPTION_DELETE = "delete";

    private static PGobject featureToPGobject(final Feature feature, Integer version) throws SQLException {
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

    private static void fillDeleteQueryFromDeletion(SQLQuery query, Entry<String, String> deletion, boolean handleUUID) {
        query.setNamedParameter("id", deletion.getKey());
        if (handleUUID)
            query.setNamedParameter("puuid", deletion.getValue());
    }

    private static void fillUpdateQueryFromFeature(SQLQuery query, Feature feature, boolean handleUUID, Integer version) throws SQLException {
        if (feature.getId() == null)
            throw new WriteFeatureException(UPDATE_ERROR_ID_MISSING);

        final String puuid = feature.getProperties().getXyzNamespace().getPuuid();
        if (handleUUID && puuid == null)
            throw new WriteFeatureException(UPDATE_ERROR_PUUID_MISSING);

        fillInsertQueryFromFeature(query, feature, version);
        if (handleUUID)
            query.setNamedParameter("puuid", puuid);
    }

    private static void fillInsertQueryFromFeature(SQLQuery query, Feature feature, Integer version) throws SQLException {
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

    private static boolean getDeletedFlagFromFeature(Feature f) {
        return f.getProperties() == null ? false :
            f.getProperties().getXyzNamespace() == null ? false :
            f.getProperties().getXyzNamespace().isDeleted();
    }

    protected static FeatureCollection insertFeatures(DatabaseHandler dbh, ModifyFeaturesEvent event, TraceItem traceItem, FeatureCollection collection,
                                                      List<FeatureCollection.ModificationFailure> fails,
                                                      List<Feature> inserts, Connection connection, Integer version)
            throws SQLException, JsonProcessingException {
        boolean transactional = event.getTransaction();
        connection.setAutoCommit(!transactional);
        SQLQuery insertQuery = SQLQueryBuilder.buildInsertStmtQuery(dbh, event);

        List<String> insertIdList = transactional ? new ArrayList<>() : null;

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
                        throw new WriteFeatureException(INSERT_ERROR_GENERAL);
                }
                catch (Exception e) {
                    if (transactional)
                        throw e;

                    if (e instanceof SQLException && "42P01".equalsIgnoreCase(((SQLException) e).getSQLState()))
                        throw (SQLException) e;

                    fails.add(new FeatureCollection.ModificationFailure().withId(feature.getId())
                        .withMessage(e instanceof WriteFeatureException ? e.getMessage() : INSERT_ERROR_GENERAL));
                    logException(e, traceItem, LOG_EXCEPTION_INSERT, dbh, event);
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
                                                      List<Feature> updates, Connection connection, Integer version)
            throws SQLException, JsonProcessingException {
        boolean transactional = event.getTransaction();
        connection.setAutoCommit(!transactional);
        SQLQuery updateQuery = SQLQueryBuilder.buildUpdateStmtQuery(dbh, event);

        List<String> updateIdList = new ArrayList<>();
        boolean handleUUID = event.getEnableUUID();

        try {
            for (Feature feature : updates) {
                try {
                    fillUpdateQueryFromFeature(updateQuery, feature, handleUUID, version);
                    PreparedStatement ps = updateQuery.prepareStatement(connection);

                    if (transactional) {
                        ps.addBatch();
                        updateIdList.add(feature.getId());
                    }
                    else
                        ps.setQueryTimeout(dbh.calculateTimeout());

                    if (transactional || ps.executeUpdate() != 0)
                        collection.getFeatures().add(feature);
                    else
                        throw new WriteFeatureException(handleUUID ? UPDATE_ERROR_UUID : UPDATE_ERROR_NOT_EXISTS);
                }
                catch (Exception e) {
                    if (transactional)
                        throw e;

                    //TODO: Handle SQL state "42P01"? (see: #insertFeatures())

                    fails.add(new FeatureCollection.ModificationFailure().withId(feature.getId())
                        .withMessage(e instanceof WriteFeatureException ? e.getMessage() : UPDATE_ERROR_GENERAL));
                    logException(e, traceItem, LOG_EXCEPTION_UPDATE, dbh, event);
                }
            }

            if (transactional) {
                executeBatchesAndCheckOnFailures(dbh, updateIdList, updateQuery.prepareStatement(connection), fails, handleUUID, TYPE_UPDATE,
                    traceItem);

                if (fails.size() > 0) {
                    logException(null, traceItem, LOG_EXCEPTION_UPDATE, dbh, event);
                    throw new SQLException(UPDATE_ERROR_GENERAL);
                }
            }
        }
        finally {
            updateQuery.closeStatement();
        }

        return collection;
    }

    protected static void deleteFeatures(DatabaseHandler dbh, ModifyFeaturesEvent event, TraceItem traceItem,
                                                      List<FeatureCollection.ModificationFailure> fails,
                                                      Map<String, String> deletes, Connection connection, Integer version)
            throws SQLException {
        boolean transactional = event.getTransaction();
        connection.setAutoCommit(!transactional);
        //If versioning is enabled, perform an update instead of a deletion. The trigger will finally delete the row.
        SQLQuery deleteQuery = SQLQueryBuilder.buildDeleteStmtQuery(dbh, event, version);

        List<String> deleteIdList = new ArrayList<>();
        boolean handleUUID = event.getEnableUUID();

        try {
            for (Entry<String, String> deletion : deletes.entrySet()) {
                try {
                    fillDeleteQueryFromDeletion(deleteQuery, deletion, handleUUID);
                    PreparedStatement ps = deleteQuery.prepareStatement(connection);

                    if (transactional) {
                        ps.addBatch();
                        deleteIdList.add(deletion.getKey());
                    }
                    else {
                        ps.setQueryTimeout(dbh.calculateTimeout());
                        if (ps.executeUpdate() == 0)
                            throw new WriteFeatureException(handleUUID ? DELETE_ERROR_UUID : DELETE_ERROR_NOT_EXISTS);
                    }

                }
                catch (Exception e) {
                    if (transactional)
                        throw e;

                    //TODO: Handle SQL state "42P01"? (see: #insertFeatures())

                    fails.add(new FeatureCollection.ModificationFailure().withId(deletion.getKey())
                        .withMessage(e instanceof WriteFeatureException ? e.getMessage() : DELETE_ERROR_GENERAL));
                    logException(e, traceItem, LOG_EXCEPTION_DELETE, dbh, event);
                }
            }

            if (transactional) {
                executeBatchesAndCheckOnFailures(dbh, deleteIdList, deleteQuery.prepareStatement(connection), fails, handleUUID, TYPE_DELETE,
                    traceItem);

                if (fails.size() > 0) {
                    logException(null, traceItem, LOG_EXCEPTION_DELETE, dbh, event);
                    throw new SQLException(DELETE_ERROR_GENERAL);
                }
            }
        }
        finally {
            deleteQuery.closeStatement();
        }
    }

    private static class WriteFeatureException extends RuntimeException {
        WriteFeatureException(String message) {
            super(message);
        }
    }

    private static void assure3d(Coordinate[] coords){
        for (Coordinate coord : coords){
            if(Double.valueOf(coord.z).isNaN())
                coord.z= 0;
        }
    }

    private static void logException(Exception e, TraceItem traceItem, String action, DatabaseHandler dbHandler, ModifyFeaturesEvent event){
        String table = dbHandler.config.readTableFromEvent(event);
        if(e != null && e.getMessage() != null && e.getMessage().contains("does not exist")) {
            /* If table not yet exist */
            logger.info("{} Failed to perform {} - table {} does not exists {}", traceItem, action, table, e);
        }
        else
            logger.info("{} Failed to perform {} on table {} {}", traceItem, action, table, e);
    }

    private static void executeBatchesAndCheckOnFailures(DatabaseHandler dbh, List<String> idList, PreparedStatement batchStmt,
        List<FeatureCollection.ModificationFailure> fails,
        boolean handleUUID, int type, TraceItem traceItem) throws SQLException {
        int[] batchStmtResult;

        try {
            if (idList.size() > 0) {
                logger.debug("{} batch execution [{}]: {} ", traceItem, type, batchStmt);

                batchStmt.setQueryTimeout(dbh.calculateTimeout());
                batchStmtResult = batchStmt.executeBatch();
                DatabaseWriter.fillFailList(batchStmtResult, fails, idList, handleUUID, type);
            }
        }
        finally {
            if (batchStmt != null)
                batchStmt.close();
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
