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

import static com.here.xyz.psql.DatabaseWriter.ModificationType.DELETE;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.INSERT;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.INSERT_HIDE_COMPOSITE;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.UPDATE;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.UPDATE_HIDE_COMPOSITE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.io.WKBWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PGobject;

public class DatabaseWriter {

    public enum ModificationType {
        INSERT("I"),
        INSERT_HIDE_COMPOSITE("H"),

        UPDATE("U"),
        UPDATE_HIDE_COMPOSITE("J"),

        DELETE("D");

        String shortValue;

        ModificationType(String shortValue) {
            this.shortValue = shortValue;
        }

        @Override
        public String toString() {
            return shortValue;
        }
    }
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

    private static PGobject featureToPGobject(ModifyFeaturesEvent event, final Feature feature, long version) throws SQLException {
        final Geometry geometry = feature.getGeometry();
        feature.setGeometry(null); // Do not serialize the geometry in the JSON object

        final String json;

        try {
            //NOTE: The following is a temporary implementation for backwards compatibility for old table structures
            if (event.isEnableGlobalVersioning() && version != -1)
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

    private static void fillDeleteQueryFromDeletion(SQLQuery query, Entry<String, String> deletion, ModifyFeaturesEvent event, long version)
        throws SQLException {
        /*
        NOTE: If versioning is activated for the space, always only inserts are performed,
        but special parameters are necessary to handle conflicts in deletion case.
        Also, the feature to be written during the insert operation has to be "mocked" to only contain the necessary information.
         */
        if (event.getVersionsToKeep() > 1) {
            Feature deletedFeature = new Feature()
                .withId(deletion.getKey())
                .withProperties(new Properties().withXyzNamespace(new XyzNamespace().withDeleted(true)));
            fillInsertQueryFromFeature(query, DELETE, deletedFeature, event, version);
        }
        else {
            //NOTE: The following is a temporary implementation for backwards compatibility for old table structures
            boolean oldTableStyle = true; //DatabaseHandler.readVersionsToKeep(event) < 1;
            query.setNamedParameter("id", deletion.getKey());
            if (event.getEnableUUID())
                if (oldTableStyle)
                    query.setNamedParameter("puuid", deletion.getValue());
                else //TODO: Check if we should not throw an exception in that case
                    query.setNamedParameter("baseVersion", deletion.getValue() != null ? Long.parseLong(deletion.getValue()) : null);
        }
    }

    private static void fillUpdateQueryFromFeature(SQLQuery query, Feature feature, ModifyFeaturesEvent event, long version) throws SQLException {
        if (feature.getId() == null)
            throw new WriteFeatureException(UPDATE_ERROR_ID_MISSING);

        //NOTE: The following is a temporary implementation for backwards compatibility for old table structures
        boolean oldTableStyle = true; //DatabaseHandler.readVersionsToKeep(event) < 1;
        final String puuid = feature.getProperties().getXyzNamespace().getPuuid();
        if (event.getEnableUUID())
            if (puuid == null && oldTableStyle)
                throw new WriteFeatureException(UPDATE_ERROR_PUUID_MISSING);
            else if (oldTableStyle)
                query.setNamedParameter("puuid", puuid);
            else
                query.setNamedParameter("baseVersion", feature.getProperties().getXyzNamespace().getVersion());

        /*
        NOTE: If versioning is activated for the space, always only inserts are performed,
        but special parameters are necessary to handle conflicts in update case.
         */

        fillInsertQueryFromFeature(query, UPDATE, feature, event, version);
    }

    private static void fillInsertQueryFromFeature(SQLQuery query, ModificationType action, Feature feature, ModifyFeaturesEvent event, long version) throws SQLException {
        query
            .withNamedParameter("id", feature.getId())
            .withNamedParameter("version", version)
            .withNamedParameter("operation", (action == DELETE || !getDeletedFlagFromFeature(feature) ? action
                : action == INSERT ? INSERT_HIDE_COMPOSITE : UPDATE_HIDE_COMPOSITE).shortValue)
            .withNamedParameter("jsondata", featureToPGobject(event, feature, version));

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

    protected static void modifyFeatures(DatabaseHandler dbh, ModifyFeaturesEvent event, ModificationType action,
        FeatureCollection collection, List<FeatureCollection.ModificationFailure> fails, List inputData, Connection connection,
        long version) throws SQLException, JsonProcessingException {
        boolean transactional = event.getTransaction();
        connection.setAutoCommit(!transactional);
        SQLQuery modificationQuery = buildModificationStmtQuery(dbh, event, action, version);

        List<String> idList = transactional ? new ArrayList<>() : null;

        try {
            for (final Object inputDatum : inputData) {
                try {
                    fillModificationQueryFromInput(modificationQuery, event, action, inputDatum, version);
                    PreparedStatement ps = modificationQuery.prepareStatement(connection);

                    if (transactional) {
                        ps.addBatch();
                        idList.add(getIdFromInput(action, inputDatum));
                    }
                    else
                        ps.setQueryTimeout(dbh.calculateTimeout());

                    if (transactional || ps.execute() || ps.getUpdateCount() != 0) {
                        if (action != DELETE)
                            collection.getFeatures().add((Feature) inputDatum);
                    }
                    else
                        throw new WriteFeatureException(getFailedRowErrorMsg(action, event));
                }
                catch (Exception e) {
                    if (transactional)
                        throw e;

                    if (e instanceof SQLException && "42P01".equalsIgnoreCase(((SQLException) e).getSQLState()))
                        throw (SQLException) e;

                    fails.add(new FeatureCollection.ModificationFailure().withId(getIdFromInput(action, inputDatum))
                        .withMessage(e instanceof WriteFeatureException ? e.getMessage() : getGeneralErrorMsg(action)));
                    logException(e, action, dbh, event);
                }
            }

            if (transactional) {
                executeBatchesAndCheckOnFailures(dbh, idList, modificationQuery.prepareStatement(connection), fails, event, action);

                if (action != INSERT && fails.size() > 0) { //Not necessary in INSERT case as no PUUID check is performed there
                    logException(null, action, dbh, event);
                    throw new SQLException(getGeneralErrorMsg(action));
                }
            }
        }
        finally {
            modificationQuery.closeStatement();
        }
    }

    private static String getFailedRowErrorMsg(ModificationType action, ModifyFeaturesEvent event) {
        switch (action) {
            case INSERT:
                return getGeneralErrorMsg(action);
            case UPDATE:
                return event.getEnableUUID() ? UPDATE_ERROR_UUID : UPDATE_ERROR_NOT_EXISTS;
            case DELETE:
                return event.getEnableUUID() ? DELETE_ERROR_UUID : DELETE_ERROR_NOT_EXISTS;
        }
        return null;
    }

    private static String getGeneralErrorMsg(ModificationType action) {
        switch (action) {
            case INSERT:
                return INSERT_ERROR_GENERAL;
            case UPDATE:
                return UPDATE_ERROR_GENERAL;
            case DELETE:
                return DELETE_ERROR_GENERAL;
        }
        return null;
    }

    private static String getIdFromInput(ModificationType action, Object inputDatum) {
        switch (action) {
            case INSERT:
            case UPDATE:
                return ((Feature) inputDatum).getId();
            case DELETE:
                return ((Entry<String, String>) inputDatum).getKey();
        }
        return null;
    }

    private static SQLQuery buildModificationStmtQuery(DatabaseHandler dbHandler, ModifyFeaturesEvent event, ModificationType action,
        long version) {
        //If versioning is activated for the space, always only perform inserts
        if (event.getVersionsToKeep() > 1)
            return SQLQueryBuilder.buildMultiModalInsertStmtQuery(dbHandler, event);
        switch (action) {
            case INSERT:
                return SQLQueryBuilder.buildInsertStmtQuery(dbHandler, event);
            case UPDATE:
                return SQLQueryBuilder.buildUpdateStmtQuery(dbHandler, event);
            case DELETE:
                return SQLQueryBuilder.buildDeleteStmtQuery(dbHandler, event, version);
        }
        return null;
    }

    private static void fillModificationQueryFromInput(SQLQuery query, ModifyFeaturesEvent event, ModificationType action, Object inputDatum,
        long version) throws SQLException {
        switch (action) {
            case INSERT: {
                fillInsertQueryFromFeature(query, action, (Feature) inputDatum, event, version);
                break;
            }
            case UPDATE: {
                fillUpdateQueryFromFeature(query, (Feature) inputDatum, event, version);
                break;
            }
            case DELETE: {
                fillDeleteQueryFromDeletion(query, (Entry<String, String>) inputDatum, event, version);
            }
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

    private static void logException(Exception e, ModificationType action, DatabaseHandler dbHandler, ModifyFeaturesEvent event){
        String table = dbHandler.config.readTableFromEvent(event);
        String message = e != null && e.getMessage() != null && e.getMessage().contains("does not exist")
            //If table doesn't exist yet
            ? "{} Failed to perform {} - table {} does not exists {}"
            : "{} Failed to perform {} on table {} {}";
        logger.info(message, dbHandler.traceItem, action.name(), table, e);
    }

    private static void executeBatchesAndCheckOnFailures(DatabaseHandler dbh, List<String> idList, PreparedStatement batchStmt,
        List<FeatureCollection.ModificationFailure> fails,
        ModifyFeaturesEvent event, ModificationType type) throws SQLException {
        int[] batchStmtResult;

        try {
            if (idList.size() > 0) {
                logger.debug("{} batch execution [{}]: {} ", dbh.traceItem, type, batchStmt);

                batchStmt.setQueryTimeout(dbh.calculateTimeout());
                batchStmtResult = batchStmt.executeBatch();
                if (event.getVersionsToKeep() <= 1)
                    DatabaseWriter.fillFailList(batchStmtResult, fails, idList, event, type);
            }
        }
        catch (Exception e) {
            if (e instanceof SQLException && ((SQLException)e).getSQLState() != null
                && ((SQLException)e).getSQLState().equalsIgnoreCase("42P01"))
                //Re-throw, as a missing table will be handled by DatabaseHandler.
                throw e;

            //If there was some error inside the multimodal insert query, fail the transaction
            int[] res = new int[idList.size()];
            Arrays.fill(res, 0);
            DatabaseWriter.fillFailList(res, fails, idList, event, type);
        }
        finally {
            if (batchStmt != null)
                batchStmt.close();
        }
    }

    private static void fillFailList(int[] batchResult, List<FeatureCollection.ModificationFailure> fails,  List<String> idList,
        ModifyFeaturesEvent event, ModificationType action) {
        for (int i = 0; i < batchResult.length; i++)
            if (batchResult[i] == 0)
                fails.add(new FeatureCollection.ModificationFailure().withId(idList.get(i)).withMessage(getFailedRowErrorMsg(action, event)));
    }
}
