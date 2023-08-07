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

package com.here.xyz.psql;

import static com.here.xyz.psql.DatabaseHandler.PARTITION_SIZE;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.DELETE;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.INSERT;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.INSERT_HIDE_COMPOSITE;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.UPDATE;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.UPDATE_HIDE_COMPOSITE;
import static com.here.xyz.psql.DatabaseWriter.XyzSqlErrors.XYZ_CONFLICT;
import static com.here.xyz.psql.query.GetFeatures.MAX_BIGINT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.psql.query.XyzEventBasedQueryRunner;
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

    private static SQLQuery buildMultiModalInsertStmtQuery(DatabaseHandler dbHandler, ModifyFeaturesEvent event) {
        return new SQLQuery("SELECT xyz_write_versioned_modification_operation(#{id}, #{version}, #{operation}, #{jsondata}, #{geo}, "
            + "#{schema}, #{table}, #{concurrencyCheck}, #{partitionSize}, #{versionsToKeep}, #{pw}, #{baseVersion})")
            .withNamedParameter("schema", dbHandler.config.getDatabaseSettings().getSchema())
            .withNamedParameter("table", XyzEventBasedQueryRunner.readTableFromEvent(event))
            .withNamedParameter("concurrencyCheck", event.isConflictDetectionEnabled())
            .withNamedParameter("partitionSize", PARTITION_SIZE)
            .withNamedParameter("versionsToKeep", event.getVersionsToKeep())
            .withNamedParameter("pw", dbHandler.getConfig().getDatabaseSettings().getPassword());
    }

    private static SQLQuery buildInsertStmtQuery(DatabaseHandler dbHandler, ModifyFeaturesEvent event) {
      return setWriteQueryComponents(new SQLQuery("${{geoWith}} INSERT INTO ${schema}.${table} (id, version, operation, jsondata, geo) "
          + "VALUES("
          + "#{id}, "
          + "#{version}, "
          + "#{operation}, "
          + "#{jsondata}::jsonb, "
          + "${{geo}}"
          + ")"), dbHandler, event);
    }

    private static SQLQuery buildUpdateStmtQuery(DatabaseHandler dbHandler, ModifyFeaturesEvent event) {
        return setWriteQueryComponents(new SQLQuery("${{geoWith}} UPDATE ${schema}.${table} SET "
            + "version = #{version}, "
            + "operation = #{operation}, "
            + "jsondata = #{jsondata}::jsonb, "
            + "geo = (${{geo}}) "
            + "WHERE id = #{id} ${{conflictCheck}}"), dbHandler, event)
            .withQueryFragment("conflictCheck", buildConflictCheckFragment(event));
    }

    private static SQLQuery setWriteQueryComponents(SQLQuery writeQuery, DatabaseHandler dbHandler, ModifyFeaturesEvent event) {
        return setTableVariables(writeQuery
            .withQueryFragment("geoWith", "WITH in_params AS (SELECT #{geo} as geo)")
            .withQueryFragment("geo", "CASE WHEN (SELECT geo FROM in_params)::geometry IS NULL THEN NULL ELSE "
                + "ST_Force3D(ST_GeomFromWKB((SELECT geo FROM in_params)::BYTEA, 4326)) END"), dbHandler, event);
    }

    private static SQLQuery setTableVariables(SQLQuery writeQuery, DatabaseHandler dbHandler, ModifyFeaturesEvent event) {
      return writeQuery
          .withVariable("schema", dbHandler.config.getDatabaseSettings().getSchema())
          .withVariable("table", XyzEventBasedQueryRunner.readTableFromEvent(event));
    }

    private static SQLQuery buildDeleteStmtQuery(DatabaseHandler dbHandler, ModifyFeaturesEvent event) {
        SQLQuery query = new SQLQuery("DELETE FROM ${schema}.${table} WHERE id = #{id} ${{conflictCheck}}")
            .withQueryFragment("conflictCheck", buildConflictCheckFragment(event));

        return setTableVariables(
            query,
            dbHandler,
            event);
    }

    private static SQLQuery buildConflictCheckFragment(ModifyFeaturesEvent event) {
      return new SQLQuery(event.isConflictDetectionEnabled()
          ? " AND (CASE WHEN #{baseVersion}::BIGINT IS NULL THEN "
          + "next_version = #{MAX_BIGINT} ELSE version = #{baseVersion} END)"
          : "")
          .withNamedParameter("MAX_BIGINT", MAX_BIGINT);
    }

    public enum XyzSqlErrors {

        XYZ_CONFLICT("XYZ49", "xyz_conflict"),
        XYZ_UNEXPECTED_ERROR("XYZ50", "xyz_unexpected_error");

        public final String errorCode;
        public final String errorName;

        XyzSqlErrors(String errorCode, String errorName) {
            this.errorCode = errorCode;
            this.errorName = errorName;
        }

        @Override
        public String toString() {
            return errorName;
        }
    }

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
    public static final String UPDATE_ERROR_CONCURRENCY = UPDATE_ERROR_GENERAL+" - Object does not exist or concurrent modification";
    private static final String UPDATE_ERROR_ID_MISSING = UPDATE_ERROR_GENERAL+" - Feature Id is missing";

    private static final String DELETE_ERROR_GENERAL = "Delete has failed";
    public static final String DELETE_ERROR_NOT_EXISTS = DELETE_ERROR_GENERAL+" - Object does not exist";
    public static final String DELETE_ERROR_CONCURRENCY = DELETE_ERROR_GENERAL+" - Object does not exist or concurrent modification";

    public static final String INSERT_ERROR_GENERAL = "Insert has failed";

    protected static final String TRANSACTION_ERROR_GENERAL = "Transaction has failed";

    private static PGobject featureToPGobject(ModifyFeaturesEvent event, final Feature feature, long version) throws SQLException {
        final Geometry geometry = feature.getGeometry();
        feature.setGeometry(null); //Do not serialize the geometry in the JSON object

        final String json;
        try {
            //Remove the version from the JSON data, because it should not be written into the "jsondata" column.
            feature.getProperties().getXyzNamespace().setVersion(-1);

            if (event.getVersionsToKeep() <= 1)
              feature.getProperties().getXyzNamespace().setAuthor(null);

            json = feature.serialize();
        }
        finally {
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
        NOTE: If history is activated for the space, always only inserts are performed,
        but special parameters are necessary to handle conflicts in deletion case.
        Also, the feature to be written during the insert operation has to be "mocked" to only contain the necessary information.
         */
        if (event.getVersionsToKeep() > 1) {
            Feature deletedFeature = new Feature()
                .withId(deletion.getKey())
                .withProperties(new Properties().withXyzNamespace(new XyzNamespace()
                    .withDeleted(true)
                    .withVersion(Long.parseLong(deletion.getValue())) //TODO: This is a workaround for legacy history
                    .withUpdatedAt(System.currentTimeMillis())));
            fillInsertQueryFromFeature(query, DELETE, deletedFeature, event, version);
        }
        else {
            query.setNamedParameter("id", deletion.getKey());
            if (event.isConflictDetectionEnabled())
              //TODO: Check if we should not throw an exception in the case of a missing baseVersion
              query.setNamedParameter("baseVersion", deletion.getValue() != null ? Long.parseLong(deletion.getValue()) : null);
        }
    }

    private static void fillUpdateQueryFromFeature(SQLQuery query, Feature feature, ModifyFeaturesEvent event, long version) throws SQLException {
        if (feature.getId() == null)
            throw new WriteFeatureException(UPDATE_ERROR_ID_MISSING);

        if (event.isConflictDetectionEnabled() && event.getVersionsToKeep() == 1)
            query.setNamedParameter("baseVersion", feature.getProperties().getXyzNamespace().getVersion());

        /*
        NOTE: If versioning is activated for the space, always only inserts are performed,
        but special parameters are necessary to handle conflicts in update case.
         */
        fillInsertQueryFromFeature(query, UPDATE, feature, event, version);
    }

    private static ModificationType resolveOperation(ModificationType action, Feature feature) {
        return (action == DELETE || !getDeletedFlagFromFeature(feature) ? action
            : action == INSERT ? INSERT_HIDE_COMPOSITE : UPDATE_HIDE_COMPOSITE);
    }

    private static void fillInsertQueryFromFeature(SQLQuery query, ModificationType action, Feature feature, ModifyFeaturesEvent event, long version) throws SQLException {
        long baseVersion = feature.getProperties().getXyzNamespace().getVersion();
        query
            .withNamedParameter("id", feature.getId())
            .withNamedParameter("version", version)
            .withNamedParameter("operation", (action == DELETE || !getDeletedFlagFromFeature(feature) ? action
                : action == INSERT ? INSERT_HIDE_COMPOSITE : UPDATE_HIDE_COMPOSITE).shortValue)
            .withNamedParameter("jsondata", featureToPGobject(event, feature, version))
            .withNamedParameter("baseVersion", baseVersion);

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
      //noinspection SimplifiableConditionalExpression
      return f.getProperties() == null ? false :
            f.getProperties().getXyzNamespace() == null ? false :
            f.getProperties().getXyzNamespace().isDeleted();
    }

    protected static void modifyFeatures(DatabaseHandler dbh, ModifyFeaturesEvent event, ModificationType action,
        FeatureCollection collection, List<FeatureCollection.ModificationFailure> fails, List inputData, Connection connection,
        long version) throws SQLException, JsonProcessingException {
        boolean transactional = event.getTransaction();
        connection.setAutoCommit(!transactional);
        SQLQuery modificationQuery = buildModificationStmtQuery(dbh, event, action);

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

                if (action != INSERT && fails.size() > 0) { //Not necessary in INSERT case as no conflict check is performed there
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
                return event.isConflictDetectionEnabled() ? UPDATE_ERROR_CONCURRENCY : UPDATE_ERROR_NOT_EXISTS;
            case DELETE:
                return event.isConflictDetectionEnabled() ? DELETE_ERROR_CONCURRENCY : DELETE_ERROR_NOT_EXISTS;
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

    private static SQLQuery buildModificationStmtQuery(DatabaseHandler dbHandler, ModifyFeaturesEvent event, ModificationType action) {
        //If versioning is activated for the space, always only perform inserts
        if (event.getVersionsToKeep() > 1)
            return buildMultiModalInsertStmtQuery(dbHandler, event);
        switch (action) {
            case INSERT:
                return buildInsertStmtQuery(dbHandler, event);
            case UPDATE:
                return buildUpdateStmtQuery(dbHandler, event);
            case DELETE:
                return buildDeleteStmtQuery(dbHandler, event);
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
        String table = XyzEventBasedQueryRunner.readTableFromEvent(event);
        String message = e != null && e.getMessage() != null && e.getMessage().contains("does not exist")
            //If table doesn't exist yet
            ? "{} Failed to perform {} - table {} does not exists"
            : "{} Failed to perform {} on table {}";

        if (e instanceof SQLException && XYZ_CONFLICT.errorCode.equals(((SQLException) e).getSQLState()))
            logger.info(message, dbHandler.traceItem, action.name(), table, e);
        else
            logger.warn(message, dbHandler.traceItem, action.name(), table, e);
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
                && ((SQLException) e).getSQLState().equalsIgnoreCase("42P01"))
                //Re-throw, as a missing table will be handled by DatabaseHandler.
                throw e;

            if (e instanceof SQLException && XYZ_CONFLICT.errorCode.equals(((SQLException) e).getSQLState()))
                logger.info("{} Conflict during transactional write operation", dbh.traceItem, e);
            else
                logger.warn("{} Unexpected error during transactional write operation", dbh.traceItem, e);

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
