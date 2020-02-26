/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.StorageConnector;
import com.here.xyz.events.*;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.*;
import com.mchange.v2.c3p0.AbstractConnectionCustomizer;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PSQLException;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class DatabaseHandler extends StorageConnector {
    private static final Logger logger = LogManager.getLogger();

    private static final Pattern pattern = Pattern.compile("^BOX\\(([-\\d\\.]*)\\s([-\\d\\.]*),([-\\d\\.]*)\\s([-\\d\\.]*)\\)$");
    private static final int MAX_PRECISE_STATS_COUNT = 10_000;
    private static final String C3P0EXT_CONFIG_SCHEMA = "config.schema()";
    private static final String HISTORY_TABLE_SUFFIX = "_hst";
    protected static final int STATEMENT_TIMEOUT_SECONDS = 24;

    /**
     * The data source connections factory.
     */
    private static Map<String, XYZDBInstance> dbInstanceMap = new HashMap<>();

    /**
     * Current event.
     */
    private Event event;
    /**
     * The config for the current event.
     */
    protected PSQLConfig config;
    /**
     * The write data source for the current event.
     */
    protected DataSource dataSource;
    /**
     * The read data source for the current event.
     */
    private DataSource readDataSource;
    /**
     * The dbMaintainer for the current event.
     */
    private DatabaseMaintainer dbMaintainer;

    private Map<String, String> replacements = new HashMap<>();

    private boolean retryAttempted;

    @Override
    protected XyzResponse processHealthCheckEvent(HealthCheckEvent event) {
        long targetResponseTime = event.getMinResponseTime() + System.currentTimeMillis();
        SQLQuery query = new SQLQuery("SELECT 1");
        try {
            /** run DB-Maintenance */
            dbMaintainer.run(event, streamId);

            executeQuery(query, (rs) -> null, dataSource);
            // establish a connection to the replica, if such is set.
            if (dataSource != readDataSource) {
                executeQuery(query, (rs) -> null, readDataSource);
            }

            long now = System.currentTimeMillis();
            if (now < targetResponseTime) {
                Thread.sleep(targetResponseTime - now);
            }
            return new HealthStatus().withStatus("OK");
        } catch (Exception e) {
            return new ErrorResponse().withStreamId(streamId).withError(XyzError.EXCEPTION).withErrorMessage(e.getMessage());
        }
    }

    @Override
    protected synchronized void initialize(Event event) {

        this.event = event;
        final String ecps = PSQLConfig.getECPS(event);

        if (!dbInstanceMap.containsKey(ecps)) {
            /** Init dataSource, readDataSource ..*/
            logger.info("{} - Create new config and data source for ECPS string: '{}'", streamId, ecps);
            final PSQLConfig config = new PSQLConfig(event, context);

            final ComboPooledDataSource source = getComboPooledDataSource(config.host(), config.port(), config.database(), config.user(),
                    config.password(), config.applicationName(), config.maxPostgreSQLConnections());

            Map<String, String> m = new HashMap<>();
            m.put(C3P0EXT_CONFIG_SCHEMA, config.schema());
            source.setExtensions(m);

            final DatabaseMaintainer dbMaintainer = new DatabaseMaintainer(source,config);
            final XYZDBInstance xyzDBInstance = new XYZDBInstance(source ,dbMaintainer, config);

            if (config.replica() != null) {
                final ComboPooledDataSource replicaDataSource = getComboPooledDataSource(config.replica(), config.port(), config.database(),
                        config.user(), config.password(), config.applicationName(), config.maxPostgreSQLConnections());
                replicaDataSource.setExtensions(m);
                xyzDBInstance.addReadDataSource(replicaDataSource);
            }
            dbInstanceMap.put(ecps, xyzDBInstance);
        }

        this.dataSource = dbInstanceMap.get(ecps).getDataSource();
        this.readDataSource = dbInstanceMap.get(ecps).getReadDataSource();
        this.dbMaintainer = dbInstanceMap.get(ecps).getDatabaseMaintainer();
        this.config = dbInstanceMap.get(ecps).getConfig();

        if(event.getPreferPrimaryDataSource() == null || event.getPreferPrimaryDataSource() == Boolean.TRUE){
            this.readDataSource = this.dataSource;
        }

        retryAttempted = false;

        String table = config.table(event);
        String hstTable = config.table(event)+"_hst";

        replacements.put("idx_serial", "idx_" + table + "_serial");
        replacements.put("idx_id", "idx_" + table + "_id");
        replacements.put("idx_tags", "idx_" + table + "_tags");
        replacements.put("idx_geo", "idx_" + table + "_geo");
        replacements.put("idx_createdAt", "idx_" + table + "_createdAt");
        replacements.put("idx_updatedAt", "idx_" + table + "_updatedAt");

        replacements.put("idx_hst_id", "idx_" + hstTable + "_id");
        replacements.put("idx_hst_uuid", "idx_" + hstTable + "_uuid");
        replacements.put("idx_hst_updatedAt", "idx_" + hstTable + "_updatedAt");
    }

    private ComboPooledDataSource getComboPooledDataSource(String host, int port, String database, String user,
                                                           String password, String applicationName, int maxPostgreSQLConnections) {
        final ComboPooledDataSource cpds = new ComboPooledDataSource();

        cpds.setJdbcUrl(
                String.format("jdbc:postgresql://%1$s:%2$d/%3$s?ApplicationName=%4$s&tcpKeepAlive=true", host, port, database, applicationName));

        cpds.setUser(user);
        cpds.setPassword(password);

        cpds.setInitialPoolSize(1);
        cpds.setMinPoolSize(1);
        cpds.setAcquireIncrement(1);
        cpds.setMaxPoolSize(maxPostgreSQLConnections);

        cpds.setConnectionCustomizerClassName(DatabaseHandler.XyzConnectionCustomizer.class.getName());
        return cpds;
    }

    /**
     * Executes the given query and returns the processed by the handler result.
     */
    protected <T> T executeQuery(SQLQuery query, ResultSetHandler<T> handler) throws SQLException {
        return executeQuery(query, handler, readDataSource);
    }

    protected FeatureCollection executeQueryWithRetry(SQLQuery query) throws SQLException {
        return executeQueryWithRetry(query, this::defaultFeatureResultSetHandler);
    }

    /**
     *
     * Executes the query and reattempt to execute the query, after
     */
    protected <T extends XyzResponse> T executeQueryWithRetry(SQLQuery query, ResultSetHandler<T> handler) throws SQLException {
        try {
            return executeQuery(query, handler);
        } catch (Exception e) {
            try {
                if (canRetryAttempt()) {
                    return executeQuery(query, handler);
                }
            } catch (Exception e1) {
                throw e;
            }
            throw e;
        }
    }
    protected <T> T executeUpdateWithRetry(SQLQuery query, ResultSetHandler<T> handler) throws SQLException {
        return executeQuery(query, handler, dataSource);
    }

    protected int executeUpdateWithRetry(SQLQuery query) throws SQLException {
        try {
            return executeUpdate(query);
        } catch (Exception e) {
            try {
                if (canRetryAttempt()) {
                    return executeUpdate(query);
                }
            } catch (Exception e1) {
                throw e;
            }
            throw e;
        }
    }

    /**
     * Executes the given query and returns the processed by the handler result using the provided dataSource.
     */
    private <T> T executeQuery(SQLQuery query, ResultSetHandler<T> handler, DataSource dataSource) throws SQLException {
        final long start = System.currentTimeMillis();
        try {
            final QueryRunner run = new QueryRunner(dataSource);
            query.setText(SQLQuery.replaceVars(query.text(), config.schema(), config.table(event)));
            final String queryText = query.text();
            final List<Object> queryParameters = query.parameters();
            logger.info("{} - executeQuery: {} - Parameter: {}", streamId, queryText, queryParameters);
            return run.query(queryText, handler, queryParameters.toArray());
        } finally {
            final long end = System.currentTimeMillis();
            logger.info("{} - query time: {}ms", streamId, (end - start));
        }
    }

    /**
     * Executes the given update or delete query and returns the number of deleted or updated records.
     *
     * @param query the update or delete query.
     * @return the amount of updated or deleted records.
     * @throws SQLException if any error occurred.
     */
    int executeUpdate(SQLQuery query) throws SQLException {
        final long start = System.currentTimeMillis();
        try {
            final QueryRunner run = new QueryRunner(dataSource);
            query.setText(SQLQuery.replaceVars(query.text(),config.schema(), config.table(event)));
            final String queryText = query.text();
            final List<Object> queryParameters = query.parameters();
            logger.info("{} - executeUpdate: {} - Parameter: {}", streamId, queryText, queryParameters);
            return run.update(queryText, queryParameters.toArray());
        } finally {
            final long end = System.currentTimeMillis();
            logger.info("{} - query time: {}ms", (end - start));
        }
    }

    /**
     *
     * @param idsToFetch Ids of objects which should get fetched
     * @return List of Features which could get fetched
     * @throws Exception  if any error occurred.
     */
    protected List<Feature> fetchOldStates(String[] idsToFetch) throws Exception {
        List<Feature> oldFeatures = null;
        SQLQuery query = new SQLQuery("SELECT jsondata, ST_AsGeojson(ST_Force3D(ST_MakeValid(geo))) FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(?)",
                SQLQuery.createSQLArray(idsToFetch, "text",dataSource));
        FeatureCollection oldFeaturesCollection = executeQueryWithRetry(query);
        if (oldFeaturesCollection != null) {
            oldFeatures = oldFeaturesCollection.getFeatures();
        }

        return oldFeatures;
    }

    protected XyzResponse executeModifyFeatures(ModifyFeaturesEvent event) throws Exception {
        final boolean includeOldStates = event.getParams() != null && event.getParams().get(PSQLConfig.INCLUDE_OLD_STATES) == Boolean.TRUE;
        final boolean handleUUID = event.getEnableUUID() == Boolean.TRUE;
        final boolean transactional = event.getTransaction() == Boolean.TRUE;

        final String schema = config.schema();
        final String table = config.table(event);

        final FeatureCollection collection = new FeatureCollection();
        collection.setFeatures(new ArrayList<>());

        List<Feature> oldFeatures;

        List<Feature> inserts = Optional.ofNullable(event.getInsertFeatures()).orElse(new ArrayList<>());
        List<Feature> updates = Optional.ofNullable(event.getUpdateFeatures()).orElse(new ArrayList<>());
        Map<String, String> deletes = Optional.ofNullable(event.getDeleteFeatures()).orElse(new HashMap<>());
        List<FeatureCollection.ModificationFailure> fails = Optional.ofNullable(event.getFailed()).orElse(new ArrayList<>());

        List<String> insertIds = inserts.stream().map(Feature::getId).filter(Objects::nonNull).collect(Collectors.toList());
        List<String> updateIds = updates.stream().map(Feature::getId).filter(Objects::nonNull).collect(Collectors.toList());
        List<String> deleteIds = new ArrayList<>(deletes.keySet());

        /** Include Old states */
        if (includeOldStates) {
            String[] idsToFetch = Stream.of(insertIds, updateIds, deleteIds).flatMap(List::stream).toArray(String[]::new);
            oldFeatures = fetchOldStates(idsToFetch);
            if (oldFeatures != null) {
                collection.setOldFeatures(oldFeatures);
            }
        }

        try (final Connection connection = dataSource.getConnection()) {
            if(transactional)
                connection.setAutoCommit(false);
            else
                connection.setAutoCommit(true);

            try {
                if (deletes.size() > 0) {
                    DatabaseWriter.deleteFeatures(schema, table, streamId, fails, deletes, connection, transactional, handleUUID);
                }
                if (inserts.size() > 0) {
                    DatabaseWriter.insertFeatures(schema, table, streamId, collection, fails, inserts, connection, transactional);
                }
                if (updates.size() > 0) {
                    DatabaseWriter.updateFeatures(schema, table, streamId, collection, fails,  updates, connection, transactional, handleUUID);
                }

                if (transactional) {
                    /** Commit SQLS in one transaction */
                    connection.commit();
                }
            }catch (Exception e){
                /** Objects which are responsible for the failed operation */
                final List<String> failedIds = fails.stream().map(FeatureCollection.ModificationFailure::getId).filter(Objects::nonNull).collect(Collectors.toList());

                if(transactional) {
                    connection.rollback();

//                    if (e.getMessage() != null && e.getMessage().contains("relation ") && e.getMessage().contains("does not exist"))

                    if((e instanceof BatchUpdateException && ((BatchUpdateException)e).getSQLState().equalsIgnoreCase("42P01"))
                            || (e instanceof PSQLException && ((PSQLException)(e).getCause()).getSQLState().equalsIgnoreCase(("42P01"))))
                        ;//Table does not exist yet - create it!
                    else{
                        /** Add all other Objects to failed list */
                        final List<String> failedIdsTotal = new LinkedList<>();
                        failedIdsTotal.addAll(insertIds.stream().filter(x -> !failedIds.contains(x)).collect(Collectors.toList()));
                        failedIdsTotal.addAll(updateIds.stream().filter(x -> !failedIds.contains(x)).collect(Collectors.toList()));
                        failedIdsTotal.addAll(deleteIds.stream().filter(x -> !failedIds.contains(x)).collect(Collectors.toList()));

                        for (String id: failedIdsTotal) {
                            fails.add(new FeatureCollection.ModificationFailure().withId(id).withMessage(DatabaseWriter.TRANSACTION_ERROR_GENERAL));
                        }
                        failedIdsTotal.addAll(failedIds);

                        /** Reset the rest */
                        collection.setFeatures(new ArrayList<>());
                        collection.setFailed(fails);
                        return collection;
                    }
                }
                if (!retryAttempted) {
                    /** Retry */
                    connection.close();
                    canRetryAttempt();

                    event.setFailed(fails);
                    return executeModifyFeatures(event);
                }
            }

            /** filter out failed ids */
            final List<String> failedIds = fails.stream().map(FeatureCollection.ModificationFailure::getId).filter(Objects::nonNull).collect(Collectors.toList());
            insertIds = inserts.stream().map(Feature::getId).filter(x -> !failedIds.contains(x)).collect(Collectors.toList());
            updateIds = updates.stream().map(Feature::getId).filter(x -> !failedIds.contains(x)).collect(Collectors.toList());
            deleteIds = deletes.keySet().stream().filter(x -> !failedIds.contains(x)).collect(Collectors.toList());

            collection.setFailed(fails);

            if (insertIds.size() > 0) {
                if (collection.getInserted() == null) {
                    collection.setInserted(new ArrayList<>());
                }
                
                collection.getInserted().addAll(insertIds);
            }

            if (updateIds.size() > 0) {
                if (collection.getUpdated() == null) {
                    collection.setUpdated(new ArrayList<>());
                }
                collection.getUpdated().addAll(updateIds);
            }

            if (deleteIds.size() > 0) {
                if (collection.getDeleted() == null) {
                    collection.setDeleted(new ArrayList<>());
                }
                collection.getDeleted().addAll(deleteIds);
            }

            return collection;
        }
    }

    protected XyzResponse executeDeleteFeaturesByTag(DeleteFeaturesByTagEvent event) throws Exception {
        boolean includeOldStates = event.getParams() != null
                && event.getParams().get(PSQLConfig.INCLUDE_OLD_STATES) == Boolean.TRUE;

        final SQLQuery searchQuery = SQLQueryBuilder.generateSearchQuery(event, dataSource);
        final SQLQuery query = SQLQueryBuilder.buildDeleteFeaturesByTagQuery(includeOldStates, searchQuery);

        //TODO: check in detail what we want to return
        if (searchQuery != null && includeOldStates)
            return executeUpdateWithRetry(query, this::oldStatesResultSetHandler);

        return new FeatureCollection().withCount((long) executeUpdateWithRetry(query));
    }

    private boolean canRetryAttempt() throws Exception {
        if (retryAttempted) {
            return false;
        }
        if (hasTable()) {
            retryAttempted = true; // the table is there, do not retry
            return false;
        }

        ensureSpace();
        retryAttempted = true;
        logger.info("{} - The table was created. Retry the execution.", streamId);
        return true;
    }

    /**
     * A helper method that will test if the table for the space does exist.
     *
     * @return true if the table for the space exists; false otherwise.
     * @throws SQLException if the test fails due to any SQL error.
     */
    protected boolean hasTable() throws SQLException {
        if (event instanceof HealthCheckEvent) {
            return true;
        }

        PSQLConfig pConfig = (PSQLConfig) config;
        long start = System.currentTimeMillis();
        try (final Connection conn = dataSource.getConnection()) {
            try (final ResultSet rs = conn.getMetaData()
                    .getTables(null, pConfig.schema(), pConfig.table(event), new String[]{"TABLE", "VIEW"})) {
                if (rs.next()) {
                    long end = System.currentTimeMillis();
                    logger.info("{} - Time for table check: " + (end - start) + "ms", streamId);
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * A helper method that will ensure that the tables for the space of this event do exist and is up to date, if not it will alter the
     * table.
     *
     * @throws SQLException if the table does not exist and can't be created or alter failed.
     */
    protected void ensureSpace() throws SQLException {
        // Note: We can assume that when the table exists, the postgis extensions are installed.
        if (hasTable()) {
            return;
        }

        try (final Connection connection = dataSource.getConnection()) {
            try {
                final String tableName = config.table(event);

                if (connection.getAutoCommit()) {
                    connection.setAutoCommit(false);
                }

                try (Statement stmt = connection.createStatement()) {
//                    String query = "CREATE TABLE ${schema}.${table} (jsondata jsonb, geo geometry(GeometryZ,4326), i SERIAL)";
//                    query = SQLQuery.replaceVars(query, config.schema(), tableName);
//                    stmt.addBatch(query);
//
//                    query = "CREATE UNIQUE INDEX ${idx_id} ON ${schema}.${table} ((jsondata->>'id'))";
//                    query = SQLQuery.replaceVars(query, replacements, config.schema(), tableName);
//                    stmt.addBatch(query);
//
//                    query = "CREATE INDEX ${idx_tags} ON ${schema}.${table} USING gin ((jsondata->'properties'->'@ns:com:here:xyz'->'tags') jsonb_ops)";
//                    query = SQLQuery.replaceVars(query, replacements, config.schema(), tableName);
//                    stmt.addBatch(query);
//
//                    query = "CREATE INDEX ${idx_geo} ON ${schema}.${table} USING gist ((geo))";
//                    query = SQLQuery.replaceVars(query, replacements, config.schema(), tableName);
//                    stmt.addBatch(query);
//
//                    query = "CREATE INDEX ${idx_serial} ON ${schema}.${table}  USING btree ((i))";
//                    query = SQLQuery.replaceVars(query, replacements, config.schema(), tableName);
//                    stmt.addBatch(query);
//
//                    query = "CREATE INDEX ${idx_updatedAt} ON ${schema}.${table} USING btree ((jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt'))";
//                    query = SQLQuery.replaceVars(query, replacements, config.schema(), tableName);
//                    stmt.addBatch(query);
//
//                    query = "CREATE INDEX ${idx_createdAt} ON ${schema}.${table} USING btree ((jsondata->'properties'->'@ns:com:here:xyz'->'createdAt'))";
//                    query = SQLQuery.replaceVars(query, replacements, config.schema(), tableName);
//                    stmt.addBatch(query);
                    createSpaceStatement(stmt,tableName);

                    stmt.executeBatch();
                    connection.commit();
                    logger.info("{} - Successfully created table for space '{}'", streamId, event.getSpace());
                }
            } catch (Exception e) {
                final String tableName = config.table(event);
                logger.error("{} - Failed to create table '{}': {}", streamId, tableName, e);
                connection.rollback();
                // check if the table was created in the meantime, by another instance.
                if (hasTable()) {
                    return;
                }
                throw new SQLException("Missing table " + SQLQuery.sqlQuote(tableName) + " and creation failed: " + e.getMessage(), e);
            }
        }
    }

    private void createSpaceStatement(Statement stmt, String tableName) throws SQLException {
        String query = "CREATE TABLE IF NOT EXISTS ${schema}.${table} (jsondata jsonb, geo geometry(GeometryZ,4326), i SERIAL, geojson jsonb)";
//        String query = "CREATE TABLE IF NOT EXISTS ${schema}.${table} (jsondata jsonb, geo geometry(GeometryZ,4326), i SERIAL)";
        query = SQLQuery.replaceVars(query, config.schema(), tableName);
        stmt.addBatch(query);

        query = "CREATE UNIQUE INDEX IF NOT EXISTS ${idx_id} ON ${schema}.${table} ((jsondata->>'id'))";
        query = SQLQuery.replaceVars(query, replacements, config.schema(), tableName);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_tags} ON ${schema}.${table} USING gin ((jsondata->'properties'->'@ns:com:here:xyz'->'tags') jsonb_ops)";
        query = SQLQuery.replaceVars(query, replacements, config.schema(), tableName);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_geo} ON ${schema}.${table} USING gist ((geo))";
        query = SQLQuery.replaceVars(query, replacements, config.schema(), tableName);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_serial} ON ${schema}.${table}  USING btree ((i))";
        query = SQLQuery.replaceVars(query, replacements, config.schema(), tableName);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_updatedAt} ON ${schema}.${table} USING btree ((jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt'))";
        query = SQLQuery.replaceVars(query, replacements, config.schema(), tableName);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_createdAt} ON ${schema}.${table} USING btree ((jsondata->'properties'->'@ns:com:here:xyz'->'createdAt'))";
        query = SQLQuery.replaceVars(query, replacements, config.schema(), tableName);
        stmt.addBatch(query);
    }

    protected void ensureHistorySpace(Integer maxVersionCount) throws SQLException {
        final String tableName = config.table(event)+HISTORY_TABLE_SUFFIX;

        try (final Connection connection = dataSource.getConnection()) {
            try {

//                try (final ResultSet rs = connection.getMetaData()
//                        .getTables(null, config.schema(), tableName, new String[]{"TABLE", "VIEW"})) {
//                    if (rs.next()) {
//                        // Table present
//                        return;
//                    }
//                }

                if (connection.getAutoCommit()) {
                    connection.setAutoCommit(false);
                }

                try (Statement stmt = connection.createStatement()) {
                    /** Create Space-Table */
                    createSpaceStatement(stmt, config.table(event));

                    String query = "CREATE TABLE IF NOT EXISTS ${schema}.${table} (uuid text NOT NULL, jsondata jsonb, geo geometry(GeometryZ,4326), CONSTRAINT \""+tableName+"_pkey\" PRIMARY KEY (uuid))";
                    query = SQLQuery.replaceVars(query, config.schema(), tableName);
                    stmt.addBatch(query);

                    query = "CREATE INDEX IF NOT EXISTS ${idx_hst_uuid} ON ${schema}.${table} USING btree (uuid)";
                    query = SQLQuery.replaceVars(query, replacements, config.schema(), tableName);
                    stmt.addBatch(query);

                    query = "CREATE INDEX IF NOT EXISTS ${idx_hst_id} ON ${schema}.${table} ((jsondata->>'id'))";
                    query = SQLQuery.replaceVars(query, replacements, config.schema(), tableName);
                    stmt.addBatch(query);

                    query = "CREATE INDEX IF NOT EXISTS ${idx_hst_updatedAt} ON ${schema}.${table} USING btree ((jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt'))";
                    query = SQLQuery.replaceVars(query, replacements, config.schema(), tableName);
                    stmt.addBatch(query);

                    query = SQLQueryBuilder.deleteHistoryTriggerSQL(config.schema(),config.table(event));
                    stmt.addBatch(query);

                    query = SQLQueryBuilder.addHistoryTriggerSQL(config.schema(),config.table(event), maxVersionCount);
                    stmt.addBatch(query);

                    stmt.executeBatch();
                    connection.commit();
                    logger.info("{} - Successfully created history table for space '{}'", streamId, event.getSpace());
                }
            } catch (Exception e) {
                throw new SQLException("History table " + SQLQuery.sqlQuote(tableName) + "  creation failed: " + e.getMessage(), e);
            }
        }
    }

/** #################################### Resultset Handlers #################################### */
    /**
     * The default handler for the most results.
     *
     * @param rs the result set.
     * @return the generated feature collection from the result set.
     * @throws SQLException when any unexpected error happened.
     */
    protected FeatureCollection defaultFeatureResultSetHandler(ResultSet rs) throws SQLException {
        final boolean isIterate = (event instanceof IterateFeaturesEvent);
        long nextHandle = 0;
        StringBuilder sb = new StringBuilder();
        String prefix = "[";
        sb.append(prefix);
        int numFeatures = 0;
        while (rs.next()) {
            sb.append(rs.getString(1));
            String geom = rs.getString(2);
            sb.setLength(sb.length() - 1);
            sb.append(",\"geometry\":");
            sb.append(geom == null ? "null" : geom);
            sb.append("}");
            sb.append(",");

            if (isIterate) {
                numFeatures++;
                nextHandle = rs.getLong(3);
            }
        }
        if (sb.length() > prefix.length()) {
            sb.setLength(sb.length() - 1);
        }
        sb.append("]");

        final FeatureCollection featureCollection = new FeatureCollection();
        featureCollection._setFeatures(sb.toString());
        if (isIterate) {
            if (numFeatures > 0 && numFeatures == ((IterateFeaturesEvent) event).getLimit()) {
                featureCollection.setHandle("" + nextHandle);
            }
        }

        return featureCollection;
    }

    /**
     * handler for delete by tags results.
     *
     * @param rs the result set.
     * @return the generated feature collection from the result set.
     * @throws SQLException when any unexpected error happened.
     */
    protected FeatureCollection oldStatesResultSetHandler(ResultSet rs) throws SQLException {
        StringBuilder sb = new StringBuilder();
        String prefix = "[";
        sb.append(prefix);
        while (rs.next()) {
            sb.append("{\"type\":\"Feature\",\"id\":");
            sb.append(rs.getString("id"));
            String geom = rs.getString("geometry");
            if (geom != null) {
                sb.append(",\"geometry\":");
                sb.append(geom);
                sb.append("}");
            }
            sb.append(",");
        }
        if (sb.length() > prefix.length()) {
            sb.setLength(sb.length() - 1);
        }
        sb.append("]");

        final FeatureCollection featureCollection = new FeatureCollection();
        featureCollection._setFeatures(sb.toString());
        return featureCollection;
    }

    /**
     * The result handler for a CountFeatures event.
     *
     * @param rs the result set.
     * @return the feature collection generated from the result.
     * @throws SQLException if any error occurred.
     */
    protected XyzResponse countResultSetHandler(ResultSet rs) throws SQLException {
        rs.next();
        long count = rs.getLong(1);
        return new CountResponse().withCount(count).withEstimated(count > MAX_PRECISE_STATS_COUNT);
    }

    /**
     * The result handler for a CountFeatures event.
     *
     * @param rs the result set.
     * @return the feature collection generated from the result.
     *
     */
    protected XyzResponse getStatisticsResultSetHandler(ResultSet rs){
        try {
            rs.next();

            StatisticsResponse.Value<Long> tablesize = XyzSerializable.deserialize(rs.getString("tablesize"), new TypeReference<StatisticsResponse.Value<Long>>() {
            });
            StatisticsResponse.Value<List<String>> geometryTypes = XyzSerializable
                    .deserialize(rs.getString("geometryTypes"), new TypeReference<StatisticsResponse.Value<List<String>>>() {
                    });
            StatisticsResponse.Value<List<StatisticsResponse.PropertyStatistics>> tags = XyzSerializable
                    .deserialize(rs.getString("tags"), new TypeReference<StatisticsResponse.Value<List<StatisticsResponse.PropertyStatistics>>>() {
                    });
            StatisticsResponse.PropertiesStatistics properties = XyzSerializable.deserialize(rs.getString("properties"), StatisticsResponse.PropertiesStatistics.class);
            StatisticsResponse.Value<Long> count = XyzSerializable.deserialize(rs.getString("count"), new TypeReference<StatisticsResponse.Value<Long>>() {
            });
            Map<String, Object> bboxMap = XyzSerializable.deserialize(rs.getString("bbox"), new TypeReference<Map<String, Object>>() {
            });

            final String searchable = rs.getString("searchable");
            properties.setSearchable(StatisticsResponse.PropertiesStatistics.Searchable.valueOf(searchable));

            String bboxs = (String) bboxMap.get("value");
            if (bboxs == null) {
                bboxs = "";
            }

            BBox bbox = new BBox();
            Matcher matcher = pattern.matcher(bboxs);
            if (matcher.matches()) {
                bbox = new BBox(
                        Math.max(-180, Math.min(180, Double.parseDouble(matcher.group(1)))),
                        Math.max(-90, Math.min(90, Double.parseDouble(matcher.group(2)))),
                        Math.max(-180, Math.min(180, Double.parseDouble(matcher.group(3)))),
                        Math.max(-90, Math.min(90, Double.parseDouble(matcher.group(4))))
                );
            }

            return new StatisticsResponse()
                    .withBBox(new StatisticsResponse.Value<BBox>().withValue(bbox).withEstimated(bboxMap.get("estimated") == Boolean.TRUE))
                    .withByteSize(tablesize)
                    .withCount(count)
                    .withGeometryTypes(geometryTypes)
                    .withTags(tags)
                    .withProperties(properties);
        } catch (Exception e) {
            return new ErrorResponse().withStreamId(streamId).withError(XyzError.EXCEPTION).withErrorMessage(e.getMessage());
        }
    }

    public class XYZDBInstance {
        private DataSource dataSource;
        private DataSource readDataSource;
        private DatabaseMaintainer databaseMaintainer;
        private PSQLConfig config;

        public XYZDBInstance(DataSource dataSource, DataSource readDataSource,
                             DatabaseMaintainer databaseMaintainer, PSQLConfig config){
            this.dataSource = dataSource;
            this.readDataSource = readDataSource;
            this.databaseMaintainer = databaseMaintainer;
            this.config = config;
        }

        public XYZDBInstance(DataSource dataSource, DatabaseMaintainer databaseMaintainer, PSQLConfig config){
            this.dataSource = dataSource;
            this.databaseMaintainer = databaseMaintainer;
            this.config = config;
        }

        public DataSource getReadDataSource() {
            if(readDataSource == null)
                return this.dataSource;
            return this.readDataSource;
        }

        public DatabaseMaintainer getDatabaseMaintainer() {
            return databaseMaintainer;
        }

        public DataSource getDataSource() {
            return dataSource;
        }

        public PSQLConfig getConfig() { return config; }

        public void addReadDataSource(DataSource readDataSource){
            this.readDataSource = readDataSource;
        }
    }

    public static class XyzConnectionCustomizer extends AbstractConnectionCustomizer { // handle initialization per db connection
        private String getSchema(String parentDataSourceIdentityToken) {
            return (String) extensionsForToken(parentDataSourceIdentityToken).get(C3P0EXT_CONFIG_SCHEMA);
        }

        public void onAcquire(Connection c, String pdsIdt) {
            String schema = getSchema(pdsIdt);  // config.schema();
            QueryRunner runner = new QueryRunner();
            try {
                runner.execute(c, "SET enable_seqscan = off;");
                runner.execute(c, "SET statement_timeout = " + (STATEMENT_TIMEOUT_SECONDS * 1000) + " ;");
                runner.execute(c, "SET search_path=" + schema + ",h3,public,topology;");
            } catch (SQLException e) {
                logger.error("Failed to initialize connection " + c + " [" + pdsIdt + "] : {}", e);
            }
        }
    }
}
