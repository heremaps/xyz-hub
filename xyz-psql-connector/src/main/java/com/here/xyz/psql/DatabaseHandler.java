/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.SimulatedContext;
import com.here.xyz.connectors.StorageConnector;
import com.here.xyz.events.DeleteFeaturesByTagEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetStorageStatisticsEvent;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.IterateHistoryEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.SearchForFeaturesOrderByEvent;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.psql.config.ConnectorParameters;
import com.here.xyz.psql.config.DatabaseSettings;
import com.here.xyz.psql.config.PSQLConfig;
import com.here.xyz.psql.query.LoadFeaturesQueryRunner;
import com.here.xyz.psql.query.StorageStatisticsQueryRunner;
import com.here.xyz.responses.BinaryResponse;
import com.here.xyz.responses.CountResponse;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.HealthStatus;
import com.here.xyz.responses.HistoryStatisticsResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.responses.changesets.Changeset;
import com.here.xyz.responses.changesets.ChangesetCollection;
import com.here.xyz.responses.changesets.CompactChangeset;
import com.here.xyz.util.DhString;
import com.mchange.v2.c3p0.AbstractConnectionCustomizer;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.PooledDataSource;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.StatementConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class DatabaseHandler extends StorageConnector {
    private static final Logger logger = LogManager.getLogger();

    private static final Pattern pattern = Pattern.compile("^BOX\\(([-\\d\\.]*)\\s([-\\d\\.]*),([-\\d\\.]*)\\s([-\\d\\.]*)\\)$");
    private static final int MAX_PRECISE_STATS_COUNT = 10_000;
    private static final String C3P0EXT_CONFIG_SCHEMA = "config.schema()";
    public static final String HISTORY_TABLE_SUFFIX = "_hst";

    public static final String APPLICATION_VND_MAPBOX_VECTOR_TILE = "application/vnd.mapbox-vector-tile";

    /**
     * Lambda Execution Time = 25s. We are actively canceling queries after STATEMENT_TIMEOUT_SECONDS
     * So if we receive a timeout prior 25s-STATEMENT_TIMEOUT_SECONDS the cancellation comes from
     * outside.
     **/
    private static final int MIN_REMAINING_TIME_FOR_RETRY_SECONDS = 3;
    protected static final int STATEMENT_TIMEOUT_SECONDS = 23;

    private static String INCLUDE_OLD_STATES = "includeOldStates"; // read from event params

    /**
     * The data source connections factory.
     */
    private static Map<String, PSQLConfig> dbInstanceMap = new HashMap<>();

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
    protected DatabaseMaintainer dbMaintainer;

    private Map<String, String> replacements = new HashMap<>();

    private boolean retryAttempted;

    protected XyzResponse processHealthCheckEventImpl(HealthCheckEvent event) throws SQLException {
        String connectorId = traceItem.getConnectorId();

        if(connectorId == null) {
            logger.warn("{} ConnectorId is missing as param in the Connector-Config! {} / {}@{}", traceItem, config.getDatabaseSettings().getDb(), config.getDatabaseSettings().getUser(), config.getDatabaseSettings().getHost());
            return new ErrorResponse().withError(XyzError.ILLEGAL_ARGUMENT).withErrorMessage("ConnectorId is missing as param in the Connector-Config! ");
        }

        if (event.getWarmupCount() == 0 && context instanceof SimulatedContext) {
            /** run DB-Maintenance - warmUp request is used */
            if (event.getMinResponseTime() != 0) {
                logger.info("{} dbMaintainer start", traceItem);
                dbMaintainer.run(traceItem);
                logger.info("{} dbMaintainer finished", traceItem);
                return new HealthStatus().withStatus("OK");
            }
        }

        SQLQuery query = new SQLQuery("SELECT 1");
        executeQuery(query, (rs) -> null, dataSource);

        // establish a connection to the replica, if such is set.
        if (dataSource != readDataSource) {
            executeQuery(query, (rs) -> null, readDataSource);
        }

        return ((HealthStatus) super.processHealthCheckEvent(event)).withStatus("OK");
    }

    void reset() {
        // clear the map and free resources for GC
        for ( String connectorId : dbInstanceMap.keySet()) {
            removeDbInstanceFromMap(connectorId);
        }
        dbInstanceMap.clear();
    }

    @Override
    protected synchronized void initialize(Event event) {
        this.event = event;
        this.config= new PSQLConfig(event, context, traceItem);
        String connectorId = traceItem.getConnectorId();

        if(connectorId == null) {
            logger.warn("{} ConnectorId is missing as param in the Connector-Config! {} / {}@{}", traceItem, config.getDatabaseSettings().getDb(), config.getDatabaseSettings().getUser(), config.getDatabaseSettings().getHost());
            connectorId =config.getConfigValuesAsString();
        }

        if(dbInstanceMap.get(connectorId) != null){
            /** Check if db-params has changed*/
            if(!dbInstanceMap.get(connectorId).getConfigValuesAsString().equalsIgnoreCase(config.getConfigValuesAsString())) {
                logger.info("{} Config has changed -> remove dbInstance from Pool. DbInstanceMap size:{}", traceItem, dbInstanceMap.size());
                removeDbInstanceFromMap(connectorId);
            }else{
                logger.debug("{} Config already loaded -> load dbInstance from Pool. DbInstanceMap size:{}", traceItem, dbInstanceMap.size());
            }
        }

        if (dbInstanceMap.get(connectorId) == null) {

            /** Init dataSource, readDataSource ..*/
            logger.info("{} Config is missing -> add new dbInstance to Pool. DbInstanceMap size:{}", traceItem, dbInstanceMap.size());
            final ComboPooledDataSource source = getComboPooledDataSource(config.getDatabaseSettings(), config.getConnectorParams(), config.applicationName() , false);

            Map<String, String> m = new HashMap<>();
            m.put(C3P0EXT_CONFIG_SCHEMA, config.getDatabaseSettings().getSchema());
            source.setExtensions(m);

            final DatabaseMaintainer dbMaintainer = new DatabaseMaintainer(source, config);
            config.addDataSource(source);
            config.addDatabaseMaintainer(dbMaintainer);

            if (config.getDatabaseSettings().getReplicaHost() != null) {
                final ComboPooledDataSource replicaDataSource = getComboPooledDataSource(config.getDatabaseSettings(), config.getConnectorParams(),  config.applicationName() , true);
                replicaDataSource.setExtensions(m);
                config.addReadDataSource(replicaDataSource);
            }
            dbInstanceMap.put(connectorId, config);
        }

        this.retryAttempted = false;
        this.dataSource = dbInstanceMap.get(connectorId).getDataSource();
        this.readDataSource = dbInstanceMap.get(connectorId).getReadDataSource();
        this.dbMaintainer = dbInstanceMap.get(connectorId).getDatabaseMaintainer();

        if (event.getPreferPrimaryDataSource() != null && event.getPreferPrimaryDataSource() == Boolean.TRUE) {
            this.readDataSource = this.dataSource;
        }

        String table = config.readTableFromEvent(event);
        String hstTable = config.readTableFromEvent(event)+HISTORY_TABLE_SUFFIX;

        replacements.put("idx_deleted", "idx_" + table + "_deleted");
        replacements.put("idx_serial", "idx_" + table + "_serial");
        replacements.put("idx_id", "idx_" + table + "_id");
        replacements.put("idx_tags", "idx_" + table + "_tags");
        replacements.put("idx_geo", "idx_" + table + "_geo");
        replacements.put("idx_createdAt", "idx_" + table + "_createdAt");
        replacements.put("idx_updatedAt", "idx_" + table + "_updatedAt");
        replacements.put("idx_viz", "idx_" + table + "_viz");

        replacements.put("idx_hst_id", "idx_" + hstTable + "_id");
        replacements.put("idx_hst_uuid", "idx_" + hstTable + "_uuid");
        replacements.put("idx_hst_updatedAt", "idx_" + hstTable + "_updatedAt");
        replacements.put("idx_hst_version", "idx_" + hstTable + "_version");
        replacements.put("idx_hst_deleted", "idx_" + hstTable + "_deleted");
        replacements.put("idx_hst_lastVersion", "idx_" + hstTable + "_lastVersion");
        replacements.put("idx_hst_idvsort", "idx_" + hstTable + "_idvsort");
        replacements.put("idx_hst_vidsort", "idx_" + hstTable + "_vidsort");
    }

    private void removeDbInstanceFromMap(String connectorId){
        synchronized (dbInstanceMap) {
            try {
                ((PooledDataSource) (dbInstanceMap.get(connectorId).getDataSource())).close();
            } catch (SQLException e) {
                logger.warn("Error while closing connections: ", e);
            }
            dbInstanceMap.remove(connectorId);
        }
    }

    private ComboPooledDataSource getComboPooledDataSource(DatabaseSettings dbSettings, ConnectorParameters connectorParameters, String applicationName, boolean useReplica) {
        final ComboPooledDataSource cpds = new ComboPooledDataSource();

        cpds.setJdbcUrl(
                DhString.format("jdbc:postgresql://%1$s:%2$d/%3$s?ApplicationName=%4$s&tcpKeepAlive=true",
                        useReplica ? dbSettings.getReplicaHost() : dbSettings.getHost(), dbSettings.getPort(), dbSettings.getDb(), applicationName));

        cpds.setUser(dbSettings.getUser());
        cpds.setPassword(dbSettings.getPassword());

        cpds.setInitialPoolSize(connectorParameters.getDbInitialPoolSize());
        cpds.setMinPoolSize(connectorParameters.getDbMinPoolSize());
        cpds.setMaxPoolSize(connectorParameters.getDbMaxPoolSize());

        cpds.setAcquireRetryAttempts(connectorParameters.getDbAcquireRetryAttempts());
        cpds.setAcquireIncrement(connectorParameters.getDbAcquireIncrement());

        cpds.setCheckoutTimeout( connectorParameters.getDbCheckoutTimeout() * 1000 );

        if(connectorParameters.getDbMaxIdleTime() != null)
            cpds.setMaxIdleTime(connectorParameters.getDbMaxIdleTime());

        if(connectorParameters.isDbTestConnectionOnCheckout())
            cpds.setTestConnectionOnCheckout(true);

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
        return executeQueryWithRetry(query, this::defaultFeatureResultSetHandler, true);
    }

    protected FeatureCollection executeQueryWithRetrySkipIfGeomIsNull(SQLQuery query) throws SQLException {
        return executeQueryWithRetry(query, this::defaultFeatureResultSetHandlerSkipIfGeomIsNull, true);
    }

    protected BinaryResponse executeBinQueryWithRetry(SQLQuery query) throws SQLException {
        return executeQueryWithRetry(query, this::defaultBinaryResultSetHandler, true);
    }

    /**
     *
     * Executes the query and reattempt to execute the query, after
     */
    protected <T> T executeQueryWithRetry(SQLQuery query, ResultSetHandler<T> handler, boolean useReadReplica) throws SQLException {
        try {
            return executeQuery(query, handler, useReadReplica ? readDataSource : dataSource);
        } catch (Exception e) {
            try {
                if (retryCausedOnServerlessDB(e) || canRetryAttempt()) {
                    logger.info("{} Retry Query permitted.", traceItem);
                    return executeQuery(query, handler, useReadReplica ? readDataSource : dataSource);
                }
            } catch (Exception e1) {
                if(retryCausedOnServerlessDB(e1)) {
                    logger.info("{} Retry Query permitted.", traceItem);
                    return executeQuery(query, handler, useReadReplica ? readDataSource : dataSource);
                }
                throw e;
            }
            throw e;
        }
    }

    protected int executeUpdateWithRetry(SQLQuery query) throws SQLException {
        try {
            return executeUpdate(query);
        } catch (Exception e) {
            try {
                if (retryCausedOnServerlessDB(e) || canRetryAttempt()) {
                    logger.info("{} Retry Update permitted.", traceItem);
                    return executeUpdate(query);
                }
            } catch (Exception e1) {
                if (retryCausedOnServerlessDB(e)) {
                    logger.info("{} Retry Update permitted.", traceItem);
                    return executeUpdate(query);
                }
                throw e;
            }
            throw e;
        }
    }

    protected boolean retryCausedOnServerlessDB(Exception e) {
        /** If a timeout comes directly after the invocation it could rely
         * on serverless aurora scaling. Then we should retry again.
         * 57014 - query_canceled
         * 57P01 - admin_shutdown
         * */
        if(e instanceof SQLException
                && ((SQLException)e).getSQLState() != null
                && (
                    ((SQLException)e).getSQLState().equalsIgnoreCase("57014") ||
                    ((SQLException)e).getSQLState().equalsIgnoreCase("57P01") ||
                    ((SQLException)e).getSQLState().equalsIgnoreCase("08003") ||
                    ((SQLException)e).getSQLState().equalsIgnoreCase("08006")
                   )
        ) {
            int remainingSeconds = context.getRemainingTimeInMillis() / 1000;

            if(!isRemainingTimeSufficient(remainingSeconds)){
                return false;
            }
            if (!retryAttempted) {
                logger.warn("{} Retry based on serverless scaling detected! RemainingTime: {} {}", traceItem, remainingSeconds, e);
                return true;
            }
        }
        return false;
    }

    /**
     * Executes the given query and returns the processed by the handler result using the provided dataSource.
     */

    private <T> T executeQuery(SQLQuery query, ResultSetHandler<T> handler, DataSource dataSource) throws SQLException {
        final long start = System.currentTimeMillis();
        try {
            final QueryRunner run = new QueryRunner(dataSource, new StatementConfiguration(null,null,null,null,calculateTimeout()));

            query.setText(SQLQuery.replaceVars(query.text(), config.getDatabaseSettings().getSchema(), config.readTableFromEvent(event)));
            final String queryText = query.text();
            final List<Object> queryParameters = query.parameters();
            logger.debug("{} executeQuery: {} - Parameter: {}", traceItem, queryText, queryParameters);
            return run.query(queryText, handler, queryParameters.toArray());
        } finally {
            final long end = System.currentTimeMillis();
            logger.info("{} query time: {}ms", traceItem, (end - start));
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
            final QueryRunner run = new QueryRunner(dataSource, new StatementConfiguration(null,null,null,null,calculateTimeout()));

            query.setText(SQLQuery.replaceVars(query.text(),config.getDatabaseSettings().getSchema(), config.readTableFromEvent(event)));
            final String queryText = query.text();
            final List<Object> queryParameters = query.parameters();
            logger.debug("{} executeUpdate: {} - Parameter: {}", traceItem, queryText, queryParameters);
            return run.update(queryText, queryParameters.toArray());
        } finally {
            final long end = System.currentTimeMillis();
            logger.info("{} query time: {}ms", traceItem, (end - start));
        }
    }

    protected XyzResponse executeLoadFeatures(LoadFeaturesEvent event) throws SQLException {
        if (event.getIdsMap() == null || event.getIdsMap().size() == 0)
            return new FeatureCollection();

        return new LoadFeaturesQueryRunner(event, this).run();
    }

    protected XyzResponse executeIterateHistory(IterateHistoryEvent event) throws SQLException {
        if(event.isCompact())
            return executeQueryWithRetry(SQLQueryBuilder.buildSquashHistoryQuery(event), this::compactHistoryResultSetHandler, false);
        return executeQueryWithRetry(SQLQueryBuilder.buildHistoryQuery(event), this::historyResultSetHandler, false);
    }

    protected XyzResponse executeIterateVersions(IterateFeaturesEvent event) throws SQLException {
        SQLQuery query = SQLQueryBuilder.buildLatestHistoryQuery(event);
        return executeQueryWithRetry(query, this::iterateVersionsHandler, false);
    }

    protected XyzResponse executeGetStorageStatistics(GetStorageStatisticsEvent event) throws SQLException {
        return new StorageStatisticsQueryRunner(event, this).run();
    }

    /**
     *
     * @param idsToFetch Ids of objects which should get fetched
     * @return List of Features which could get fetched
     * @throws Exception if any error occurred.
     */
    protected List<Feature> fetchOldStates(String[] idsToFetch) throws Exception {
        List<Feature> oldFeatures = null;
        FeatureCollection oldFeaturesCollection = executeQueryWithRetry(SQLQueryBuilder.generateLoadOldFeaturesQuery(idsToFetch, dataSource));

        if (oldFeaturesCollection != null) {
            oldFeatures = oldFeaturesCollection.getFeatures();
        }

        return oldFeatures;
    }

    /**
     *
     * @param idsToFetch Ids of objects which should get fetched
     * @return List of Feature's id which could get fetched
     * @throws Exception if any error occurred.
     */
    protected List<String> fetchExistingIds(String[] idsToFetch) throws Exception {
      return executeQueryWithRetry(SQLQueryBuilder.generateLoadExistingIdsQuery(idsToFetch, dataSource), this::idsListResultSetHandler, true);
    }

    protected XyzResponse executeModifyFeatures(ModifyFeaturesEvent event) throws Exception {
        final boolean includeOldStates = event.getParams() != null && event.getParams().get(INCLUDE_OLD_STATES) == Boolean.TRUE;
        final boolean handleUUID = event.getEnableUUID() == Boolean.TRUE;
        final boolean transactional = event.getTransaction() == Boolean.TRUE;

        final String schema = config.getDatabaseSettings().getSchema();
        final String table = config.readTableFromEvent(event);

        Integer version = null;
        final FeatureCollection collection = new FeatureCollection();
        collection.setFeatures(new ArrayList<>());

        List<Feature> inserts = Optional.ofNullable(event.getInsertFeatures()).orElse(new ArrayList<>());
        List<Feature> updates = Optional.ofNullable(event.getUpdateFeatures()).orElse(new ArrayList<>());
        List<Feature> upserts = Optional.ofNullable(event.getUpsertFeatures()).orElse(new ArrayList<>());
        Map<String, String> deletes = Optional.ofNullable(event.getDeleteFeatures()).orElse(new HashMap<>());
        List<FeatureCollection.ModificationFailure> fails = Optional.ofNullable(event.getFailed()).orElse(new ArrayList<>());
        boolean forExtendedSpace = isForExtendedSpace(event);

        //Handle deletes / updates on extended spaces
        if (forExtendedSpace && event.getContext() == DEFAULT) {
            if (deletes.size() > 0) {
                //Transform the incoming deletes into upserts with deleted flag
                for (String featureId : deletes.keySet()) {
                    upserts.add(new Feature()
                        .withId(featureId)
                        .withProperties(new Properties().withXyzNamespace(new XyzNamespace().withDeleted(true))));

                }
                deletes = Collections.emptyMap();
            }
            if (updates.size() > 0) {
                //Transform the incoming updates into upserts, because we don't know whether the object is existing in the extension already
                upserts.addAll(updates);
                updates.clear();
            }
        }

        try {
          /** Include Old states */
          if (includeOldStates) {
            String[] idsToFetch = getAllIds(inserts, updates, upserts, deletes).stream().filter(Objects::nonNull).toArray(String[]::new);
            List<Feature> oldFeatures = fetchOldStates(idsToFetch);
            if (oldFeatures != null) {
              collection.setOldFeatures(oldFeatures);
            }
          }

          /** Include Upserts */
          if (!upserts.isEmpty()) {
            String[] upsertIds = upserts.stream().map(Feature::getId).filter(Objects::nonNull).toArray(String[]::new);
            List<String> existingIds = fetchExistingIds(upsertIds);
            upserts.forEach(f -> (existingIds.contains(f.getId()) ? updates : inserts).add(f));
          }

          /** get next Version */
          if(event.isEnableGlobalVersioning()) {
              SQLQuery query = SQLQueryBuilder.buildGetNextVersionQuery(table);
              version = executeQueryWithRetry(query, rs -> {
                    if (rs.next()) {
                        return rs.getInt(1);
                    }
                    return -1;
                }, false);   // false -> not use readreplica due to sequence 'update' statement: SELECT nextval("...._hst_seq"')
              collection.setVersion(version);
          }
        } catch (Exception e) {
          if (!retryAttempted) {
            canRetryAttempt();
            return executeModifyFeatures(event);
          }
        }

        try (final Connection connection = dataSource.getConnection()) {

            boolean previousAutoCommitState = connection.getAutoCommit();
            connection.setAutoCommit(!transactional);

            try {
                if (deletes.size() > 0) {
                    DatabaseWriter.deleteFeatures(this, schema, table, traceItem, fails, deletes, connection, transactional, handleUUID, version);
                }
                if (inserts.size() > 0) {
                    DatabaseWriter.insertFeatures(this, schema, table, traceItem, collection, fails, inserts, connection, transactional, version, forExtendedSpace);
                }
                if (updates.size() > 0) {
                    DatabaseWriter.updateFeatures(this, schema, table, traceItem, collection, fails, updates, connection, transactional, handleUUID, version, forExtendedSpace);
                }

                if (transactional) {
                    /** Commit SQLS in one transaction */
                    connection.commit();
                }
            } catch (Exception e) {
                /** No time left for processing */
                if(e instanceof SQLException && ((SQLException)e).getSQLState() !=null
                        &&((SQLException)e).getSQLState().equalsIgnoreCase("54000")) {
                    throw e;
                }

                /** Add objects which are responsible for the failed operation */
                event.setFailed(fails);

                if (retryCausedOnServerlessDB(e) && !retryAttempted) {
                    retryAttempted = true;

                    if(!connection.isClosed())
                    { connection.setAutoCommit(previousAutoCommitState);
                      connection.close();
                    }

                    return executeModifyFeatures(event);
                }

                if (transactional) {
                    connection.rollback();

                    if ((e instanceof SQLException && ((SQLException)e).getSQLState() != null
                            && ((SQLException)e).getSQLState().equalsIgnoreCase("42P01")))
                        ;//Table does not exist yet - create it!
                    else {

                        logger.warn("{} Transaction has failed. {]", traceItem, e);
                        connection.close();

                        Map<String, Object> errorDetails = new HashMap<>();

                        if(e instanceof BatchUpdateException || fails.size() >=1 ){
                            //23505 = Object already exists
                            if(e instanceof BatchUpdateException && !((BatchUpdateException) e).getSQLState().equalsIgnoreCase("23505"))
                                throw e;

                            errorDetails.put("FailedList", fails);
                            return new ErrorResponse().withErrorDetails(errorDetails).withError(XyzError.CONFLICT).withErrorMessage(DatabaseWriter.TRANSACTION_ERROR_GENERAL);
                        }else {
                            errorDetails.put(DatabaseWriter.TRANSACTION_ERROR_GENERAL,
                                    (e instanceof SQLException && ((SQLException)e).getSQLState() != null)
                                            ? "SQL-state: "+((SQLException) e).getSQLState() : "Unexpected Error occurred");
                            return new ErrorResponse().withErrorDetails(errorDetails).withError(XyzError.BAD_GATEWAY).withErrorMessage(DatabaseWriter.TRANSACTION_ERROR_GENERAL);
                        }
                    }
                }

                if (!retryAttempted) {
                    if(!connection.isClosed()) {
                        connection.setAutoCommit(previousAutoCommitState);
                        connection.close();
                    }
                    /** Retry */
                    canRetryAttempt();
                    return executeModifyFeatures(event);
                }
            }
            finally
            {
                if(!connection.isClosed()) {
                    connection.setAutoCommit(previousAutoCommitState);
                    connection.close();
                }
            }

            if(event.isEnableGlobalVersioning() && event.getMaxVersionCount() != null) {
                SQLQuery q = dbMaintainer.maintainHistory(traceItem, schema, table, version,  event.getMaxVersionCount());
                if(q != null) {
                    executeUpdateWithRetry(q);
                }
            }

            /** filter out failed ids */
            final List<String> failedIds = fails.stream().map(FeatureCollection.ModificationFailure::getId).filter(Objects::nonNull).collect(Collectors.toList());
            final List<String> insertIds = inserts.stream().map(Feature::getId).filter(x -> !failedIds.contains(x)).collect(Collectors.toList());
            final List<String> updateIds = updates.stream().map(Feature::getId).filter(x -> !failedIds.contains(x)).collect(Collectors.toList());
            final List<String> deleteIds = deletes.keySet().stream().filter(x -> !failedIds.contains(x)).collect(Collectors.toList());

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
            connection.close();

            return collection;
        }
    }

    private List<String> getAllIds(List<Feature> inserts, List<Feature> updates, List<Feature> upserts, Map<String, ?> deletes) {
      List<String> ids = Stream.of(inserts, updates, upserts).flatMap(Collection::stream).map(Feature::getId).collect(Collectors.toList());
      ids.addAll(deletes.keySet());

      return ids;
    }

    protected XyzResponse executeDeleteFeaturesByTag(DeleteFeaturesByTagEvent event) throws Exception {
        boolean includeOldStates = event.getParams() != null
                && event.getParams().get(INCLUDE_OLD_STATES) == Boolean.TRUE;

        final SQLQuery searchQuery = SQLQueryBuilder.generateSearchQuery(event, dataSource);
        final SQLQuery query = SQLQueryBuilder.buildDeleteFeaturesByTagQuery(includeOldStates, searchQuery);

        //TODO: check in detail what we want to return
        if (searchQuery != null && includeOldStates)
            return executeQueryWithRetry(query, this::oldStatesResultSetHandler,false);

        return new FeatureCollection().withCount((long) executeUpdateWithRetry(query));
    }

    private boolean canRetryAttempt() throws Exception {

        if (retryAttempted || !isRemainingTimeSufficient(context.getRemainingTimeInMillis() / 1000)) {
            return false;
        }

        ensureSpace();
        retryAttempted = true;

        logger.info("{} Retry the execution.", traceItem);
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

        long start = System.currentTimeMillis();

        try (final Connection connection = dataSource.getConnection()) {
            Statement stmt = connection.createStatement();
            String query = "SELECT to_regclass('${schema}.${table}')";

            query = SQLQuery.replaceVars(query, config.getDatabaseSettings().getSchema(), config.readTableFromEvent(event));
            ResultSet rs;

            stmt.setQueryTimeout(calculateTimeout());
            if ((rs = stmt.executeQuery(query)).next()) {
                logger.debug("{} Time for table check: " + (System.currentTimeMillis() - start) + "ms", traceItem);
                String oid = rs.getString(1);
                return oid != null ? true : false;
            }
            return false;
        }catch (Exception e){
            if(!retryAttempted) {
                retryAttempted = true;
                logger.info("{} Retry table check.", traceItem);
                return hasTable();
            }
            else
                throw e;
        }
    }

    /**
     * A helper method that will ensure that the tables for the space of this event do exist and is up to date, if not it will alter the
     * table.
     *
     * @throws SQLException if the table does not exist and can't be created or alter failed.
     */

    private static String lockSql   = "select pg_advisory_lock( ('x' || left(md5('%s'),15) )::bit(60)::bigint )",
                          unlockSql = "select pg_advisory_unlock( ('x' || left(md5('%s'),15) )::bit(60)::bigint )";

    private static void _advisory(String tablename, Connection connection,boolean lock ) throws SQLException
    {
     boolean cStateFlag = connection.getAutoCommit();
     connection.setAutoCommit(true);

     try(Statement stmt = connection.createStatement())
     { stmt.executeQuery(DhString.format( lock? lockSql : unlockSql,tablename)); }

     connection.setAutoCommit(cStateFlag);
    }

    private static void advisoryLock(String tablename, Connection connection ) throws SQLException { _advisory(tablename,connection,true); }

    private static void advisoryUnlock(String tablename, Connection connection ) throws SQLException { _advisory(tablename,connection,false); }

    private static boolean isForExtendedSpace(Event event) {
        return event.getParams() != null && event.getParams().containsKey("extends");
    }

    protected void ensureSpace() throws SQLException {
        // Note: We can assume that when the table exists, the postgis extensions are installed.
        if (hasTable()) return;

        final String tableName = config.readTableFromEvent(event);

        try (final Connection connection = dataSource.getConnection()) {
            advisoryLock( tableName, connection );
            boolean cStateFlag = connection.getAutoCommit();
            try {

                if (cStateFlag)
                  connection.setAutoCommit(false);

                try (Statement stmt = connection.createStatement()) {
                    createSpaceStatement(stmt, tableName, isForExtendedSpace(event));

                    stmt.setQueryTimeout(calculateTimeout());
                    stmt.executeBatch();
                    connection.commit();
                    logger.debug("{} Successfully created table '{}' for space id '{}'", traceItem, tableName, event.getSpace());
                }
            } catch (Exception e) {
                logger.error("{} Failed to create table '{}' for space id: '{}': {}", traceItem, tableName, event.getSpace(), e);
                connection.rollback();
                // check if the table was created in the meantime, by another instance.
                if (hasTable()) {
                    return;
                }
                throw new SQLException("Missing table " + SQLQuery.sqlQuote(tableName) + " and creation failed: " + e.getMessage(), e);
            } finally {
                advisoryUnlock( tableName, connection );
                if (cStateFlag)
                 connection.setAutoCommit(true);
            }
        }
    }

    private void createSpaceStatement(Statement stmt, String tableName) throws SQLException {
        createSpaceStatement(stmt, tableName, false);
    }

    private void createSpaceStatement(Statement stmt, String tableName, boolean withDeletedColumn) throws SQLException {
        String query = "CREATE TABLE IF NOT EXISTS ${schema}.${table} (jsondata jsonb, geo geometry(GeometryZ,4326), i BIGSERIAL"
            + (withDeletedColumn ? ", deleted BOOLEAN DEFAULT FALSE" : "") + ")";

        query = SQLQuery.replaceVars(query, config.getDatabaseSettings().getSchema(), tableName);
        stmt.addBatch(query);

        if (withDeletedColumn) {
            query = "CREATE INDEX IF NOT EXISTS ${idx_deleted} ON ${schema}.${table} USING btree (deleted ASC NULLS LAST) WHERE deleted = TRUE";
            query = SQLQuery.replaceVars(query, replacements, config.getDatabaseSettings().getSchema(), tableName);
            stmt.addBatch(query);
        }

        query = "CREATE UNIQUE INDEX IF NOT EXISTS ${idx_id} ON ${schema}.${table} ((jsondata->>'id'))";
        query = SQLQuery.replaceVars(query, replacements, config.getDatabaseSettings().getSchema(), tableName);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_tags} ON ${schema}.${table} USING gin ((jsondata->'properties'->'@ns:com:here:xyz'->'tags') jsonb_ops)";
        query = SQLQuery.replaceVars(query, replacements, config.getDatabaseSettings().getSchema(), tableName);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_geo} ON ${schema}.${table} USING gist ((geo))";
        query = SQLQuery.replaceVars(query, replacements, config.getDatabaseSettings().getSchema(), tableName);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_serial} ON ${schema}.${table}  USING btree ((i))";
        query = SQLQuery.replaceVars(query, replacements, config.getDatabaseSettings().getSchema(), tableName);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_updatedAt} ON ${schema}.${table} USING btree ((jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt'), (jsondata->>'id'))";
        query = SQLQuery.replaceVars(query, replacements, config.getDatabaseSettings().getSchema(), tableName);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_createdAt} ON ${schema}.${table} USING btree ((jsondata->'properties'->'@ns:com:here:xyz'->'createdAt'), (jsondata->>'id'))";
        query = SQLQuery.replaceVars(query, replacements, config.getDatabaseSettings().getSchema(), tableName);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_viz} ON ${schema}.${table} USING btree (left( md5(''||i),5))";
        query = SQLQuery.replaceVars(query, replacements, config.getDatabaseSettings().getSchema(), tableName);
        stmt.addBatch(query);

        stmt.setQueryTimeout(calculateTimeout());
    }

    protected void ensureHistorySpace(Integer maxVersionCount, boolean compactHistory, boolean isEnableGlobalVersioning) throws SQLException {
        final String tableName = config.readTableFromEvent(event);

        try (final Connection connection = dataSource.getConnection()) {
            advisoryLock( tableName, connection );
            boolean cStateFlag = connection.getAutoCommit();
            try {
                if (cStateFlag)
                 connection.setAutoCommit(false);

                try (Statement stmt = connection.createStatement()) {
                    /** Create Space-Table */
                    createSpaceStatement(stmt, tableName);

                    String query = "CREATE TABLE IF NOT EXISTS ${schema}.${hsttable} (uuid text NOT NULL, jsondata jsonb, geo geometry(GeometryZ,4326)," +
                            (isEnableGlobalVersioning ? " vid text ," : "")+
                            " CONSTRAINT \""+tableName+"_pkey\" PRIMARY KEY (uuid))";
                    query = SQLQuery.replaceVars(query, config.getDatabaseSettings().getSchema(), tableName);
                    stmt.addBatch(query);

                    query = "CREATE INDEX IF NOT EXISTS ${idx_hst_uuid} ON ${schema}.${hsttable} USING btree (uuid)";
                    query = SQLQuery.replaceVars(query, replacements, config.getDatabaseSettings().getSchema(), tableName);
                    stmt.addBatch(query);

                    query = "CREATE INDEX IF NOT EXISTS ${idx_hst_id} ON ${schema}.${hsttable} ((jsondata->>'id'))";
                    query = SQLQuery.replaceVars(query, replacements, config.getDatabaseSettings().getSchema(), tableName);
                    stmt.addBatch(query);

                    query = "CREATE INDEX IF NOT EXISTS ${idx_hst_updatedAt} ON ${schema}.${hsttable} USING btree ((jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt'))";
                    query = SQLQuery.replaceVars(query, replacements, config.getDatabaseSettings().getSchema(), tableName);
                    stmt.addBatch(query);

                    if(isEnableGlobalVersioning) {
                        query = "CREATE INDEX IF NOT EXISTS ${idx_hst_deleted} ON ${schema}.${hsttable} USING btree (((jsondata->'properties'->'@ns:com:here:xyz'->'deleted')::jsonb))";
                        query = SQLQuery.replaceVars(query, replacements, config.getDatabaseSettings().getSchema(), tableName);
                        stmt.addBatch(query);

                        query = "CREATE INDEX IF NOT EXISTS ${idx_hst_version} ON ${schema}.${hsttable} USING btree (((jsondata->'properties'->'@ns:com:here:xyz'->'version')::jsonb))";
                        query = SQLQuery.replaceVars(query, replacements, config.getDatabaseSettings().getSchema(), tableName);
                        stmt.addBatch(query);

                        query = "CREATE INDEX IF NOT EXISTS ${idx_hst_lastVersion} ON ${schema}.${hsttable} USING btree (((jsondata->'properties'->'@ns:com:here:xyz'->'lastVersion')::jsonb))";
                        query = SQLQuery.replaceVars(query, replacements, config.getDatabaseSettings().getSchema(), tableName);
                        stmt.addBatch(query);

                        query = "CREATE INDEX IF NOT EXISTS ${idx_hst_idvsort} ON ${schema}.${hsttable} USING btree ((jsondata ->> 'id'::text), ((jsondata->'properties'->'@ns:com:here:xyz'->'version')::jsonb) DESC )";
                        query = SQLQuery.replaceVars(query, replacements, config.getDatabaseSettings().getSchema(), tableName);
                        stmt.addBatch(query);

                        query = "CREATE INDEX IF NOT EXISTS ${idx_hst_vidsort} ON ${schema}.${hsttable} USING btree (((jsondata->'properties'->'@ns:com:here:xyz'->'version')::jsonb) , (jsondata ->> 'id'::text))";
                        query = SQLQuery.replaceVars(query, replacements, config.getDatabaseSettings().getSchema(), tableName);
                        stmt.addBatch(query);

                        query = "CREATE SEQUENCE  IF NOT EXISTS " + config.getDatabaseSettings().getSchema() + ".\"" + tableName.replaceAll("-", "_") + "_hst_seq\"";
                        query = SQLQuery.replaceVars(query, replacements, config.getDatabaseSettings().getSchema(), tableName);
                        stmt.addBatch(query);
                    }

                    if(!isEnableGlobalVersioning) {
                        /** old naming */
                        query = SQLQueryBuilder.deleteHistoryTriggerSQL(config.getDatabaseSettings().getSchema(), tableName)[0];
                        stmt.addBatch(query);
                        /** new naming */
                        query = SQLQueryBuilder.deleteHistoryTriggerSQL(config.getDatabaseSettings().getSchema(), tableName)[1];
                        stmt.addBatch(query);
                    }

                    query = SQLQueryBuilder.addHistoryTriggerSQL(config.getDatabaseSettings().getSchema(), tableName, maxVersionCount, compactHistory, isEnableGlobalVersioning);
                    stmt.addBatch(query);

                    stmt.setQueryTimeout(calculateTimeout());
                    stmt.executeBatch();
                    connection.commit();
                    logger.debug("{} Successfully created history table '{}' for space id '{}'", traceItem, tableName, event.getSpace());
                }
            } catch (Exception e) {
                throw new SQLException("Creation of history table has failed: "+tableName, e);
            } finally {
              advisoryUnlock( tableName, connection );
              if (cStateFlag)
                connection.setAutoCommit(true);
            }
        }
    }

    protected void updateHistoryTrigger(Integer maxVersionCount, boolean compactHistory, boolean isEnableGlobalVersioning) throws SQLException {
        final String tableName = config.readTableFromEvent(event);

        try (final Connection connection = dataSource.getConnection()) {
            advisoryLock( tableName, connection );
            boolean cStateFlag = connection.getAutoCommit();
            try {
                if (cStateFlag)
                    connection.setAutoCommit(false);

                try (Statement stmt = connection.createStatement()) {
                    /** Create Space-Table */
                    createSpaceStatement(stmt, tableName);

                    /** old naming */
                    String query = SQLQueryBuilder.deleteHistoryTriggerSQL(config.getDatabaseSettings().getSchema(), tableName)[0];
                    stmt.addBatch(query);
                    /** new naming */
                    query = SQLQueryBuilder.deleteHistoryTriggerSQL(config.getDatabaseSettings().getSchema(), tableName)[1];
                    stmt.addBatch(query);

                    query = SQLQueryBuilder.addHistoryTriggerSQL(config.getDatabaseSettings().getSchema(), tableName, maxVersionCount, compactHistory, isEnableGlobalVersioning);
                    stmt.addBatch(query);

                    stmt.setQueryTimeout(calculateTimeout());
                    stmt.executeBatch();
                    connection.commit();
                    logger.debug("{} Updated of trigger has failed: '{}'", traceItem, tableName);
                }
            } catch (Exception e) {
                throw new SQLException("Update of trigger has failed: "+tableName, e);
            } finally {
                advisoryUnlock( tableName, connection );
                if (cStateFlag)
                    connection.setAutoCommit(true);
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

    private final long MAX_RESULT_CHARS = 100 * 1024 *1024;
    private final int F_Iterate = 1,
                      F_IterateOrderBy  = 2;

    protected FeatureCollection _defaultFeatureResultSetHandler(ResultSet rs, boolean skipNullGeom) throws SQLException {
        int hint = ((event instanceof IterateFeaturesEvent) ? F_Iterate : ((event instanceof SearchForFeaturesOrderByEvent ) ? F_IterateOrderBy : 0) );
        String nextHandle = "";

        StringBuilder sb = new StringBuilder();
        String prefix = "[";
        sb.append(prefix);
        int numFeatures = 0;

        while (rs.next() && MAX_RESULT_CHARS > sb.length()) {
            String geom = rs.getString(2);
            if( skipNullGeom && (geom == null) ) continue;
            sb.append(rs.getString(1));
            sb.setLength(sb.length() - 1);
            sb.append(",\"geometry\":");
            sb.append(geom == null ? "null" : geom);
            sb.append("}");
            sb.append(",");

            if ( hint > 0 ) numFeatures++;
            switch( hint )
            { case F_Iterate         : nextHandle = "" + rs.getLong(3); break;
              case F_IterateOrderBy  : nextHandle = rs.getString(3); break;
            }

        }

        if (sb.length() > prefix.length()) {
            sb.setLength(sb.length() - 1);
        }
        sb.append("]");

        final FeatureCollection featureCollection = new FeatureCollection();
        featureCollection._setFeatures(sb.toString());

        if (sb.length() > MAX_RESULT_CHARS) throw new SQLException(DhString.format("Maxchar limit(%d) reached", MAX_RESULT_CHARS));

        if( hint > 0 && numFeatures > 0 && numFeatures == ((SearchForFeaturesEvent) event).getLimit() ) {
          featureCollection.setHandle(nextHandle);
          featureCollection.setNextPageToken(nextHandle);
        }

        return featureCollection;
    }

    public FeatureCollection defaultFeatureResultSetHandler(ResultSet rs) throws SQLException
    { return _defaultFeatureResultSetHandler(rs,false); }

    protected FeatureCollection defaultFeatureResultSetHandlerSkipIfGeomIsNull(ResultSet rs) throws SQLException
    { return _defaultFeatureResultSetHandler(rs,true); }

    protected BinaryResponse defaultBinaryResultSetHandler(ResultSet rs) throws SQLException {
        BinaryResponse br = new BinaryResponse()
            .withMimeType(APPLICATION_VND_MAPBOX_VECTOR_TILE);

        if (rs.next())
            br.setBytes(rs.getBytes(1));

        if (br.getBytes() != null && br.getBytes().length > MAX_RESULT_CHARS)
            throw new SQLException(DhString.format("Maximum bytes limit (%d) reached", MAX_RESULT_CHARS));

        return br;
    }

    /**
     * handler for iterate through history.
     *
     * @param rs the result set.
     * @return the generated CompactChangeset from the result set.
     * @throws SQLException when any unexpected error happened.
     */
    protected CompactChangeset compactHistoryResultSetHandler(ResultSet rs) throws SQLException {
        long numFeatures = 0;
        long limit = ((IterateHistoryEvent) event).getLimit();
        String id = "";

        CompactChangeset cc = new CompactChangeset();

        List<Feature> inserts = new ArrayList<>();
        List<Feature> updates = new ArrayList<>();
        List<Feature> deletes = new ArrayList<>();

        while (rs.next()) {
            Feature feature;
            String operation = rs.getString("Operation");
            try {
                feature =  new ObjectMapper().readValue(rs.getString("Feature"), Feature.class);
            }catch (JsonProcessingException e){
                logger.error("{} Error in compactHistoryResultSetHandler for space id '{}': {}", traceItem, event.getSpace(),e);
                throw new SQLException("Cant read json from database!");
            }

            switch (operation){
                case "INSERTED":
                    inserts.add(feature);
                    break;
                case "UPDATED":
                    updates.add(feature);
                    break;
                case "DELETED":
                    deletes.add(feature);
                    break;
            }
            id = rs.getString("id");
            numFeatures++;
        }

        cc.setInserted(new FeatureCollection().withFeatures(inserts));
        cc.setUpdated(new FeatureCollection().withFeatures(updates));
        cc.setDeleted(new FeatureCollection().withFeatures(deletes));

        if (numFeatures > 0 && numFeatures == limit) {
            cc.setNextPageToken(id);
        }

        return cc;
    }

    /**
     * handler for iterate through history.
     *
     * @param rs the result set.
     * @return the generated ChangesetCollection from the result set.
     * @throws SQLException when any unexpected error happened.
     */
    protected ChangesetCollection historyResultSetHandler(ResultSet rs) throws SQLException {
        long numFeatures = 0;
        long limit = ((IterateHistoryEvent) event).getLimit();
        String npt = ((IterateHistoryEvent) event).getPageToken();

        ChangesetCollection ccol = new ChangesetCollection();
        Map<Integer,Changeset> versions = new HashMap<>();
        Integer lastVersion = null;
        Integer startVersion = null;
        boolean wroteStart = false;

        List<Feature> inserts = new ArrayList<>();
        List<Feature> updates = new ArrayList<>();
        List<Feature> deletes = new ArrayList<>();

        while (rs.next()) {
            Feature feature = null;
            String operation = rs.getString("Operation");
            Integer version = rs.getInt("Version");

            if(!wroteStart){
                startVersion = version;
                wroteStart = true;
            }

            if(lastVersion !=  null && version > lastVersion) {
                Changeset cs = new Changeset().withInserted(new FeatureCollection().withFeatures(inserts))
                        .withUpdated(new FeatureCollection().withFeatures(updates))
                        .withDeleted(new FeatureCollection().withFeatures(deletes));
                versions.put(lastVersion, cs);
                inserts = new ArrayList<>();
                updates = new ArrayList<>();
                deletes = new ArrayList<>();
            }

            try {
                feature =  new ObjectMapper().readValue(rs.getString("Feature"), Feature.class);
            }catch (JsonProcessingException e){
                logger.error("{} Error in historyResultSetHandler for space id '{}': {}", traceItem, event.getSpace(),e);
                throw new SQLException("Cant read json from database!");
            }

            switch (operation){
                case "INSERTED":
                    inserts.add(feature);
                    break;
                case "UPDATED":
                    updates.add(feature);
                    break;
                case "DELETED":
                    deletes.add(feature);
                    break;
            }

            npt = rs.getString("vid");
            lastVersion = version;
            numFeatures++;
        }

        if(wroteStart) {
            Changeset cs = new Changeset().withInserted(new FeatureCollection().withFeatures(inserts))
                    .withUpdated(new FeatureCollection().withFeatures(updates))
                    .withDeleted(new FeatureCollection().withFeatures(deletes));
            versions.put(lastVersion, cs);
            ccol.setStartVersion(startVersion);
            ccol.setEndVersion(lastVersion);
        }

        ccol.setVersions(versions);

        if (numFeatures > 0 && numFeatures == limit) {
            ccol.setNextPageToken(npt);
        }

        return ccol;
    }

    /**
     * handler for iterate through versions.
     *
     * @param rs the result set.
     * @return the generated feature collection from the result set.
     * @throws SQLException when any unexpected error happened.
     */
    protected FeatureCollection iterateVersionsHandler(ResultSet rs) throws SQLException {
        String id="";

        StringBuilder sb = new StringBuilder();
        String prefix = "[";
        sb.append(prefix);
        int numFeatures = 0;

        while (rs.next() && MAX_RESULT_CHARS > sb.length()) {
            String geom = rs.getString("geo");
            sb.append(rs.getString("jsondata"));
            sb.setLength(sb.length() - 1);
            sb.append(",\"geometry\":");
            sb.append(geom == null ? "null" : geom);
            sb.append("}");
            sb.append(",");

            id = rs.getString("id");
            numFeatures++;
        }

        if (sb.length() > prefix.length()) {
            sb.setLength(sb.length() - 1);
        }
        sb.append("]");

        final FeatureCollection featureCollection = new FeatureCollection();
        featureCollection._setFeatures(sb.toString());

        if( MAX_RESULT_CHARS <= sb.length() ) throw new SQLException(DhString.format("Maxchar limit(%d) reached", MAX_RESULT_CHARS));

        if (numFeatures > 0 && numFeatures == ((IterateFeaturesEvent) event).getLimit()) {
            featureCollection.setHandle(id);
            featureCollection.setNextPageToken(id);
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
     * handler for list of feature's id results.
     *
     * @param rs the result set.
     * @return the generated feature collection from the result set.
     * @throws SQLException when any unexpected error happened.
     */
    protected List<String> idsListResultSetHandler(ResultSet rs) throws SQLException {
      final ArrayList<String> result = new ArrayList<>();

      while (rs.next()) {
        result.add(rs.getString("id"));
      }

      return result;
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

    protected int calculateTimeout() throws SQLException{
        int remainingSeconds = context.getRemainingTimeInMillis() / 1000;

        if(!isRemainingTimeSufficient(remainingSeconds)) {
            throw new SQLException("No time left to execute query.","54000");
        }

        int timeout = remainingSeconds >= STATEMENT_TIMEOUT_SECONDS ? STATEMENT_TIMEOUT_SECONDS :
                (remainingSeconds - 2);

        logger.debug("{} New timeout for query set to '{}'", traceItem, timeout);
        return timeout;
    }

    protected boolean isRemainingTimeSufficient(int remainingSeconds){
        if(remainingSeconds <= MIN_REMAINING_TIME_FOR_RETRY_SECONDS) {
            logger.warn("{} No time left to execute query '{}' s", traceItem, remainingSeconds);
            return false;
        }
        return true;
    }

    /**
     * The result handler for a getHistoryStatisticsEvent.
     *
     * @param rs the result set.
     * @return the feature collection generated from the result.
     *
     */
    protected XyzResponse getHistoryStatisticsResultSetHandler(ResultSet rs){
        try {
            rs.next();
            StatisticsResponse.Value<Long> tablesize = XyzSerializable.deserialize(rs.getString("tablesize"), new TypeReference<StatisticsResponse.Value<Long>>() {});
            StatisticsResponse.Value<Long> count = XyzSerializable.deserialize(rs.getString("count"), new TypeReference<StatisticsResponse.Value<Long>>() {});
            StatisticsResponse.Value<Integer> maxversion = XyzSerializable.deserialize(rs.getString("maxversion"), new TypeReference<StatisticsResponse.Value<Integer>>() {});
            StatisticsResponse.Value<Integer> minversion = XyzSerializable.deserialize(rs.getString("minversion"), new TypeReference<StatisticsResponse.Value<Integer>>() {});

            return new HistoryStatisticsResponse()
                    .withByteSize(tablesize)
                    .withDataSize(tablesize)
                    .withCount(count)
                    .withMinVersion(minversion)
                    .withMaxVersion(maxversion);
        } catch (Exception e) {
            return new ErrorResponse().withStreamId(streamId).withError(XyzError.EXCEPTION).withErrorMessage(e.getMessage());
        }
    }

    /**
     * The result handler for a getStatistics event.
     *
     * @param rs the result set.
     * @return the feature collection generated from the result.
     *
     */
    protected XyzResponse getStatisticsResultSetHandler(ResultSet rs){
        try {
            rs.next();

            StatisticsResponse.Value<Long> tablesize = XyzSerializable.deserialize(rs.getString("tablesize"), new TypeReference<StatisticsResponse.Value<Long>>() {});
            StatisticsResponse.Value<List<String>> geometryTypes = XyzSerializable
                    .deserialize(rs.getString("geometryTypes"), new TypeReference<StatisticsResponse.Value<List<String>>>() {
            });
            StatisticsResponse.Value<List<StatisticsResponse.PropertyStatistics>> tags = XyzSerializable
                    .deserialize(rs.getString("tags"), new TypeReference<StatisticsResponse.Value<List<StatisticsResponse.PropertyStatistics>>>() {
            });
            StatisticsResponse.PropertiesStatistics properties = XyzSerializable.deserialize(rs.getString("properties"), StatisticsResponse.PropertiesStatistics.class);
            StatisticsResponse.Value<Long> count = XyzSerializable.deserialize(rs.getString("count"), new TypeReference<StatisticsResponse.Value<Long>>() {});
            Map<String, Object> bboxMap = XyzSerializable.deserialize(rs.getString("bbox"), new TypeReference<Map<String, Object>>() {});

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
                    .withDataSize(tablesize)
                    .withCount(count)
                    .withGeometryTypes(geometryTypes)
                    .withTags(tags)
                    .withProperties(properties);
        } catch (Exception e) {
            return new ErrorResponse().withStreamId(streamId).withError(XyzError.EXCEPTION).withErrorMessage(e.getMessage());
        }
    }

    public PSQLConfig getConfig() {
        return config;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public String getStreamId() {
        return streamId;
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
