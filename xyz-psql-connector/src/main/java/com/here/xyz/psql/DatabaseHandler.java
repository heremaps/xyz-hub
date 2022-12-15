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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.models.hub.Space.DEFAULT_VERSIONS_TO_KEEP;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.DELETE;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.INSERT;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.UPDATE;
import static com.here.xyz.psql.query.helpers.GetNextVersion.VERSION_SEQUENCE_SUFFIX;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.connectors.SimulatedContext;
import com.here.xyz.connectors.StorageConnector;
import com.here.xyz.events.DeleteFeaturesByTagEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.IterateHistoryEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySpaceEvent.Operation;
import com.here.xyz.events.ModifySubscriptionEvent;
import com.here.xyz.events.OneTimeActionEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.psql.config.ConnectorParameters;
import com.here.xyz.psql.config.DatabaseSettings;
import com.here.xyz.psql.config.PSQLConfig;
import com.here.xyz.psql.query.ExtendedSpace;
import com.here.xyz.psql.query.ModifySpace;
import com.here.xyz.psql.query.helpers.FetchExistingIds;
import com.here.xyz.psql.query.helpers.FetchExistingIds.FetchIdsInput;
import com.here.xyz.psql.query.helpers.GetTablesWithColumn;
import com.here.xyz.psql.query.helpers.GetNextVersion;
import com.here.xyz.psql.query.helpers.GetTablesWithColumn.GetTablesWithColumnInput;
import com.here.xyz.psql.query.helpers.GetTablesWithComment;
import com.here.xyz.psql.query.helpers.GetTablesWithComment.GetTablesWithCommentInput;
import com.here.xyz.psql.query.helpers.SetVersion;
import com.here.xyz.psql.query.helpers.SetVersion.SetVersionInput;
import com.here.xyz.psql.tools.DhString;
import com.here.xyz.responses.BinaryResponse;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.HealthStatus;
import com.here.xyz.responses.HistoryStatisticsResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.responses.changesets.Changeset;
import com.here.xyz.responses.changesets.ChangesetCollection;
import com.here.xyz.responses.changesets.CompactChangeset;
import com.mchange.v2.c3p0.AbstractConnectionCustomizer;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.PooledDataSource;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.json.JSONObject;

public abstract class DatabaseHandler extends StorageConnector {
    private static final Logger logger = LogManager.getLogger();

    private static final Pattern pattern = Pattern.compile("^BOX\\(([-\\d\\.]*)\\s([-\\d\\.]*),([-\\d\\.]*)\\s([-\\d\\.]*)\\)$");
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
    public static final String OTA_PHASE_1_COMPLETE = "phase1_complete";
    public static final String OTA_PHASE_1_STARTED = "phase1_started";

    public static final String OTA_PHASE_X_COMPLETE = "phaseX_complete";

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

    @Override
    protected XyzResponse processOneTimeActionEvent(OneTimeActionEvent event) throws Exception {
        try {
            if (executeOneTimeAction(event.getPhase(), event.getInputData()))
                return new SuccessResponse().withStatus("EXECUTED");
            return new SuccessResponse().withStatus("ALREADY RUNNING");
        }
        catch (Exception e) {
            logger.error("OTA: Error during one time action execution:", e);
            return new ErrorResponse()
                .withErrorMessage(e.getMessage())
                .withError(XyzError.EXCEPTION);
        }
    }

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

        HealthStatus status = ((HealthStatus) super.processHealthCheckEvent(event)).withStatus("OK");

        try {
            if (System.getenv("OTA_PHASE") != null) {
                Map<String, Object> inputData = null;
                if (System.getenv("OTA_INPUT_DATA") != null)
                    inputData = new JSONObject(System.getenv("OTA_INPUT_DATA")).toMap();
                executeOneTimeAction(System.getenv("OTA_PHASE"), inputData);
            }
        }
        catch (Exception e) {
            logger.error("OTA: Error during one time action execution:", e);
        }

        return status;
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
        String hstTable = table+HISTORY_TABLE_SUFFIX;

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

    public FeatureCollection executeQueryWithRetry(SQLQuery query, boolean useReadReplica) throws SQLException {
        return executeQueryWithRetry(query, this::defaultFeatureResultSetHandler, useReadReplica);
    }

    public FeatureCollection executeQueryWithRetry(SQLQuery query) throws SQLException {
        return executeQueryWithRetry(query, true);
    }

    protected FeatureCollection executeQueryWithRetrySkipIfGeomIsNull(SQLQuery query) throws SQLException {
        return executeQueryWithRetry(query, this::defaultFeatureResultSetHandlerSkipIfGeomIsNull, true);
    }

    protected BinaryResponse executeBinQueryWithRetry(SQLQuery query, boolean useReadReplica) throws SQLException {
        return executeQueryWithRetry(query, this::defaultBinaryResultSetHandler, useReadReplica);
    }

    protected BinaryResponse executeBinQueryWithRetry(SQLQuery query) throws SQLException {
        return executeBinQueryWithRetry(query, true);
    }

    /**
     *
     * Executes the query and reattempt to execute the query, after
     */
    protected <T> T executeQueryWithRetry(SQLQuery query, ResultSetHandler<T> handler, boolean useReadReplica) throws SQLException {
        try {
            query.replaceUnnamedParameters();
            query.replaceFragments();
            query.replaceNamedParameters();
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

            query.setText(SQLQuery.replaceVars(query.text(), config.getDatabaseSettings().getSchema(), config.readTableFromEvent(event)));
            final String queryText = query.text();
            final List<Object> queryParameters = query.parameters();
            logger.debug("{} executeUpdate: {} - Parameter: {}", traceItem, queryText, queryParameters);
            return run.update(queryText, queryParameters.toArray());
        } finally {
            final long end = System.currentTimeMillis();
            logger.info("{} query time: {}ms", traceItem, (end - start));
        }
    }

    protected XyzResponse executeModifySpace(ModifySpaceEvent event) throws SQLException, ErrorResponseException {
        if (event.getSpaceDefinition() != null && event.getSpaceDefinition().isEnableHistory()) {
            Integer maxVersionCount = event.getSpaceDefinition().getMaxVersionCount();
            boolean isEnableGlobalVersioning = event.getSpaceDefinition().isEnableGlobalVersioning();
            boolean compactHistory = config.getConnectorParams().isCompactHistory();

            if (event.getOperation() == Operation.CREATE)
                //Create History Table
                ensureHistorySpace(maxVersionCount, compactHistory, isEnableGlobalVersioning);
            else if(event.getOperation() == Operation.UPDATE)
                //Update HistoryTrigger to apply maxVersionCount.
                updateHistoryTrigger(maxVersionCount, compactHistory, isEnableGlobalVersioning);
        }

        new ModifySpace(event, this).write();
        if (event.getOperation() != Operation.DELETE)
            dbMaintainer.maintainSpace(traceItem, config.getDatabaseSettings().getSchema(), config.readTableFromEvent(event));

        //If we reach this point we are okay!
        return new SuccessResponse().withStatus("OK");
    }


    protected XyzResponse executeModifySubscription(ModifySubscriptionEvent event) throws SQLException {

        String space = event.getSpace(),
               tableName  = config.readTableFromEvent(event),
               schemaName = config.getDatabaseSettings().getSchema();

        boolean bLastSubscriptionToDelete = event.getHasNoActiveSubscriptions();

        switch(event.getOperation())
        { case CREATE :
          case UPDATE :
            long rVal = (long) executeUpdateWithRetry( SQLQueryBuilder.buildAddSubscriptionQuery(space, schemaName, tableName ) );
            setReplicaIdentity();
            return new FeatureCollection().withCount(rVal);

          case DELETE :
           if( !bLastSubscriptionToDelete )
            return new FeatureCollection().withCount( 1l );
           else
            return new FeatureCollection().withCount((long) executeUpdateWithRetry(SQLQueryBuilder.buildRemoveSubscriptionQuery(space, schemaName)));

          default: break;
        }

         return null;
       }

    protected XyzResponse executeIterateHistory(IterateHistoryEvent event) throws SQLException {
        if(event.isCompact())
            return executeQueryWithRetry(SQLQueryBuilder.buildSquashHistoryQuery(event), this::compactHistoryResultSetHandler, true);
        return executeQueryWithRetry(SQLQueryBuilder.buildHistoryQuery(event), this::historyResultSetHandler, true);
    }

    protected XyzResponse executeIterateVersions(IterateFeaturesEvent event) throws SQLException {
        SQLQuery query = SQLQueryBuilder.buildLatestHistoryQuery(event);
        return executeQueryWithRetry(query, this::iterateVersionsHandler, true);
    }

    /**
     *
     * @param idsToFetch Ids of objects which should get fetched
     * @return List of Features which could get fetched
     * @throws Exception if any error occurred.
     */
    protected List<Feature> fetchOldStates(ModifyFeaturesEvent event, String[] idsToFetch) throws Exception {
        List<Feature> oldFeatures = null;
        FeatureCollection oldFeaturesCollection = executeQueryWithRetry(SQLQueryBuilder.generateLoadOldFeaturesQuery(event, idsToFetch));

        if (oldFeaturesCollection != null) {
            oldFeatures = oldFeaturesCollection.getFeatures();
        }

        return oldFeatures;
    }

    protected XyzResponse executeModifyFeatures(ModifyFeaturesEvent event) throws Exception {
        final boolean includeOldStates = event.getParams() != null && event.getParams().get(INCLUDE_OLD_STATES) == Boolean.TRUE;

        final String schema = config.getDatabaseSettings().getSchema();
        final String table = config.readTableFromEvent(event);

        final FeatureCollection collection = new FeatureCollection();
        collection.setFeatures(new ArrayList<>());

        List<Feature> inserts = Optional.ofNullable(event.getInsertFeatures()).orElse(new ArrayList<>());
        List<Feature> updates = Optional.ofNullable(event.getUpdateFeatures()).orElse(new ArrayList<>());
        List<Feature> upserts = Optional.ofNullable(event.getUpsertFeatures()).orElse(new ArrayList<>());
        Map<String, String> deletes = Optional.ofNullable(event.getDeleteFeatures()).orElse(new HashMap<>());
        List<FeatureCollection.ModificationFailure> fails = Optional.ofNullable(event.getFailed()).orElse(new ArrayList<>());

        List<String> originalUpdates = updates.stream().map(f -> f.getId()).collect(Collectors.toList());
        List<String> originalDeletes = new ArrayList<>(deletes.keySet());
        //Handle deletes / updates on extended spaces
        if (isForExtendingSpace(event) && event.getContext() == DEFAULT) {
            if (!deletes.isEmpty()) {
                //Transform the incoming deletes into upserts with deleted flag for features which don't exist in the extended layer (base)
                List<String> existingIdsInBase = new FetchExistingIds(
                    //NOTE: The following is a temporary implementation for backwards compatibility for old base spaces which have no id column filled yet
                    new FetchIdsInput(ExtendedSpace.getExtendedTable(event, this), originalDeletes, true), this).run();

                for (String featureId : originalDeletes) {
                    if (existingIdsInBase.contains(featureId)) {
                        upserts.add(new Feature()
                            .withId(featureId)
                            .withProperties(new Properties().withXyzNamespace(new XyzNamespace().withDeleted(true))));
                        deletes.remove(featureId);
                    }
                }
            }
            if (!updates.isEmpty()) {
                //Transform the incoming updates into upserts, because we don't know whether the object is existing in the extension already
                upserts.addAll(updates);
                updates.clear();
            }
        }

        int version = -1;
        try {
          /** Include Old states */
          if (includeOldStates) {
            String[] idsToFetch = getAllIds(inserts, updates, upserts, deletes).stream().filter(Objects::nonNull).toArray(String[]::new);
            List<Feature> oldFeatures = fetchOldStates(event, idsToFetch);
            if (oldFeatures != null) {
              collection.setOldFeatures(oldFeatures);
            }
          }

          /** Include Upserts */
          if (!upserts.isEmpty()) {
            List<String> upsertIds = upserts.stream().map(Feature::getId).filter(Objects::nonNull).collect(Collectors.toList());
            List<String> existingIds = new FetchExistingIds(new FetchIdsInput(config.readTableFromEvent(event),
                //NOTE: The following is a temporary implementation for backwards compatibility for old spaces which have no id column filled yet
                upsertIds, readVersionsToKeep(event) < 1), this).run();
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
                }, false);   // false -> not use readreplica due to sequence 'update' statement: SELECT nextval("...._hst_seq"') and to make sure the read sequence value is the correct (most recent) one
              collection.setVersion(version);
              //NOTE: The following is a temporary implementation for backwards compatibility for old spaces with globalVersioning
              new SetVersion(new SetVersionInput(event, version), this).run();
          }
          else if (event.getVersionsToKeep() > 0) //Backwards compatibility check
              version = new GetNextVersion<>(event, this).run();
        } catch (Exception e) {
          if (!retryAttempted) {
            canRetryAttempt();
            return executeModifyFeatures(event);
          }
        }

        try (final Connection connection = dataSource.getConnection()) {

            boolean previousAutoCommitState = connection.getAutoCommit();
            connection.setAutoCommit(!event.getTransaction());

            try {
                if (deletes.size() > 0) {
                    DatabaseWriter.modifyFeatures(this, event, DELETE, collection, fails, new ArrayList(deletes.entrySet()), connection, version);
                }
                if (inserts.size() > 0) {
                    DatabaseWriter.modifyFeatures(this, event, INSERT, collection, fails, inserts, connection, version);
                }
                if (updates.size() > 0) {
                    DatabaseWriter.modifyFeatures(this, event, UPDATE, collection, fails, updates, connection, version);
                }

                if (event.getTransaction()) {
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

                if (event.getTransaction()) {
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
            final List<String> failedIds = fails.stream().map(FeatureCollection.ModificationFailure::getId)
                .filter(Objects::nonNull).collect(Collectors.toList());
            final List<String> insertIds = inserts.stream().map(Feature::getId)
                .filter(x -> !failedIds.contains(x) && !originalUpdates.contains(x) && !originalDeletes.contains(x)).collect(Collectors.toList());
            final List<String> updateIds = originalUpdates.stream()
                .filter(x -> !failedIds.contains(x) && !originalDeletes.contains(x)).collect(Collectors.toList());
            final List<String> deleteIds = originalDeletes.stream()
                .filter(x -> !failedIds.contains(x)).collect(Collectors.toList());

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

    @Deprecated
    protected XyzResponse executeDeleteFeaturesByTag(DeleteFeaturesByTagEvent event) throws SQLException {
        boolean includeOldStates = event.getParams() != null
                && event.getParams().get(INCLUDE_OLD_STATES) == Boolean.TRUE;

        final SQLQuery searchQuery = SQLQueryBuilder.generateSearchQuery(event);
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
        return hasTable(null);
    }

    protected boolean hasTable(String tableName) throws SQLException {
        if (tableName == null && event instanceof HealthCheckEvent)
            return true;

        if (tableName == null)
            tableName = config.readTableFromEvent(event);

        long start = System.currentTimeMillis();

        try (final Connection connection = dataSource.getConnection()) {
            Statement stmt = connection.createStatement();
            String query = "SELECT to_regclass('${schema}.${table}')";

            query = SQLQuery.replaceVars(query, config.getDatabaseSettings().getSchema(), tableName);
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
                return hasTable(tableName);
            }
            else
                throw e;
        }
    }

    private static boolean _advisory(String key, Connection connection, boolean lock, boolean block) throws SQLException
    {
     boolean cStateFlag = connection.getAutoCommit();
     connection.setAutoCommit(true);
     try (Statement stmt = connection.createStatement()) {
         ResultSet rs = stmt.executeQuery("SELECT pg_" + (lock && !block ? "try_" : "") + "advisory_" + (lock ? "" : "un") + "lock(('x' || left(md5('" + key + "'), 15))::bit(60)::bigint)");
         if (!block && rs.next())
             return rs.getBoolean(1);
         return false;
     }
     finally {
         connection.setAutoCommit(cStateFlag);
     }
    }

    private static void advisoryLock(String key, Connection connection ) throws SQLException { _advisory(key,connection,true, true); }

    private static void advisoryUnlock(String key, Connection connection ) throws SQLException { _advisory(key,connection,false, true); }

    protected static boolean isForExtendingSpace(Event event) {
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
                    createSpaceStatement(stmt, event);

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
                throw new SQLException("Missing table \"" + tableName + "\" and creation failed: " + e.getMessage(), e);
            } finally {
                advisoryUnlock( tableName, connection );
                if (cStateFlag)
                 connection.setAutoCommit(true);
            }
        }
    }

    public static int readVersionsToKeep(Event event) {
        if (event.getParams() == null || !event.getParams().containsKey("versionsToKeep"))
            return DEFAULT_VERSIONS_TO_KEEP;
        return (int) event.getParams().get("versionsToKeep");
    }

    private boolean executeOneTimeAction(String phase, Map<String, Object> inputData) throws SQLException, ErrorResponseException {
        inputData =  getDefaultInputDataForOneTimeAction(phase, inputData);
        boolean phaseLock = "phase1".equals(phase) ? false : true;
        try (final Connection connection = dataSource.getConnection()) {
            logger.info("oneTimeAction " + phase + ": Starting execution ...");
            if (!phaseLock || _advisory(phase, connection, true, false)) {
                try {
                    switch (phase) {
                        case "phase0": {
                            oneTimeActionForVersioning(phase, inputData, connection);
                            break;
                        }
                        case "phase1": {
                            setupOneTimeActionFillNewColumns(phase, connection);
                            oneTimeActionForVersioning(phase, inputData, connection);
                            break;
                        }
                        case "phaseX": {
                            oneTimeActionForVersioning(phase, inputData, connection);
                            break;
                        }
                        case "cleanup": {

                            break;
                        }
                        case "test": {
                            logger.info("oneTimeAction " + phase + ": inputData" + inputData);
                            logger.info("oneTimeAction " + phase + ": Test succeeded.");
                            break;
                        }
                        default:
                            throw new IllegalArgumentException("Illegal OTA phase.");
                    }
                }
                finally {
                    if (phaseLock)
                        _advisory(phase, connection, false, false);
                }
                return true;
            }
            else {
                logger.info("oneTimeActionVersioningMigration " + phase + ": Currently another process is already running.");
                return false;
            }
        }
    }

    private Map<String, Object> getDefaultInputDataForOneTimeAction(String phase, Map<String, Object> inputData) throws SQLException, ErrorResponseException {
        inputData = inputData == null ? new HashMap<>() : new HashMap<>(inputData);
        switch (phase) {
            case "phase0": {
                if (!inputData.containsKey("tableNames"))
                    inputData.put("tableNames",
                        new GetTablesWithColumn(new GetTablesWithColumnInput("id", false, 100), this).run());
                break;
            }
            case "phase1": {
                if (!inputData.containsKey("tableSizeLimit"))
                    inputData.put("tableSizeLimit", 100_000);
                if (!inputData.containsKey("rowProcessingLimit"))
                    inputData.put("rowProcessingLimit", 3_000);
                if (!inputData.containsKey("tableNames"))
                    inputData.put("tableNames", new GetTablesWithComment(new GetTablesWithCommentInput(OTA_PHASE_1_COMPLETE,
                        false, (int) inputData.get("tableSizeLimit"), 100), this).run());
                break;
            }
            case "phaseX": {
                if (!inputData.containsKey("tableSizeLimit"))
                    inputData.put("tableSizeLimit", 1_000_000_000);
                if (!inputData.containsKey("tableNames"))
                    inputData.put("tableNames", new GetTablesWithComment(new GetTablesWithCommentInput(OTA_PHASE_1_COMPLETE,
                        true, (int) inputData.get("tableSizeLimit"), 100), this).run());
                break;
            }
        }
        return inputData;
    }

    private void oneTimeActionForVersioning(String phase, Map<String, Object> inputData, Connection connection) throws SQLException {
        if (inputData == null || !inputData.containsKey("tableNames") || !(inputData.get("tableNames") instanceof List))
            throw new IllegalArgumentException("Table names have to be defined for OTA phase: " + phase);
        List<String> tableNames = (List) inputData.get("tableNames");
        if (tableNames.isEmpty()) {
            logger.info("oneTimeActionVersioningMigration " + phase + ": Nothing to do.");
            return;
        }

        logger.info("Executing " + phase + " for tables: " + String.join(", ", tableNames));
        final String schema = config.getDatabaseSettings().getSchema();
        int processedCount = 0;
        long overallDuration = 0;
        for (String tableName : tableNames) {
            boolean tableCompleted = false;
            logger.info(phase + ": process table: " + tableName);

            if (_advisory(tableName, connection, true, false)) {
                long tableStartTime = System.currentTimeMillis();
                boolean cStateFlag = connection.getAutoCommit();
                try {
                    if (cStateFlag)
                        connection.setAutoCommit(false);

                    switch (phase) {
                        case "phase0": {
                            oneTimeAlterExistingTablesAddNewColumnsAndIndices(connection, schema, tableName);
                            tableCompleted = true;
                            break;
                        }
                        case "phase1": {
                            tableCompleted = oneTimeFillNewColumns(connection, schema, tableName, inputData);
                            break;
                        }
                        case "phaseX": {
                            oneTimeAddConstraintsToOldTables(connection, schema, tableName);
                            tableCompleted = true;
                            break;
                        }
                    }
                    processedCount++;
                    long tableDuration = System.currentTimeMillis() - tableStartTime;
                    overallDuration += tableDuration;
                    logger.info(phase + ": table: " + tableName + (tableCompleted ? " done" : " partially processed") + ". took: " + tableDuration + "ms");
                }
                catch (Exception e) {
                    if (e instanceof SQLException && "54000".equals(((SQLException) e).getSQLState())) {
                        //No time left for processing of further tables
                        logger.info(phase + ": {} Table '{}' could not be processed anymore. No time left. Processed {} tables : {}", traceItem, tableName, processedCount, e);
                        connection.rollback();
                        break;
                    }
                    else {
                        logger.error(phase + ": {} Failed process table '{}' : {}", traceItem, tableName, e);
                        connection.rollback();
                    }
                }
                finally {
                    _advisory(tableName, connection, false, false);
                    if (cStateFlag)
                        connection.setAutoCommit(true);
                }
            }
            else
                logger.info(phase + ": lock on table" + tableName + " could not be acquired. Continuing with next one.");
        }
        logger.info(phase + ": processed {} tables. Took: {}ms", processedCount, overallDuration);
    }

    private void oneTimeAlterExistingTablesAddNewColumnsAndIndices(Connection connection, String schema, String tableName) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            //Alter existing tables: add new columns
            SQLQuery alterQuery = new SQLQuery("ALTER TABLE ${schema}.${table} "
                + "ADD COLUMN id TEXT, "
                + "ADD COLUMN version BIGINT NOT NULL DEFAULT 0, "
                + "ADD COLUMN next_version BIGINT NOT NULL DEFAULT 9223372036854775807::BIGINT, "
                + "ADD COLUMN operation CHAR NOT NULL DEFAULT 'I'")
                .withVariable("schema", schema)
                .withVariable("table", tableName);
            stmt.addBatch(alterQuery.substitute().text());
            //Add new indices for existing tables
            createVersioningIndices(stmt, schema, tableName);
            //Add new sequence for existing tables
            stmt.addBatch(buildCreateSequenceQuery(schema, tableName, VERSION_SEQUENCE_SUFFIX).substitute().text());

            stmt.setQueryTimeout(calculateTimeout());
            stmt.executeBatch();
            connection.commit();
            logger.info("phase0: {} Successfully altered table and created indices for table '{}'", traceItem, tableName);
        }
    }

    private static void setupOneTimeActionFillNewColumns(String phase, Connection connection) throws SQLException {
        logger.info("oneTimeAction " + phase + ": Setting up PSQL function ...");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE OR REPLACE FUNCTION fill_versioning_fields(schema_name TEXT, space_table_name TEXT, row_limit INTEGER) RETURNS INTEGER AS $$ "
                + "DECLARE "
                + "    operation_column_result INTEGER; "
                + "    update_sql TEXT; "
                + "    updated_rows INTEGER; "
                + "BEGIN "
                + "    operation_column_result := (SELECT 1 WHERE EXISTS (SELECT column_name "
                + "                                                       FROM information_schema.columns "
                + "                                                       WHERE table_name = space_table_name and column_name = 'deleted')); "
                + " "
                + "    update_sql = 'WITH rows_to_update AS (SELECT jsondata->>''id'' as id FROM \"' || schema_name || '\".\"' || space_table_name || '\" WHERE id IS NULL LIMIT ' || row_limit || ') ' || "
                + "                 'UPDATE \"' || schema_name || '\".\"' || space_table_name || '\" t ' || "
                + "                 'SET id = jsondata->>''id'', ' || "
                + "                 'version = (CASE WHEN version > 0 THEN version ELSE (CASE WHEN (jsondata->''properties''->''@ns:com:here:xyz''->''version'')::BIGINT IS NULL THEN 0::BIGINT ELSE (jsondata->''properties''->''@ns:com:here:xyz''->''version'')::BIGINT END) END)'; "
                + " "
                + "    IF operation_column_result = 1 THEN "
                + "        update_sql = update_sql || ', operation = (CASE WHEN deleted = TRUE THEN ''D'' ELSE operation END)'; "
                + "    END IF; "
                + " "
                + "    EXECUTE update_sql || ' FROM rows_to_update WHERE t.jsondata->>''id'' = rows_to_update.id'; "
                + "    GET DIAGNOSTICS updated_rows = ROW_COUNT; "
                + " "
                + "    IF (updated_rows < row_limit) THEN "
                + "        EXECUTE 'COMMENT ON TABLE \"' || schema_name || '\".\"' || space_table_name || '\" IS ''" + OTA_PHASE_1_COMPLETE + "'''; "
                + "    ELSE "
                + "        EXECUTE 'COMMENT ON TABLE \"' || schema_name || '\".\"' || space_table_name || '\" IS ''" + OTA_PHASE_1_STARTED + "'''; "
                + "    END IF; "
                + " "
                + "    return updated_rows; "
                + "END; "
                + "$$ LANGUAGE plpgsql;");
        }
    }

    /**
     * Fill id, version & operation columns
     * @param connection
     * @param schema
     * @param tableName
     * @return
     * @throws SQLException
     */
    private boolean oneTimeFillNewColumns(Connection connection, String schema, String tableName, Map<String, Object> inputData) throws SQLException {
        int limit = (int) inputData.get("rowProcessingLimit");
        //NOTE: If table processing has been fully done a comment "phase1_complete" will be added to the table
        SQLQuery fillNewColumnsQuery = new SQLQuery("SELECT fill_versioning_fields(#{schema}, #{table}, #{limit})")
            .withNamedParameter("schema", schema)
            .withNamedParameter("table", tableName)
            .withNamedParameter("limit", limit);

        final QueryRunner run = new QueryRunner(new StatementConfiguration(null,null,null,null, calculateTimeout()));
        int updatedRows = run.query(connection, fillNewColumnsQuery.substitute().text(), rs -> {
            if (rs.next())
                return rs.getInt(1);
            else
                throw new SQLException("Error while calling function fill_versioning_fields()");
        }, fillNewColumnsQuery.parameters().toArray());

        boolean tableCompleted = updatedRows < limit;
        logger.info("phase1: {} Successfully filled columns for " + updatedRows + " rows "
                + (tableCompleted ? "and set comment '" + OTA_PHASE_1_COMPLETE + "' " : "and set comment '" + OTA_PHASE_1_STARTED + "' ") + "for table '{}'",
            traceItem, tableName);
        return tableCompleted;
    }

    private void oneTimeAddConstraintsToOldTables(Connection connection, String schema, String tableName) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            //Alter existing tables: add new columns
            SQLQuery alterQuery = new SQLQuery("ALTER TABLE ${schema}.${table} "
                + "ADD COLUMN author TEXT, "
                + "ALTER COLUMN version DROP DEFAULT, "
                + "ALTER COLUMN id SET NOT NULL, "
                + "ALTER COLUMN operation DROP DEFAULT, "
                + "ADD CONSTRAINT ${constraintName} PRIMARY KEY (id, version)")
                .withVariable("schema", schema)
                .withVariable("table", tableName)
                .withVariable("constraintName", tableName + "_primKey");
            stmt.addBatch(alterQuery.substitute().text());

            //Add index for new author column
            stmt.addBatch(buildCreateIndexQuery(schema, tableName, "author", "BTREE").substitute().text());

            //Add comment "phaseX_complete"
            SQLQuery setPhaseXCOmment = new SQLQuery("COMMENT ON TABLE ${schema}.${table} IS '" + OTA_PHASE_X_COMPLETE + "'")
                .withVariable("schema", schema)
                .withVariable("table", tableName);
            stmt.addBatch(setPhaseXCOmment.substitute().text());


            stmt.setQueryTimeout(calculateTimeout());
            stmt.executeBatch();
            connection.commit();
            logger.info("phase0: {} Successfully altered table and created indices for table '{}'", traceItem, tableName);
        }
    }

    private void createSpaceStatement(Statement stmt, Event event) throws SQLException {
        String tableFields =
            "id TEXT NOT NULL, "
            + "version BIGINT NOT NULL, "
            + "next_version BIGINT NOT NULL DEFAULT 9223372036854775807::BIGINT, "
            + "operation CHAR NOT NULL, "
            + "author TEXT, "
            + "jsondata JSONB, "
            + "geo geometry(GeometryZ, 4326), "
            + "i BIGSERIAL"
            + ", CONSTRAINT ${constraintName} PRIMARY KEY (id, version)";

        String schema = config.getDatabaseSettings().getSchema();
        String table = config.readTableFromEvent(event);

        SQLQuery q = new SQLQuery("CREATE TABLE IF NOT EXISTS ${schema}.${table} (${{tableFields}})")
            .withQueryFragment("tableFields", tableFields)
            .withVariable("schema", schema)
            .withVariable("table", table)
            .withVariable("constraintName", table + "_primKey");

        stmt.addBatch(q.substitute().text());

        String query;

        createVersioningIndices(stmt, schema, table);

        if (readVersionsToKeep(event) <= 1) {
            query = "CREATE " + (readVersionsToKeep(event) < 1 ? "UNIQUE" : "") + " INDEX IF NOT EXISTS ${idx_id} ON ${schema}.${table} ((jsondata->>'id'))";
            query = SQLQuery.replaceVars(query, replacements, schema, table);
            stmt.addBatch(query);
        }

        query = "CREATE INDEX IF NOT EXISTS ${idx_tags} ON ${schema}.${table} USING gin ((jsondata->'properties'->'@ns:com:here:xyz'->'tags') jsonb_ops)";
        query = SQLQuery.replaceVars(query, replacements, schema, table);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_geo} ON ${schema}.${table} USING gist ((geo))";
        query = SQLQuery.replaceVars(query, replacements, schema, table);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_serial} ON ${schema}.${table}  USING btree ((i))";
        query = SQLQuery.replaceVars(query, replacements, schema, table);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_updatedAt} ON ${schema}.${table} USING btree ((jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt'), (jsondata->>'id'))";
        query = SQLQuery.replaceVars(query, replacements, schema, table);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_createdAt} ON ${schema}.${table} USING btree ((jsondata->'properties'->'@ns:com:here:xyz'->'createdAt'), (jsondata->>'id'))";
        query = SQLQuery.replaceVars(query, replacements, schema, table);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_viz} ON ${schema}.${table} USING btree (left( md5(''||i),5))";
        query = SQLQuery.replaceVars(query, replacements, schema, table);
        stmt.addBatch(query);

        stmt.addBatch(buildCreateSequenceQuery(schema, table, VERSION_SEQUENCE_SUFFIX).substitute().text());

        stmt.setQueryTimeout(calculateTimeout());
    }

    private static SQLQuery buildCreateSequenceQuery(String schema, String table, String sequenceNameSuffix) {
        return new SQLQuery("CREATE SEQUENCE IF NOT EXISTS ${schema}.${sequence} MINVALUE 0")
            .withVariable("schema", schema)
            .withVariable("sequence", table + sequenceNameSuffix);
    }

    private static void createVersioningIndices(Statement stmt, String schema, String table) throws SQLException {
        stmt.addBatch(buildCreateIndexQuery(schema, table, "id", "BTREE", "idx_" + table + "_idnew").substitute().text());
        stmt.addBatch(buildCreateIndexQuery(schema, table, "version", "BTREE").substitute().text());
        stmt.addBatch(buildCreateIndexQuery(schema, table, Arrays.asList("id", "version"), "BTREE").substitute().text());
        stmt.addBatch(buildCreateIndexQuery(schema, table, Arrays.asList("id", "version", "next_version"), "BTREE").substitute().text());
        stmt.addBatch(buildCreateIndexQuery(schema, table, "operation", "BTREE").substitute().text());
    }

    static SQLQuery buildCreateIndexQuery(String schema, String table, String columnName, String method) {
      return buildCreateIndexQuery(schema, table, Collections.singletonList(columnName), method);
    }

    static SQLQuery buildCreateIndexQuery(String schema, String table, List<String> columnNames, String method) {
        return buildCreateIndexQuery(schema, table, columnNames, method, "idx_" + table + "_"
            + columnNames.stream().map(colName -> colName.replace("_", "")).collect(Collectors.joining()));
    }

    static SQLQuery buildCreateIndexQuery(String schema, String table, String columnName, String method, String indexName) {
        return buildCreateIndexQuery(schema, table, Arrays.asList(columnName), method, indexName);
    }

    private static SQLQuery buildCreateIndexQuery(String schema, String table, List<String> columnNamesOrExpressions, String method,
        String indexName) {
        return new SQLQuery("CREATE INDEX IF NOT EXISTS ${indexName} ON ${schema}.${table} USING " + method
            + " (" + String.join(", ", columnNamesOrExpressions) + ")")
            .withVariable("schema", schema)
            .withVariable("table", table)
            .withVariable("indexName", indexName);
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
                    createSpaceStatement(stmt, event);

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
                    createSpaceStatement(stmt, event);

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

    protected void setReplicaIdentity() throws SQLException {
        final String tableName = config.readTableFromEvent(event);

        try (final Connection connection = dataSource.getConnection()) {
            advisoryLock( tableName, connection );
            boolean cStateFlag = connection.getAutoCommit();
            try {
                if (cStateFlag)
                    connection.setAutoCommit(false);

                String infoSql = SQLQueryBuilder.getReplicaIdentity(config.getDatabaseSettings().getSchema(), tableName),
                       setReplIdSql = SQLQueryBuilder.setReplicaIdentity(config.getDatabaseSettings().getSchema(), tableName);

                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery(infoSql); )
                {
                    if( !rs.next() )
                    { createSpaceStatement(stmt, event); /** Create Space-Table */
                      stmt.addBatch(setReplIdSql);
                    }
                    else if(! "f".equals(rs.getString(1) ) ) /** Table exists, but wrong replic identity */
                     stmt.addBatch(setReplIdSql);
                    else
                     return; /** Table exists with propper replic identity */

                    stmt.setQueryTimeout(calculateTimeout());
                    stmt.executeBatch();
                    connection.commit();
                }
            } catch (Exception e) {
                throw new SQLException("set replica identity to full failed: "+tableName, e);
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

    protected FeatureCollection _defaultFeatureResultSetHandler(ResultSet rs, boolean skipNullGeom) throws SQLException {
        String nextIOffset = "";
        String nextDataset = null;

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

            if (event instanceof IterateFeaturesEvent) {
                numFeatures++;
                nextIOffset = rs.getString(3);
                if (rs.getMetaData().getColumnCount() >= 4)
                    nextDataset = rs.getString(4);
            }
        }

        if (sb.length() > prefix.length()) {
            sb.setLength(sb.length() - 1);
        }
        sb.append("]");

        final FeatureCollection featureCollection = new FeatureCollection();
        featureCollection._setFeatures(sb.toString());

        if (sb.length() > MAX_RESULT_CHARS) throw new SQLException(DhString.format("Maxchar limit(%d) reached", MAX_RESULT_CHARS));

        if (event instanceof IterateFeaturesEvent && numFeatures > 0 && numFeatures == ((SearchForFeaturesEvent) event).getLimit() ) {
          String nextHandle = (nextDataset != null ? nextDataset + "_" : "") + nextIOffset;
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
