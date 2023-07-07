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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.models.hub.Space.DEFAULT_VERSIONS_TO_KEEP;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.DELETE;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.INSERT;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.UPDATE;
import static com.here.xyz.psql.QueryRunner.SCHEMA;
import static com.here.xyz.psql.QueryRunner.TABLE;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.connectors.StorageConnector;
import com.here.xyz.connectors.runtime.ConnectorRuntime;
import com.here.xyz.events.Event;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySpaceEvent.Operation;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.psql.config.ConnectorParameters;
import com.here.xyz.psql.config.DatabaseSettings;
import com.here.xyz.psql.config.PSQLConfig;
import com.here.xyz.psql.query.ExtendedSpace;
import com.here.xyz.psql.query.ModifySpace;
import com.here.xyz.psql.query.XyzEventBasedQueryRunner;
import com.here.xyz.psql.query.helpers.FetchExistingFeatures;
import com.here.xyz.psql.query.helpers.FetchExistingFeatures.FetchExistingFeaturesInput;
import com.here.xyz.psql.query.helpers.FetchExistingIds;
import com.here.xyz.psql.query.helpers.FetchExistingIds.FetchIdsInput;
import com.here.xyz.psql.query.helpers.TableExists;
import com.here.xyz.psql.query.helpers.TableExists.Table;
import com.here.xyz.psql.query.helpers.versioning.GetNextVersion;
import com.here.xyz.psql.tools.DhString;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.mchange.v2.c3p0.AbstractConnectionCustomizer;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.PooledDataSource;
import java.sql.BatchUpdateException;
import java.sql.Connection;
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
    private static final String C3P0EXT_CONFIG_SCHEMA = "config.schema()";

    /**
     * Lambda Execution Time = 25s. We are actively canceling queries after STATEMENT_TIMEOUT_SECONDS
     * So if we receive a timeout prior 25s-STATEMENT_TIMEOUT_SECONDS the cancellation comes from
     * outside.
     **/
    private static final int MIN_REMAINING_TIME_FOR_RETRY_SECONDS = 3;
    public static final String HEAD_TABLE_SUFFIX = "_head";

    private static String INCLUDE_OLD_STATES = "includeOldStates"; // read from event params

    public static final long PARTITION_SIZE = 100_000;

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
    protected static PSQLConfig config;
    /**
     * The write data source for the current event.
     */
    protected DataSource dataSource;
    /**
     * The read data source for the current event.
     */
    protected DataSource readDataSource;
    /**
     * The dbMaintainer for the current event.
     */
    protected DatabaseMaintainer dbMaintainer;

    private boolean retryAttempted;

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
        config = new PSQLConfig(event, context);
        String connectorId = traceItem.getConnectorId();

        if (connectorId == null) {
            logger.warn("{} ConnectorId is missing as param in the Connector-Config! {} / {}@{}", traceItem, config.getDatabaseSettings().getDb(), config.getDatabaseSettings().getUser(), config.getDatabaseSettings().getHost());
            connectorId =config.getConfigValuesAsString();
        }

        if (dbInstanceMap.get(connectorId) != null){
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
            config.addDataSource(source);

            if (config.getDatabaseSettings().getReplicaHost() != null) {
                final ComboPooledDataSource replicaDataSource = getComboPooledDataSource(config.getDatabaseSettings(), config.getConnectorParams(),  config.applicationName() , true);
                replicaDataSource.setExtensions(m);
                config.addReadDataSource(replicaDataSource);
            }
            dbInstanceMap.put(connectorId, config);
        }

        retryAttempted = false;
        dataSource = dbInstanceMap.get(connectorId).getDataSource();
        readDataSource = dbInstanceMap.get(connectorId).getReadDataSource();
        dbMaintainer = new DatabaseMaintainer(dataSource, config);
        //Always use the writer in case of event.preferPrimaryDataSource == true
        if (event.getPreferPrimaryDataSource() != null && event.getPreferPrimaryDataSource() == Boolean.TRUE)
            this.readDataSource = this.dataSource;
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

    private static ComboPooledDataSource getComboPooledDataSource(DatabaseSettings dbSettings, ConnectorParameters connectorParameters, String applicationName, boolean useReplica) {
        String jdbcUrl = DhString.format("jdbc:postgresql://%1$s:%2$d/%3$s?ApplicationName=%4$s&tcpKeepAlive=true",
            useReplica ? dbSettings.getReplicaHost() : dbSettings.getHost(), dbSettings.getPort(), dbSettings.getDb(), applicationName);
        ComboPooledDataSource pooledDataSource = getComboPooledDataSource(jdbcUrl, dbSettings.getUser(), dbSettings.getPassword(), connectorParameters.getDbMinPoolSize(),
            connectorParameters.getDbMaxPoolSize(), connectorParameters.getDbInitialPoolSize(),
            connectorParameters.getDbAcquireRetryAttempts(), connectorParameters.getDbAcquireIncrement(),
            connectorParameters.getDbCheckoutTimeout() * 1000,
            connectorParameters.getDbMaxIdleTime() != null ? connectorParameters.getDbMaxIdleTime() : 0,
            connectorParameters.isDbTestConnectionOnCheckout());

        pooledDataSource.setConnectionCustomizerClassName(DatabaseHandler.XyzConnectionCustomizer.class.getName());

        return pooledDataSource;
    }

    private static ComboPooledDataSource getComboPooledDataSource(String jdbcUrl, String user, String password,
        int minPoolSize, int maxPoolSize, int initialPoolSize, int acquireRetryAttempts, int acquireIncrement, int checkoutTimeout,
        int maxIdleTime, boolean testConnectionOnCheckout) {

        final ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setJdbcUrl(jdbcUrl);
        cpds.setUser(user);
        cpds.setPassword(password);
        cpds.setInitialPoolSize(initialPoolSize);
        cpds.setMinPoolSize(minPoolSize);
        cpds.setMaxPoolSize(maxPoolSize);
        cpds.setAcquireRetryAttempts(acquireRetryAttempts);
        cpds.setAcquireIncrement(acquireIncrement);
        cpds.setCheckoutTimeout(checkoutTimeout);
        cpds.setMaxIdleTime(maxIdleTime);
        cpds.setTestConnectionOnCheckout(testConnectionOnCheckout);

        return cpds;
    }

    //TODO: Move into QueryRunner top level class
    public <T> T executeQueryWithRetry(SQLQuery query, ResultSetHandler<T> handler, DataSource dataSource) throws SQLException {
        try {
            return executeQuery(query, handler, dataSource);
        }
        catch (Exception e) {
            try {
                if (retryCausedOnServerlessDB(e) || canRetryAttempt()) {
                    logger.info("{} Retry Query permitted.", traceItem);
                    return executeQuery(query, handler, dataSource);
                }
            }
            catch (Exception e1) {
                if(retryCausedOnServerlessDB(e1)) {
                    logger.info("{} Retry Query permitted.", traceItem);
                    return executeQuery(query, handler, dataSource);
                }
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
            final QueryRunner run = new QueryRunner(dataSource, new StatementConfiguration(null,null,null,null,calculateTimeout()));
            final String queryText = SQLQuery.replaceVars(query.text(), config.getDatabaseSettings().getSchema(), XyzEventBasedQueryRunner.readTableFromEvent(event));
            final List<Object> queryParameters = query.parameters();
            logger.debug("{} executeQuery: {} - Parameter: {}", traceItem, queryText, queryParameters);
            return run.query(queryText, handler, queryParameters.toArray());
        }
        finally {
            final long end = System.currentTimeMillis();
            final String dataSourceURL = dataSource instanceof ComboPooledDataSource ? ((ComboPooledDataSource) dataSource).getJdbcUrl() : "n/a";
            logger.info("{} query time: {}ms, event: {}, dataSource: {}", traceItem, (end - start), event.getClass().getSimpleName(), dataSourceURL);
        }
    }

    protected int executeUpdateWithRetry(SQLQuery query, DataSource dataSource) throws SQLException {
        try {
            return executeUpdate(query, dataSource);
        } catch (Exception e) {
            try {
                if (retryCausedOnServerlessDB(e) || canRetryAttempt()) {
                    logger.info("{} Retry Update permitted.", ConnectorRuntime.getInstance().getStreamId());
                    return executeUpdate(query, dataSource);
                }
            } catch (Exception e1) {
                if (retryCausedOnServerlessDB(e)) {
                    logger.info("{} Retry Update permitted.", ConnectorRuntime.getInstance().getStreamId());
                    return executeUpdate(query, dataSource);
                }
                throw e;
            }
            throw e;
        }
    }

    /**
     * Executes the given update or delete query and returns the number of deleted or updated records.
     *
     * @param query the update or delete query.
     * @return the amount of updated or deleted records.
     * @throws SQLException if any error occurred.
     */
    private int executeUpdate(SQLQuery query, DataSource dataSource) throws SQLException {
        final long start = System.currentTimeMillis();
        try {
            final QueryRunner run = new QueryRunner(dataSource, new StatementConfiguration(null,null,null,null,calculateTimeout()));
            final String queryText = SQLQuery.replaceVars(query.text(), config.getDatabaseSettings().getSchema(), XyzEventBasedQueryRunner.readTableFromEvent(event));
            final List<Object> queryParameters = query.parameters();
            logger.debug("{} executeUpdate: {} - Parameter: {}", ConnectorRuntime.getInstance().getStreamId(), queryText, queryParameters);
            return run.update(queryText, queryParameters.toArray());
        } finally {
            final long end = System.currentTimeMillis();
            logger.info("{} query time: {}ms", ConnectorRuntime.getInstance().getStreamId(), (end - start));
        }
    }

    private boolean retryCausedOnServerlessDB(Exception e) {
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
            int remainingSeconds = ConnectorRuntime.getInstance().getRemainingTime() / 1000;

            if(!isRemainingTimeSufficient(remainingSeconds)){
                return false;
            }
            if (!retryAttempted) {
                logger.warn("{} Retry based on serverless scaling detected! RemainingTime: {} ", traceItem, remainingSeconds, e);
                return true;
            }
        }
        return false;
    }

    protected XyzResponse executeModifySpace(ModifySpaceEvent event) throws SQLException, ErrorResponseException {
        if (event.getSpaceDefinition() != null && event.getOperation() == Operation.CREATE) {
            //Create Space Table
            ensureSpace();
        }

        new ModifySpace(event).write();
        if (event.getOperation() != Operation.DELETE)
            dbMaintainer.maintainSpace(traceItem, config.getDatabaseSettings().getSchema(), XyzEventBasedQueryRunner.readTableFromEvent(event));

        //If we reach this point we are okay!
        return new SuccessResponse().withStatus("OK");
    }

    protected XyzResponse executeModifyFeatures(ModifyFeaturesEvent event) throws Exception {
        final boolean includeOldStates = event.getParams() != null && event.getParams().get(INCLUDE_OLD_STATES) == Boolean.TRUE;

        final FeatureCollection collection = new FeatureCollection();
        collection.setFeatures(new ArrayList<>());

        List<Feature> inserts = Optional.ofNullable(event.getInsertFeatures()).orElse(new ArrayList<>());
        List<Feature> updates = Optional.ofNullable(event.getUpdateFeatures()).orElse(new ArrayList<>());
        List<Feature> upserts = Optional.ofNullable(event.getUpsertFeatures()).orElse(new ArrayList<>());

        Map<String, String> deletes = Optional.ofNullable(event.getDeleteFeatures()).orElse(new HashMap<>());
        List<FeatureCollection.ModificationFailure> fails = Optional.ofNullable(event.getFailed()).orElse(new ArrayList<>());

        List<String> originalUpdates = updates.stream().map(Feature::getId).collect(Collectors.toList());
        List<String> originalDeletes = new ArrayList<>(deletes.keySet());
        //Handle deletes / updates on extended spaces
        if (isForExtendingSpace(event) && event.getContext() == DEFAULT) {
            if (!deletes.isEmpty()) {
                //Transform the incoming deletes into upserts with deleted flag for features which exist in the extended layer (base)
                List<String> existingIdsInBase = new FetchExistingIds(
                    new FetchIdsInput(ExtendedSpace.getExtendedTable(event), originalDeletes)).run();

                for (String featureId : originalDeletes) {
                  if (existingIdsInBase.contains(featureId)) {
                    Feature toDelete = new Feature()
                        .withId(featureId)
                        .withProperties(new Properties().withXyzNamespace(new XyzNamespace().withDeleted(true)));

                    try {
                      toDelete.getProperties().getXyzNamespace().setVersion(Long.parseLong(deletes.get(featureId)));
                    }
                    catch (Exception ignore) {}

                    upserts.add(toDelete);
                    deletes.remove(featureId);
                  }
                }
            }

            //TODO: The following is a workaround for a bug in the implementation LFE (in GetFeatures QR) that it filters out operations "H" / "J" which it should not in that case
            if (!inserts.isEmpty() && readVersionsToKeep(event) == 1) {
              //Transform the incoming inserts into upserts. That way on the creation of the query we make sure to always check again whether some object exists already for the ID
              upserts.addAll(inserts);
              inserts.clear();
            }

            if (!updates.isEmpty()) {
                //Transform the incoming updates into upserts, because we don't know whether the object is existing in the extension already
                upserts.addAll(updates);
                updates.clear();
            }
        }

        long version = -1;
        try {
          /** Include Old states */
          if (includeOldStates) {
            List<String> idsToFetch = getAllIds(inserts, updates, upserts, deletes);
            List<Feature> existingFeatures = new FetchExistingFeatures(new FetchExistingFeaturesInput(event, idsToFetch)).run();
            if (existingFeatures != null) {
              collection.setOldFeatures(existingFeatures);
            }
          }

          /** Include Upserts */
          if (!upserts.isEmpty()) {
            List<String> upsertIds = upserts.stream().map(Feature::getId).filter(Objects::nonNull).collect(Collectors.toList());
            List<String> existingIds = new FetchExistingIds(new FetchIdsInput(XyzEventBasedQueryRunner.readTableFromEvent(event),
                upsertIds)).run();
            upserts.forEach(f -> (existingIds.contains(f.getId()) ? updates : inserts).add(f));
          }

          version = new GetNextVersion<>(event).run();
        }
        catch (Exception e) {
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

                    if (e instanceof SQLException && ((SQLException)e).getSQLState() != null
                            && ((SQLException)e).getSQLState().equalsIgnoreCase("42P01"))
                        ;//Table does not exist yet - create it!
                    else {

                        logger.warn("{} Transaction has failed. ", traceItem, e);
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

            // set the version in the returning elements
            if (event.getVersionsToKeep() > 1) {
              for (Feature f : collection.getFeatures()) {
                f.getProperties().getXyzNamespace().setVersion(version);
              }
            }

            connection.close();

            return collection;
        }
    }

    private List<String> getAllIds(List<Feature> inserts, List<Feature> updates, List<Feature> upserts, Map<String, ?> deletes) {
      List<String> ids = Stream.concat(
              Stream
                  .of(inserts, updates, upserts)
                  .flatMap(Collection::stream)
                  .map(Feature::getId),
              deletes.keySet().stream()
          )
          .filter(Objects::nonNull)
          .collect(Collectors.toList());

      return ids;
    }

    private boolean canRetryAttempt() {
        if (retryAttempted || !isRemainingTimeSufficient(ConnectorRuntime.getInstance().getRemainingTime() / 1000))
            return false;
        retryAttempted = true;
//        logger.info("{} Retry the execution.", traceItem);
        return true;
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

    private static boolean isForExtendingSpace(Event event) {
        return event.getParams() != null && event.getParams().containsKey("extends");
    }

    //TODO: Move the following into ModifySpace QR
    private void ensureSpace() throws SQLException, ErrorResponseException {
        // Note: We can assume that when the table exists, the postgis extensions are installed.
        final String tableName = XyzEventBasedQueryRunner.readTableFromEvent(event);

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
                //Check if the table was created in the meantime, by another instance.
                boolean tableExists = new TableExists(new Table(config.getDatabaseSettings().getSchema(),
                    XyzEventBasedQueryRunner.readTableFromEvent(event))).run();
                if (tableExists)
                    return;
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

    private void alterColumnStorage(Statement stmt, String schema, String tableName) throws SQLException {
        stmt.addBatch(new SQLQuery("ALTER TABLE ${schema}.${table} "
            + "ALTER COLUMN id SET STORAGE MAIN, "
            + "ALTER COLUMN jsondata SET STORAGE MAIN, "
            + "ALTER COLUMN geo SET STORAGE MAIN, "
            + "ALTER COLUMN operation SET STORAGE PLAIN, "
            + "ALTER COLUMN next_version SET STORAGE PLAIN, "
            + "ALTER COLUMN version SET STORAGE PLAIN, "
            + "ALTER COLUMN i SET STORAGE PLAIN, "
            + "ALTER COLUMN author SET STORAGE MAIN, "

            + "ALTER COLUMN id SET COMPRESSION lz4, "
            + "ALTER COLUMN jsondata SET COMPRESSION lz4, "
            + "ALTER COLUMN geo SET COMPRESSION lz4, "
            + "ALTER COLUMN author SET COMPRESSION lz4;")
            .withVariable("schema", schema)
            .withVariable("table", tableName)
            .substitute()
            .text());
    }

    private void createHeadPartition(Statement stmt, String schema, String rootTable) throws SQLException {
        SQLQuery q = new SQLQuery("CREATE TABLE IF NOT EXISTS ${schema}.${partitionTable} "
            + "PARTITION OF ${schema}.${rootTable} FOR VALUES FROM (max_bigint()) TO (MAXVALUE)")
            .withVariable(SCHEMA, schema)
            .withVariable("rootTable", rootTable)
            .withVariable("partitionTable", rootTable + HEAD_TABLE_SUFFIX);

        stmt.addBatch(q.substitute().text());
    }

    private void createHistoryPartition(Statement stmt, String schema, String rootTable, long partitionNo) throws SQLException {
        SQLQuery q = new SQLQuery("SELECT xyz_create_history_partition('" + schema + "', '" + rootTable + "', " + partitionNo + ", " + PARTITION_SIZE + ")");
        stmt.addBatch(q.substitute().text());
    }

    private void createSpaceTableStatement(Statement stmt, String schema, String table, boolean withIndices, String existingSerial) throws SQLException {
        String tableFields = "id TEXT NOT NULL, "
                + "version BIGINT NOT NULL, "
                + "next_version BIGINT NOT NULL DEFAULT 9223372036854775807::BIGINT, "
                + "operation CHAR NOT NULL, "
                + "author TEXT, "
                + "jsondata JSONB, "
                + "geo geometry(GeometryZ, 4326), "
                + "i " + (existingSerial == null ? "BIGSERIAL" : "BIGINT NOT NULL DEFAULT nextval('${schema}.${existingSerial}')")
                + ", CONSTRAINT ${constraintName} PRIMARY KEY (id, version, next_version)";

        SQLQuery createTable = new SQLQuery("CREATE TABLE IF NOT EXISTS ${schema}.${table} (${{tableFields}}) PARTITION BY RANGE (next_version)")
            .withQueryFragment("tableFields", tableFields)
            .withVariable("schema", schema)
            .withVariable("table", table)
            .withVariable("constraintName", table + "_primKey");

        if (existingSerial != null)
            createTable.setVariable("existingSerial", existingSerial);

        stmt.addBatch(createTable.substitute().text());

        //Add new way of using different storage-type for columns
        alterColumnStorage(stmt, schema, table);

        String query;

        if (withIndices) {
            createVersioningIndices(stmt, schema, table);

            query = "CREATE INDEX IF NOT EXISTS ${indexName} ON ${schema}.${table} USING gin ((jsondata->'properties'->'@ns:com:here:xyz'->'tags') jsonb_ops)";
            doReplacementsAndAdd(stmt, new SQLQuery(query).withVariable("indexName", "idx_" + table + "_tags"), schema, table);

            query = "CREATE INDEX IF NOT EXISTS ${indexName} ON ${schema}.${table} USING gist ((geo))";
            doReplacementsAndAdd(stmt, new SQLQuery(query).withVariable("indexName", "idx_" + table + "_geo"), schema, table);

            query = "CREATE INDEX IF NOT EXISTS ${indexName} ON ${schema}.${table} USING btree ((i))";
            doReplacementsAndAdd(stmt, new SQLQuery(query).withVariable("indexName", "idx_" + table + "_serial"), schema, table);

            query = "CREATE INDEX IF NOT EXISTS ${indexName} ON ${schema}.${table} USING btree ((jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt'), id)";
            doReplacementsAndAdd(stmt, new SQLQuery(query).withVariable("indexName", "idx_" + table + "_updatedAt"), schema, table);

            query = "CREATE INDEX IF NOT EXISTS ${indexName} ON ${schema}.${table} USING btree ((jsondata->'properties'->'@ns:com:here:xyz'->'createdAt'), id)";
            doReplacementsAndAdd(stmt, new SQLQuery(query).withVariable("indexName", "idx_" + table + "_createdAt"), schema, table);

            query = "CREATE INDEX IF NOT EXISTS ${indexName} ON ${schema}.${table} USING btree (left(md5('' || i), 5))";
            doReplacementsAndAdd(stmt, new SQLQuery(query).withVariable("indexName", "idx_" + table + "_viz"), schema, table);
        }
    }

    private void doReplacementsAndAdd(Statement stmt, SQLQuery q, String schema, String table) throws SQLException {
        stmt.addBatch(q.withVariable(SCHEMA, schema).withVariable(TABLE, table).substitute().text());
    }

    private void createSpaceStatement(Statement stmt, Event event) throws SQLException {
        String schema = config.getDatabaseSettings().getSchema();
        String table = XyzEventBasedQueryRunner.readTableFromEvent(event);

        createSpaceTableStatement(stmt, schema, table, true, null);
        createHeadPartition(stmt, schema, table);
        createHistoryPartition(stmt, schema, table, 0L);

        stmt.addBatch(buildCreateSequenceQuery(schema, table, "version").substitute().text());

        stmt.setQueryTimeout(calculateTimeout());
    }

    private static SQLQuery buildCreateSequenceQuery(String schema, String table, String columnName) {
        return new SQLQuery("CREATE SEQUENCE IF NOT EXISTS ${schema}.${sequence} MINVALUE 0 OWNED BY ${schema}.${table}.${columnName}")
            .withVariable("schema", schema)
            .withVariable("table", table)
            .withVariable("sequence", table + "_" + columnName + "_seq")
            .withVariable("columnName", columnName);
    }

    private static void createVersioningIndices(Statement stmt, String schema, String table) throws SQLException {
        stmt.addBatch(buildCreateIndexQuery(schema, table, "id", "BTREE", "idx_" + table + "_idnew").substitute().text());
        stmt.addBatch(buildCreateIndexQuery(schema, table, "version", "BTREE").substitute().text());
        stmt.addBatch(buildCreateIndexQuery(schema, table, "next_version", "BTREE").substitute().text());
        stmt.addBatch(buildCreateIndexQuery(schema, table, Arrays.asList("id", "version"), "BTREE").substitute().text());
        stmt.addBatch(buildCreateIndexQuery(schema, table, "operation", "BTREE").substitute().text());
        stmt.addBatch(buildCreateIndexQuery(schema, table, "author", "BTREE").substitute().text());
    }

    static SQLQuery buildCreateIndexQuery(String schema, String table, String columnName, String method) {
      return buildCreateIndexQuery(schema, table, Collections.singletonList(columnName), method);
    }

    static SQLQuery buildCreateIndexQuery(String schema, String table, String columnName, String method, boolean withNulls) {
        return buildCreateIndexQuery(schema, table, Collections.singletonList(columnName), method, withNulls ? null : columnName + " IS NOT NULL");
    }

    static SQLQuery buildCreateIndexQuery(String schema, String table, List<String> columnNames, String method) {
        return buildCreateIndexQuery(schema, table, columnNames, method, null);
    }

    private static SQLQuery buildCreateIndexQuery(String schema, String table, List<String> columnNames, String method, String predicate) {
        return buildCreateIndexQuery(schema, table, columnNames, method, "idx_" + table + "_"
            + columnNames.stream().map(colName -> colName.replace("_", "")).collect(Collectors.joining()), predicate);
    }

    static SQLQuery buildCreateIndexQuery(String schema, String table, String columnName, String method, String indexName) {
        return buildCreateIndexQuery(schema, table, Arrays.asList(columnName), method, indexName, null);
    }

    private static SQLQuery buildCreateIndexQuery(String schema, String table, List<String> columnNamesOrExpressions, String method,
        String indexName, String predicate) {
        return new SQLQuery("CREATE INDEX IF NOT EXISTS ${indexName} ON ${schema}.${table} USING " + method
            + " (" + String.join(", ", columnNamesOrExpressions) + ") ${{predicate}}")
            .withVariable("schema", schema)
            .withVariable("table", table)
            .withVariable("indexName", indexName)
            .withQueryFragment("predicate", predicate != null ? "WHERE " + predicate : "");
    }

/** #################################### Resultset Handlers #################################### */
    /**
     * The default handler for the most results.
     *
     * @param rs the result set.
     * @return the generated feature collection from the result set.
     * @throws SQLException when any unexpected error happened.
     */

    public static final long MAX_RESULT_CHARS = 100 * 1024 *1024;

    private FeatureCollection _defaultFeatureResultSetHandler(ResultSet rs, boolean skipNullGeom, boolean useColumnNames) throws SQLException {
        String nextIOffset = "";
        String nextDataset = null;

        StringBuilder sb = new StringBuilder();
        String prefix = "[";
        sb.append(prefix);
        int numFeatures = 0;

        while (rs.next() && MAX_RESULT_CHARS > sb.length()) {
            String geom = getGeoFromResultSet(rs, useColumnNames);
            if (skipNullGeom && geom == null)
                continue;
            sb.append(getJsondataFromResultSet(rs, useColumnNames));
            sb.setLength(sb.length() - 1);
            sb.append(",\"geometry\":");
            sb.append(geom == null ? "null" : geom);
            sb.append("}");
            sb.append(",");

            if (event instanceof IterateFeaturesEvent) {
                numFeatures++;
                nextIOffset = rs.getString(3);
                if (rs.getMetaData().getColumnCount() >= 5)
                    nextDataset = rs.getString("dataset");
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

    private String getGeoFromResultSet(ResultSet rs, boolean useColumnName) throws SQLException {
        return useColumnName ? rs.getString("geo") : rs.getString(2);
    }

    private String getJsondataFromResultSet(ResultSet rs, boolean useColumnName) throws SQLException {
        return useColumnName ? rs.getString("jsondata") : rs.getString(1);
    }

    /**
     * @deprecated Please solely use an instance of {@link QueryRunner} from now on, to run db tasks.
     * The QR will also care about handling the specific db response.
     */
    @Deprecated
    public FeatureCollection legacyDefaultFeatureResultSetHandler(ResultSet rs) throws SQLException
    { return _defaultFeatureResultSetHandler(rs,false, false); }

    public FeatureCollection defaultFeatureResultSetHandler(ResultSet rs) throws SQLException
    { return _defaultFeatureResultSetHandler(rs,false, true); }

    public FeatureCollection defaultFeatureResultSetHandlerSkipIfGeomIsNull(ResultSet rs) throws SQLException
    { return _defaultFeatureResultSetHandler(rs,true, true); }

    protected int calculateTimeout() throws SQLException{
        int remainingSeconds = ConnectorRuntime.getInstance().getRemainingTime() / 1000;

        if(!isRemainingTimeSufficient(remainingSeconds)) {
            throw new SQLException("No time left to execute query.","54000");
        }

        int timeout = remainingSeconds - 2;

        logger.debug("{} New timeout for query set to '{}'", traceItem, timeout);
        return timeout;
    }

    private boolean isRemainingTimeSufficient(int remainingSeconds){
        if(remainingSeconds <= MIN_REMAINING_TIME_FOR_RETRY_SECONDS) {
            logger.warn("{} No time left to execute query '{}' s", traceItem, remainingSeconds);
            return false;
        }
        return true;
    }

    public PSQLConfig getConfig() {
        return config;
    }

    public static void setConfig(PSQLConfig config) {
        DatabaseHandler.config = config;
    }

    private static int getStatementTimeoutSeconds() {
        return config.getConnectorParams().getStatementTimeoutSeconds();
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
                runner.execute(c, "SET statement_timeout = " + (getStatementTimeoutSeconds() * 1000) + " ;");
                runner.execute(c, "SET search_path=" + schema + ",h3,public,topology;");
            } catch (SQLException e) {
                logger.error("Failed to initialize connection " + c + " [" + pdsIdt + "] : {}", e);
            }
        }
    }
}
