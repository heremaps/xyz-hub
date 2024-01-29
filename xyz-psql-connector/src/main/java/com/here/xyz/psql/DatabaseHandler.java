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
import static com.here.xyz.events.ModifySpaceEvent.Operation.CREATE;
import static com.here.xyz.models.hub.Space.DEFAULT_VERSIONS_TO_KEEP;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.DELETE;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.INSERT;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.UPDATE;
import static com.here.xyz.psql.QueryRunner.SCHEMA;
import static com.here.xyz.psql.QueryRunner.TABLE;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildSpaceTableIndexQueries;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.connectors.StorageConnector;
import com.here.xyz.connectors.runtime.ConnectorRuntime;
import com.here.xyz.events.Event;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.psql.config.ConnectorParameters;
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
import com.here.xyz.psql.tools.ECPSTool;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.db.DatabaseSettings;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.CachedPooledDataSources;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class DatabaseHandler extends StorageConnector {

    public static final String ECPS_PHRASE = "ECPS_PHRASE";
    private static final Logger logger = LogManager.getLogger();
    private static final String MAINTENANCE_ENDPOINT = "MAINTENANCE_SERVICE_ENDPOINT";

    public static final String HEAD_TABLE_SUFFIX = "_head";
    /**
     * Lambda Execution Time = 25s. We are actively canceling queries after STATEMENT_TIMEOUT_SECONDS
     * So if we receive a timeout prior 25s-STATEMENT_TIMEOUT_SECONDS the cancellation comes from
     * outside.
     **/
    static final int MIN_REMAINING_TIME_FOR_RETRY_SECONDS = 3;

    private static String INCLUDE_OLD_STATES = "includeOldStates"; // read from event params

    public static final long PARTITION_SIZE = 100_000;

    /**
     * Current event.
     */
    private Event event;

    /**
     * The dbMaintainer for the current event.
     */
    public DatabaseMaintainer dbMaintainer;

    private boolean retryAttempted;

    void reset() {
        //Clear the data sources cache
        CachedPooledDataSources.invalidateCache();
    }

    protected DataSourceProvider dataSourceProvider;
    protected DatabaseSettings dbSettings;

    @Override
    protected void initialize(Event event) {
        this.event = event;
        String connectorId = traceItem.getConnectorId();
        ConnectorParameters connectorParams = ConnectorParameters.fromEvent(event);

        //Decrypt the ECPS into an instance of DatabaseSettings
        dbSettings = new DatabaseSettings(connectorId,
            ECPSTool.decryptToMap(ConnectorRuntime.getInstance().getEnvironmentVariable(ECPS_PHRASE), connectorParams.getEcps()));

        //Set some additional DB settings
        dbSettings
            .withApplicationName(ConnectorRuntime.getInstance().getApplicationName());

        dataSourceProvider = new CachedPooledDataSources(dbSettings);
        retryAttempted = false;
        dbMaintainer = new DatabaseMaintainer(dataSourceProvider, dbSettings, connectorParams,
            ConnectorRuntime.getInstance().getEnvironmentVariable(MAINTENANCE_ENDPOINT));
        DataSourceProvider.setDefaultProvider(dataSourceProvider);
    }

    protected <R, T extends com.here.xyz.psql.QueryRunner<?, R>> R run(T runner) throws SQLException, ErrorResponseException {
        return runner.withDataSourceProvider(dataSourceProvider).run();
    }

    protected <R, T extends com.here.xyz.psql.QueryRunner<?, R>> R write(T runner) throws SQLException, ErrorResponseException {
        return runner.withDataSourceProvider(dataSourceProvider).write();
    }

    /**
     * @deprecated No retries are necessary anymore as table creation is not done lazily anymore
     */
    @Deprecated
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
        if (event.getSpaceDefinition() != null && event.getOperation() == CREATE)
            //Create Space Table
            ensureSpace();

        return write(new ModifySpace(event).withDbMaintainer(dbMaintainer));
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
                List<String> existingIdsInBase = run(new FetchExistingIds(
                    new FetchIdsInput(ExtendedSpace.getExtendedTable(event), originalDeletes)));

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

        long version;
        try {
          /** Include Old states */
          if (includeOldStates) {
            List<String> idsToFetch = getAllIds(inserts, updates, upserts, deletes);
            List<Feature> existingFeatures = run(new FetchExistingFeatures(new FetchExistingFeaturesInput(event, idsToFetch)));
            if (existingFeatures != null) {
              collection.setOldFeatures(existingFeatures);
            }
          }

          /** Include Upserts */
          if (!upserts.isEmpty()) {
            List<String> upsertIds = upserts.stream().map(Feature::getId).filter(Objects::nonNull).collect(Collectors.toList());
            List<String> existingIds = run(new FetchExistingIds(new FetchIdsInput(XyzEventBasedQueryRunner.readTableFromEvent(event),
                upsertIds)));
            upserts.forEach(f -> (existingIds.contains(f.getId()) ? updates : inserts).add(f));
          }

          version = run(new GetNextVersion<>(event));
        }
        catch (Exception e) {
          if (!retryAttempted)
            return executeModifyFeatures(event);
          else
              throw e;
        }

        try (final Connection connection = dataSourceProvider.getWriter().getConnection()) {

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
                    //Retry
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

            //Set the version in the elements being returned
            collection.getFeatures().forEach(f -> f.getProperties().getXyzNamespace().setVersion(version));

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

        try (final Connection connection = dataSourceProvider.getWriter().getConnection()) {
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
                boolean tableExists = run(new TableExists(new Table(getDatabaseSettings().getSchema(),
                    XyzEventBasedQueryRunner.readTableFromEvent(event))));
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
            .withVariable(SCHEMA, schema)
            .withVariable(TABLE, tableName)
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
            .withVariable(SCHEMA, schema)
            .withVariable(TABLE, table)
            .withVariable("constraintName", table + "_primKey");

        if (existingSerial != null)
            createTable.setVariable("existingSerial", existingSerial);

        stmt.addBatch(createTable.substitute().text());

        //Add new way of using different storage-type for columns
        alterColumnStorage(stmt, schema, table);

        if (withIndices)
            createIndices(stmt, schema, table);
    }

    private void createSpaceStatement(Statement stmt, Event event) throws SQLException {
        String schema = getDatabaseSettings().getSchema();
        String table = XyzEventBasedQueryRunner.readTableFromEvent(event);

        createSpaceTableStatement(stmt, schema, table, true, null);
        createHeadPartition(stmt, schema, table);
        createHistoryPartition(stmt, schema, table, 0L);

        stmt.addBatch(buildCreateSequenceQuery(schema, table, "version").substitute().text());

        stmt.setQueryTimeout(calculateTimeout());
    }

    private static SQLQuery buildCreateSequenceQuery(String schema, String table, String columnName) {
        return new SQLQuery("CREATE SEQUENCE IF NOT EXISTS ${schema}.${sequence} MINVALUE 0 OWNED BY ${schema}.${table}.${columnName}")
            .withVariable(SCHEMA, schema)
            .withVariable(TABLE, table)
            .withVariable("sequence", table + "_" + columnName + "_seq")
            .withVariable("columnName", columnName);
    }

    private static void createIndices(Statement stmt, String schema, String table) throws SQLException {
        for (SQLQuery indexCreationQuery : buildSpaceTableIndexQueries(schema, table))
            stmt.addBatch(indexCreationQuery.substitute().text());
    }

    static int calculateTimeout() throws SQLException{
        int remainingSeconds = ConnectorRuntime.getInstance().getRemainingTime() / 1000;

        if (!isRemainingTimeSufficient(remainingSeconds))
            throw new SQLException("No time left to execute query.","54000");

        int timeout = remainingSeconds - 2;
        logger.debug("{} New timeout for query set to '{}'", ConnectorRuntime.getInstance().getStreamId(), timeout);
        return timeout;
    }

    private static boolean isRemainingTimeSufficient(int remainingSeconds) {
        if (remainingSeconds <= MIN_REMAINING_TIME_FOR_RETRY_SECONDS) {
            logger.warn("{} Not enough time left to execute query: {}s", ConnectorRuntime.getInstance().getStreamId(), remainingSeconds);
            return false;
        }
        return true;
    }

    public DatabaseSettings getDatabaseSettings() {
        return dbSettings;
    }
}
