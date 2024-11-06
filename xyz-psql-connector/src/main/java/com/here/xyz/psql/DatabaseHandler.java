/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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
import static com.here.xyz.psql.query.XyzEventBasedQueryRunner.readTableFromEvent;
import static com.here.xyz.responses.XyzError.NOT_IMPLEMENTED;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.connectors.StorageConnector;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.psql.query.ExtendedSpace;
import com.here.xyz.psql.query.GetFeaturesById;
import com.here.xyz.psql.query.helpers.FetchExistingIds;
import com.here.xyz.psql.query.helpers.FetchExistingIds.FetchIdsInput;
import com.here.xyz.psql.query.helpers.versioning.GetNextVersion;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.util.Random;
import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.util.db.ECPSTool;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.CachedPooledDataSources;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.datasource.DatabaseSettings.ScriptResourcePath;
import com.here.xyz.util.runtime.FunctionRuntime;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
    //TODO - set scriptResourcePath if ext & h3 functions should get installed here.
    private static final List<ScriptResourcePath> SCRIPT_RESOURCE_PATHS = List.of(new ScriptResourcePath("/sql", "hub", "common"));
    public static final String ECPS_PHRASE = "ECPS_PHRASE";
    private static final Logger logger = LogManager.getLogger();
    private static final String MAINTENANCE_ENDPOINT = "MAINTENANCE_SERVICE_ENDPOINT";

    /**
     * Lambda Execution Time = 25s. We are actively canceling queries after STATEMENT_TIMEOUT_SECONDS
     * So if we receive a timeout prior 25s-STATEMENT_TIMEOUT_SECONDS the cancellation comes from
     * outside.
     **/
    static final int MIN_REMAINING_TIME_FOR_RETRY_SECONDS = 3;

    private static String INCLUDE_OLD_STATES = "includeOldStates"; // read from event params

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
        String connectorId = traceItem.getConnectorId();
        ConnectorParameters connectorParams = ConnectorParameters.fromEvent(event);

        //Decrypt the ECPS into an instance of DatabaseSettings
        dbSettings = new DatabaseSettings(connectorId,
            ECPSTool.decryptToMap(FunctionRuntime.getInstance().getEnvironmentVariable(ECPS_PHRASE), connectorParams.getEcps()))
            .withApplicationName(FunctionRuntime.getInstance().getApplicationName())
            .withScriptResourcePaths(SCRIPT_RESOURCE_PATHS);

        dataSourceProvider = new CachedPooledDataSources(dbSettings);
        retryAttempted = false;
        dbMaintainer = new DatabaseMaintainer(dbSettings, connectorParams,
            FunctionRuntime.getInstance().getEnvironmentVariable(MAINTENANCE_ENDPOINT));
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
            int remainingSeconds = FunctionRuntime.getInstance().getRemainingTime() / 1000;

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

    protected FeatureCollection executeModifyFeatures(ModifyFeaturesEvent event) throws Exception {
        if (ConnectorParameters.fromEvent(event).isReadOnly())
            throw new ErrorResponseException(NOT_IMPLEMENTED, "ModifyFeaturesEvent is not supported by this storage connector.");

        //Update the features to insert
        List<Feature> inserts = Optional.ofNullable(event.getInsertFeatures()).orElse(new ArrayList<>());
        List<Feature> updates = Optional.ofNullable(event.getUpdateFeatures()).orElse(new ArrayList<>());
        List<Feature> upserts = Optional.ofNullable(event.getUpsertFeatures()).orElse(new ArrayList<>());

        //Generate feature ID
        Stream.of(inserts, upserts)
            .flatMap(Collection::stream)
            .filter(feature -> feature.getId() == null)
            .forEach(feature -> feature.setId(Random.randomAlphaNumeric(16)));

        //Call finalize feature
        Stream.of(inserts, updates, upserts)
            .flatMap(Collection::stream)
            .forEach(feature -> Feature.finalizeFeature(feature, event.getSpace()));

        final boolean includeOldStates = event.getParams() != null && event.getParams().get(INCLUDE_OLD_STATES) == Boolean.TRUE;

        final FeatureCollection collection = new FeatureCollection();
        collection.setFeatures(new ArrayList<>());

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
                      long now = System.currentTimeMillis();
                      Feature toDelete = new Feature()
                        .withId(featureId)
                        .withProperties(new Properties().withXyzNamespace(new XyzNamespace()
                            .withDeleted(true)
                            .withAuthor(event.getAuthor())
                            .withCreatedAt(now)
                            .withUpdatedAt(now)
                        ));

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
            List<Feature> existingFeatures = loadExistingFeatures(event, idsToFetch);
            if (existingFeatures != null) {
              collection.setOldFeatures(existingFeatures);
            }
          }

          /** Include Upserts */
          if (!upserts.isEmpty()) {
            List<String> upsertIds = upserts.stream().map(Feature::getId).filter(Objects::nonNull).collect(Collectors.toList());
            List<String> existingIds = run(new FetchExistingIds(new FetchIdsInput(readTableFromEvent(event),
                upsertIds)));
            upserts.forEach(f -> {
                if (existingIds.contains(f.getId())) {
                    f.getProperties().getXyzNamespace().withCreatedAt(0);
                    updates.add(f);
                }
                else {
                    f.getProperties().getXyzNamespace().withCreatedAt(f.getProperties().getXyzNamespace().getUpdatedAt());
                    inserts.add(f);
                }
            });
          }

          version = run(new GetNextVersion<>(event));
        }
        catch (Exception e) {
          if (!retryAttempted)
            return executeModifyFeatures(event);
          else
              throw e;
        }

        /*
        NOTE: This is a workaround for tables which have no unique constraint
        TODO: Remove this workaround once all constraints have been adjusted accordingly
         */
        boolean uniqueConstraintExists = checkUniqueTableConstraint(event);

        try (final Connection connection = dataSourceProvider.getWriter().getConnection()) {

            boolean previousAutoCommitState = connection.getAutoCommit();
            connection.setAutoCommit(!event.getTransaction());

            try {
                if (deletes.size() > 0) {
                    DatabaseWriter.modifyFeatures(this, event, DELETE, collection, fails, new ArrayList(deletes.entrySet()), connection, version, uniqueConstraintExists);
                }
                if (inserts.size() > 0) {
                    DatabaseWriter.modifyFeatures(this, event, INSERT, collection, fails, inserts, connection, version, uniqueConstraintExists);
                }
                if (updates.size() > 0) {
                    DatabaseWriter.modifyFeatures(this, event, UPDATE, collection, fails, updates, connection, version, uniqueConstraintExists);
                }

                if (event.getTransaction()) {
                    /** Commit SQLS in one transaction */
                    connection.commit();
                }
            } catch (Exception e) {
                /** No time left for processing */
                if(e instanceof SQLException sqlException && sqlException.getSQLState() !=null
                        && sqlException.getSQLState().equalsIgnoreCase("54000")) {
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

                    if (e instanceof SQLException sqlException && sqlException.getSQLState() != null
                            && sqlException.getSQLState().equalsIgnoreCase("42P01"))
                        ;//Table does not exist yet - create it!
                    else {

                        logger.warn("{} Transaction has failed. ", traceItem, e);
                        connection.close();

                        Map<String, Object> errorDetails = new HashMap<>();

                        if (e instanceof BatchUpdateException || fails.size() >= 1) {
                            //23505 = Object already exists
                            if (e instanceof BatchUpdateException bue && !bue.getSQLState().equalsIgnoreCase("23505"))
                                throw e;

                            errorDetails.put("FailedList", fails);
                            throw new ErrorResponseException(new ErrorResponse().withErrorDetails(errorDetails).withError(XyzError.CONFLICT).withErrorMessage(DatabaseWriter.TRANSACTION_ERROR_GENERAL));
                        }
                        else {
                            errorDetails.put(DatabaseWriter.TRANSACTION_ERROR_GENERAL,
                                    (e instanceof SQLException sqlException && sqlException.getSQLState() != null)
                                            ? "SQL-state: " + sqlException.getSQLState() : "Unexpected Error occurred");
                            throw new ErrorResponseException(new ErrorResponse().withErrorDetails(errorDetails).withError(XyzError.BAD_GATEWAY).withErrorMessage(DatabaseWriter.TRANSACTION_ERROR_GENERAL));
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

    private boolean checkUniqueTableConstraint(ModifyFeaturesEvent event) throws SQLException {
        return new SQLQuery("SELECT 1 FROM pg_catalog.pg_constraint "
            + "WHERE connamespace::regnamespace::text = #{schema} AND conname = #{constraintName}")
            .withNamedParameter("schema", getDatabaseSettings().getSchema())
            .withNamedParameter("constraintName", readTableFromEvent(event) + "_unique")
            .run(dataSourceProvider, rs -> rs.next());
    }

    private List<Feature> loadExistingFeatures(ModifyFeaturesEvent event, List<String> idsToFetch) throws SQLException,
        ErrorResponseException {
        GetFeaturesByIdEvent fetchEvent = new GetFeaturesByIdEvent()
            .withSpace(event.getSpace())
            .withContext(event.getContext())
            .withStreamId(event.getStreamId())
            .withParams(event.getParams())
            .withConnectorParams(event.getConnectorParams())
            .withIds(idsToFetch);

      try {
        return run(new GetFeaturesById(fetchEvent)).getFeatures();
      }
      catch (JsonProcessingException e) {
        logger.error("Error while fetching existing features during feature modification.", e);
        return Collections.emptyList();
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

    private static boolean isForExtendingSpace(Event event) {
        return event.getParams() != null && event.getParams().containsKey("extends");
    }

    public static int readVersionsToKeep(Event event) {
        if (event.getParams() == null || !event.getParams().containsKey("versionsToKeep"))
            return DEFAULT_VERSIONS_TO_KEEP;
        return (int) event.getParams().get("versionsToKeep");
    }

    static int calculateTimeout() throws SQLException{
        int remainingSeconds = FunctionRuntime.getInstance().getRemainingTime() / 1000;

        if (!isRemainingTimeSufficient(remainingSeconds))
            throw new SQLException("No time left to execute query.","54000");

        int timeout = remainingSeconds - 2;
        logger.debug("{} New timeout for query set to '{}'", FunctionRuntime.getInstance().getStreamId(), timeout);
        return timeout;
    }

    private static boolean isRemainingTimeSufficient(int remainingSeconds) {
        if (remainingSeconds <= MIN_REMAINING_TIME_FOR_RETRY_SECONDS) {
            logger.warn("{} Not enough time left to execute query: {}s", FunctionRuntime.getInstance().getStreamId(), remainingSeconds);
            return false;
        }
        return true;
    }

    public DatabaseSettings getDatabaseSettings() {
        return dbSettings;
    }
}
