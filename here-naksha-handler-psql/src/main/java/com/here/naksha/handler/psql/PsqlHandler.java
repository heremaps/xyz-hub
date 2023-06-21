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

package com.here.naksha.handler.psql;

import static com.here.naksha.lib.core.NakshaLogger.currentLogger;
import static com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByTileResponseType.MVT;
import static com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByTileResponseType.MVT_FLATTENED;
import static com.here.naksha.lib.core.models.payload.events.space.ModifySpaceEvent.Operation.CREATE;
import static com.here.naksha.lib.core.models.payload.events.space.ModifySpaceEvent.Operation.UPDATE;
import static com.here.naksha.lib.core.models.payload.responses.XyzError.EXCEPTION;
import static com.here.naksha.lib.psql.sql.QuadbinSQL.COUNTMODE_ESTIMATED;
import static com.here.naksha.lib.psql.sql.QuadbinSQL.COUNTMODE_MIXED;
import static com.here.naksha.lib.psql.sql.QuadbinSQL.COUNTMODE_REAL;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.naksha.handler.psql.query.ExtendedSpace;
import com.here.naksha.handler.psql.query.GetFeaturesByGeometry;
import com.here.naksha.handler.psql.query.GetFeaturesById;
import com.here.naksha.handler.psql.query.GetStorageStatistics;
import com.here.naksha.handler.psql.query.IterateFeatures;
import com.here.naksha.handler.psql.query.LoadFeatures;
import com.here.naksha.handler.psql.query.ModifySpace;
import com.here.naksha.handler.psql.query.SearchForFeatures;
import com.here.naksha.handler.psql.query.helpers.FetchExistingIds;
import com.here.naksha.handler.psql.query.helpers.FetchExistingIds.FetchIdsInput;
import com.here.naksha.lib.core.ExtendedEventHandler;
import com.here.naksha.lib.core.IEventContext;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.geojson.HQuad;
import com.here.naksha.lib.core.models.geojson.WebMercatorTile;
import com.here.naksha.lib.core.models.geojson.coordinates.BBox;
import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import com.here.naksha.lib.core.models.geojson.implementation.FeatureCollection;
import com.here.naksha.lib.core.models.hub.plugins.Connector;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.admin.ModifySubscriptionEvent;
import com.here.naksha.lib.core.models.payload.events.clustering.Clustering;
import com.here.naksha.lib.core.models.payload.events.clustering.ClusteringHexBin;
import com.here.naksha.lib.core.models.payload.events.clustering.ClusteringQuadBin;
import com.here.naksha.lib.core.models.payload.events.clustering.ClusteringQuadBin.CountMode;
import com.here.naksha.lib.core.models.payload.events.feature.DeleteFeaturesByTagEvent;
import com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByBBoxEvent;
import com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByGeometryEvent;
import com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByIdEvent;
import com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByTileEvent;
import com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByTileResponseType;
import com.here.naksha.lib.core.models.payload.events.feature.IterateFeaturesEvent;
import com.here.naksha.lib.core.models.payload.events.feature.LoadFeaturesEvent;
import com.here.naksha.lib.core.models.payload.events.feature.ModifyFeaturesEvent;
import com.here.naksha.lib.core.models.payload.events.feature.SearchForFeaturesEvent;
import com.here.naksha.lib.core.models.payload.events.feature.history.IterateHistoryEvent;
import com.here.naksha.lib.core.models.payload.events.info.GetHistoryStatisticsEvent;
import com.here.naksha.lib.core.models.payload.events.info.GetStatisticsEvent;
import com.here.naksha.lib.core.models.payload.events.info.GetStorageStatisticsEvent;
import com.here.naksha.lib.core.models.payload.events.info.HealthCheckEvent;
import com.here.naksha.lib.core.models.payload.events.space.ModifySpaceEvent;
import com.here.naksha.lib.core.models.payload.events.tweaks.Tweaks;
import com.here.naksha.lib.core.models.payload.events.tweaks.TweaksEnsure;
import com.here.naksha.lib.core.models.payload.events.tweaks.TweaksSampling;
import com.here.naksha.lib.core.models.payload.events.tweaks.TweaksSimplification;
import com.here.naksha.lib.core.models.payload.responses.BinaryResponse;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.models.payload.responses.HealthStatus;
import com.here.naksha.lib.core.models.payload.responses.HistoryStatisticsResponse;
import com.here.naksha.lib.core.models.payload.responses.StatisticsResponse;
import com.here.naksha.lib.core.models.payload.responses.StatisticsResponse.Value;
import com.here.naksha.lib.core.models.payload.responses.SuccessResponse;
import com.here.naksha.lib.core.models.payload.responses.XyzError;
import com.here.naksha.lib.core.models.payload.responses.changesets.Changeset;
import com.here.naksha.lib.core.models.payload.responses.changesets.ChangesetCollection;
import com.here.naksha.lib.core.models.payload.responses.changesets.CompactChangeset;
import com.here.naksha.lib.core.util.NanoTime;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.lib.psql.PsqlCollection;
import com.here.naksha.lib.psql.PsqlDataSource;
import com.here.naksha.lib.psql.sql.DhString;
import com.here.naksha.lib.psql.sql.SQLQuery;
import com.here.naksha.lib.psql.sql.TweaksSQL;
import com.mchange.v2.c3p0.AbstractConnectionCustomizer;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.StatementConfiguration;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("NotNullFieldNotInitialized")
public class PsqlHandler extends ExtendedEventHandler<Connector> {

    public PsqlHandler(@NotNull Connector eventHandler) throws XyzErrorException {
        super(eventHandler);
        connectorParams = new PsqlHandlerParams(eventHandler.getProperties());
    }

    private static final Logger logger = LoggerFactory.getLogger(PsqlHandler.class);

    private final @NotNull PsqlHandlerParams connectorParams;
    private @NotNull Event event;
    private @NotNull String applicationName;
    private @NotNull PsqlDataSource adminDataSource;
    private @NotNull PsqlSpaceMasterDataSource masterDataSource;
    private @NotNull PsqlSpaceReplicaDataSource replicaDataSource;
    private @NotNull String spaceId;
    private @NotNull String table;

    @Override
    public void initialize(@NotNull IEventContext ctx) {
        this.event = ctx.getEvent();
        this.retryAttempted = false;
        applicationName = eventHandler.getId() + ":" + event.getStreamId();
        assert event.getSpaceId() != null;
        spaceId = event.getSpaceId();
        table = event.getCollection(spaceId);
        masterDataSource = new PsqlSpaceMasterDataSource(connectorParams, spaceId, table);
        replicaDataSource = new PsqlSpaceReplicaDataSource(connectorParams, spaceId, table);

        replacements.put("idx_deleted", "idx_" + table + "_deleted");
        replacements.put("idx_serial", "idx_" + table + "_serial");
        replacements.put("idx_id", "idx_" + table + "_id");
        replacements.put("idx_tags", "idx_" + table + "_tags");
        replacements.put("idx_geo", "idx_" + table + "_geo");
        replacements.put("idx_createdAt", "idx_" + table + "_createdAt");
        replacements.put("idx_updatedAt", "idx_" + table + "_updatedAt");
        replacements.put("idx_viz", "idx_" + table + "_viz");

        // TODO: Do we need this, I would say no!
        final String historyTable = table + "_hst";
        replacements.put("idx_hst_id", "idx_" + historyTable + "_id");
        replacements.put("idx_hst_uuid", "idx_" + historyTable + "_uuid");
        replacements.put("idx_hst_updatedAt", "idx_" + historyTable + "_updatedAt");
        replacements.put("idx_hst_version", "idx_" + historyTable + "_version");
        replacements.put("idx_hst_deleted", "idx_" + historyTable + "_deleted");
        replacements.put("idx_hst_lastVersion", "idx_" + historyTable + "_lastVersion");
        replacements.put("idx_hst_idvsort", "idx_" + historyTable + "_idvsort");
        replacements.put("idx_hst_vidsort", "idx_" + historyTable + "_vidsort");
    }

    public final @NotNull Event event() {
        return event;
    }

    public final boolean isReadOnly() {
        // TODO: Allow the connector via configuration in the params to make the space read-only!
        return false;
    }

    public final @NotNull String streamId() {
        return currentLogger().streamId();
    }

    public final @NotNull DataSource masterDataSource() {
        return masterDataSource;
    }

    public final @NotNull DataSource readDataSource() {
        return replicaDataSource;
    }

    public final @Nullable PsqlCollection getSpaceById(@Nullable CharSequence spaceId) {
        throw new UnsupportedOperationException("getSpaceById");
    }

    public final @NotNull String spaceId() {
        return event.getSpaceId();
    }

    public final @NotNull String spaceSchema() {
        return masterDataSource.getSchema();
    }

    public final @NotNull String spaceTable() {
        return table;
    }

    @Deprecated
    public final @NotNull String spaceHistoryTable() {
        return table + "_hst";
    }

    public final @NotNull PsqlHandlerParams connectorParams() {
        return connectorParams;
    }

    void maintainSpace() {
        // TODO: Ensure that table and history is correct, garbage collect history.
    }

    @Override
    public @NotNull XyzResponse processHealthCheckEvent(@NotNull HealthCheckEvent event) {
        try {
            currentLogger().info("Received HealthCheckEvent, perform database maintenance");
            // Naksha.setupH3(this.event.masterPool(), this.event.logId());
            // NakshaPsqlClient.maintain();
            return new HealthStatus();
            //    } catch (SQLException e) {
            //      return checkSQLException(e);
            //    } catch (IOException e) {
            //      currentTask().error("Failed to load resources", e);
            //      return new ErrorResponse()
            //          .withStreamId(streamId())
            //          .withError(EXCEPTION)
            //          .withErrorMessage("Failed to install SQL extension in connector storage.");
        } catch (Exception e) {
            return new ErrorResponse()
                    .withStreamId(streamId())
                    .withError(EXCEPTION)
                    .withErrorMessage("Unknown internal error.");
        } finally {
            currentLogger().info("Finished HealthCheckEvent");
        }
    }

    @Override
    public @NotNull XyzResponse processGetHistoryStatisticsEvent(@NotNull GetHistoryStatisticsEvent event) {
        try {
            currentLogger().info("Received HistoryStatisticsEvent");
            return executeQueryWithRetry(
                    SQLQueryBuilder.buildGetStatisticsQuery(this, true),
                    this::getHistoryStatisticsResultSetHandler,
                    true);
        } catch (SQLException e) {
            return checkSQLException(e);
        } finally {
            currentLogger().info("Finished GetHistoryStatisticsEvent");
        }
    }

    @Override
    public @NotNull XyzResponse processGetStatistics(@NotNull GetStatisticsEvent event) throws Exception {
        try {
            currentLogger().info("Received GetStatisticsEvent");
            return executeQueryWithRetry(
                    SQLQueryBuilder.buildGetStatisticsQuery(this, false), this::getStatisticsResultSetHandler, true);
        } catch (SQLException e) {
            return checkSQLException(e);
        } finally {
            currentLogger().info("Finished GetStatisticsEvent");
        }
    }

    @Override
    public @NotNull XyzResponse processGetFeaturesByIdEvent(@NotNull GetFeaturesByIdEvent event) throws Exception {
        try {
            currentLogger().info("Received GetFeaturesByIdEvent");
            if (event.getIds() == null || event.getIds().size() == 0) {
                return new FeatureCollection();
            }

            return new GetFeaturesById(event, this).run();
        } catch (SQLException e) {
            return checkSQLException(e);
        } finally {
            currentLogger().info("Finished GetFeaturesByIdEvent");
        }
    }

    @Override
    public @NotNull XyzResponse processGetFeaturesByGeometryEvent(@NotNull GetFeaturesByGeometryEvent event)
            throws Exception {
        try {
            currentLogger().info("Received GetFeaturesByGeometryEvent");
            return new GetFeaturesByGeometry(event, this).run();
        } catch (SQLException e) {
            return checkSQLException(e);
        } finally {
            currentLogger().info("Finished GetFeaturesByGeometryEvent");
        }
    }

    private static class TupleTime {

        private static Map<String, String> rTuplesMap = new ConcurrentHashMap<String, String>();
        long outTs;

        TupleTime() {
            outTs = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(15);
            rTuplesMap.clear();
        }

        boolean expired() {
            return System.currentTimeMillis() > outTs;
        }
    }

    private static TupleTime ttime = new TupleTime();

    private static boolean isVeryLargeSpace(String rTuples) {
        long tresholdVeryLargeSpace = 350000000L; // 350M Objects

        if (rTuples == null) {
            return false;
        }

        String[] a = rTuples.split("~");
        if (a == null || a[0] == null) {
            return false;
        }

        try {
            return tresholdVeryLargeSpace <= Long.parseLong(a[0]);
        } catch (NumberFormatException e) {
        }

        return false;
    }

    @Override
    public @NotNull XyzResponse processGetFeaturesByBBoxEvent(@NotNull GetFeaturesByBBoxEvent event) throws Exception {
        try {
            currentLogger().info("Received {}", event.getClass().getSimpleName());

            final BBox bbox = event.getBbox();

            final Tweaks tweaks = event.getTweaks();
            final Clustering clustering = event.getClustering();

            GetFeaturesByTileResponseType responseType = SQLQueryBuilder.getResponseType(event);
            int mvtMargin = 0;
            boolean bMvtRequested = responseType == MVT || responseType == MVT_FLATTENED,
                    bMvtFlattend = responseType == MVT_FLATTENED;

            WebMercatorTile mercatorTile = null;
            HQuad hereTile = null;
            GetFeaturesByTileEvent tileEv =
                    (event instanceof GetFeaturesByTileEvent ? (GetFeaturesByTileEvent) event : null);
            if (tileEv != null && tileEv.getHereTileFlag()) {
                if (clustering != null) {
                    throw new XyzErrorException(
                            XyzError.ILLEGAL_ARGUMENT,
                            "clustering=[hexbin,quadbin] is not supported for 'here' tile type. Use Web Mercator projection (quadkey, web, tms).");
                }
            }

            if (bMvtRequested && tileEv == null) {
                throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "mvt format needs tile request");
            }

            if (bMvtRequested) {
                if (eventHandler.getProperties().get("mvtSupport") != Boolean.TRUE) {
                    throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "mvt format is not supported");
                }

                if (!tileEv.getHereTileFlag()) {
                    mercatorTile = WebMercatorTile.forWeb(tileEv.getLevel(), tileEv.getX(), tileEv.getY());
                } else {
                    hereTile = new HQuad(tileEv.getX(), tileEv.getY(), tileEv.getLevel());
                }

                mvtMargin = tileEv.getMargin();
            }

            final boolean explicitlySelectAll;
            if (event.getSelection() != null
                    && event.getSelection().size() > 0
                    && "*".equals(event.getSelection().get(0))) {
                event.setSelection(null);
                // Differentiation needed, due to different semantic of "event.getSelection() == null"
                // tweaks vs. nonTweaks.
                explicitlySelectAll = true;
            } else {
                explicitlySelectAll = false;
            }

            final String spaceId = event.getSpaceId();
            assert spaceId != null;
            if (tweaks != null) {
                boolean bSortByHashedValue = false;
                if (tweaks instanceof TweaksEnsure) {
                    final TweaksEnsure ensure = (TweaksEnsure) tweaks;
                    if (ttime.expired()) {
                        ttime = new TupleTime();
                    }

                    String rTuples = TupleTime.rTuplesMap.get(event.getSpaceId());
                    final Feature estimateFtr = executeQueryWithRetry(
                                    SQLQueryBuilder.buildEstimateSamplingStrengthQuery(event, bbox, rTuples))
                            .getFeatures()
                            .get(0);
                    final int rCount = (int) estimateFtr.get("rcount");
                    if (rTuples == null) {
                        rTuples = (String) estimateFtr.get("rtuples");
                        TupleTime.rTuplesMap.put(event.getSpaceId(), rTuples);
                    }
                    bSortByHashedValue = isVeryLargeSpace(rTuples);

                    if (event.getSelection() == null && !ensure.defaultSelection && !explicitlySelectAll) {
                        event.setSelection(Arrays.asList("id", "type"));
                    }

                    final int distStrength = TweaksSQL.calculateDistributionStrength(rCount, ensure.sampling.threshold);
                    ensure.algorithm = TweaksSampling.Algorithm.DISTRIBUTION;
                    // fall thru tweaks=sampling
                }
                if (tweaks instanceof TweaksSampling) {
                    final TweaksSampling sampling = (TweaksSampling) tweaks;
                    if (!bMvtRequested) {
                        final FeatureCollection collection = executeQueryWithRetrySkipIfGeomIsNull(
                                SQLQueryBuilder.buildSamplingTweaksQuery(event, bbox, sampling, bSortByHashedValue));
                        if (sampling.sampling.strength == 0) {
                            collection.setPartial(true);
                        }
                        return collection;
                    }
                    return executeBinQueryWithRetry(SQLQueryBuilder.buildMvtEncapsuledQuery(
                            spaceId,
                            SQLQueryBuilder.buildSamplingTweaksQuery(event, bbox, sampling, bSortByHashedValue),
                            mercatorTile,
                            hereTile,
                            bbox,
                            mvtMargin,
                            bMvtFlattend));
                }
                assert tweaks instanceof TweaksSimplification;
                final TweaksSimplification simplification = (TweaksSimplification) tweaks;
                if (!bMvtRequested) {
                    return executeQueryWithRetrySkipIfGeomIsNull(
                            SQLQueryBuilder.buildSimplificationTweaksQuery(event, bbox, simplification));
                }
                return executeBinQueryWithRetry(SQLQueryBuilder.buildMvtEncapsuledQuery(
                        event.getSpaceId(),
                        SQLQueryBuilder.buildSimplificationTweaksQuery(event, bbox, simplification),
                        mercatorTile,
                        hereTile,
                        bbox,
                        mvtMargin,
                        bMvtFlattend));
            }

            if (clustering != null) {
                if (clustering instanceof ClusteringHexBin) {
                    final ClusteringHexBin hexBinClustering = (ClusteringHexBin) clustering;
                    if (!bMvtRequested) {
                        return executeQueryWithRetry(
                                SQLQueryBuilder.buildHexbinClusteringQuery(event, bbox, hexBinClustering), false);
                    }
                    return executeBinQueryWithRetry(
                            SQLQueryBuilder.buildMvtEncapsuledQuery(
                                    event.getSpaceId(),
                                    SQLQueryBuilder.buildHexbinClusteringQuery(event, bbox, hexBinClustering),
                                    mercatorTile,
                                    hereTile,
                                    bbox,
                                    mvtMargin,
                                    bMvtFlattend),
                            false);
                }
                if (clustering instanceof ClusteringQuadBin) {
                    final ClusteringQuadBin quadBinClustering = (ClusteringQuadBin) clustering;
                    final int relResolution;
                    if (quadBinClustering.getRelativeResolution() != null) {
                        relResolution = quadBinClustering.getRelativeResolution();
                    } else {
                        relResolution = 0;
                    }
                    final int absResolution;
                    if (quadBinClustering.getAbsoluteResolution() != null) {
                        absResolution = quadBinClustering.getAbsoluteResolution();
                    } else {
                        absResolution = 0;
                    }

                    checkQuadbinInput(quadBinClustering.countMode, relResolution, event, streamId());

                    if (!bMvtRequested) {
                        return executeQueryWithRetry(SQLQueryBuilder.buildQuadbinClusteringQuery(
                                event,
                                bbox,
                                relResolution,
                                absResolution,
                                quadBinClustering.countMode,
                                quadBinClustering.noBuffer));
                    } else {
                        return executeBinQueryWithRetry(SQLQueryBuilder.buildMvtEncapsuledQuery(
                                spaceTable(),
                                SQLQueryBuilder.buildQuadbinClusteringQuery(
                                        event,
                                        bbox,
                                        relResolution,
                                        absResolution,
                                        quadBinClustering.countMode,
                                        quadBinClustering.noBuffer),
                                mercatorTile,
                                hereTile,
                                bbox,
                                mvtMargin,
                                bMvtFlattend));
                    }
                }
            }

            final boolean isBigQuery =
                    (bbox.widthInDegree(false) >= (360d / 4d) || (bbox.heightInDegree() >= (180d / 4d)));

            if (isBigQuery)
            // Check if Properties are indexed
            {
                checkCanSearchFor(event);
            }

            if (!bMvtRequested) {
                return executeQueryWithRetry(SQLQueryBuilder.buildGetFeaturesByBBoxQuery(event));
            } else {
                return executeBinQueryWithRetry(SQLQueryBuilder.buildMvtEncapsuledQuery(
                        event.getSpaceId(),
                        SQLQueryBuilder.buildGetFeaturesByBBoxQuery(event),
                        mercatorTile,
                        hereTile,
                        bbox,
                        mvtMargin,
                        bMvtFlattend));
            }

        } catch (SQLException e) {
            return checkSQLException(e);
        } finally {
            currentLogger().info("Finished : {}", event.getClass().getSimpleName());
        }
    }

    /** Check if request parameters are valid. In case of invalidity throw an Exception */
    private void checkQuadbinInput(
            @Nullable CountMode countMode, int relResolution, @NotNull GetFeaturesByBBoxEvent event, String streamId)
            throws XyzErrorException {
        if (countMode == null) {
            throw new XyzErrorException(
                    XyzError.ILLEGAL_ARGUMENT,
                    "Invalid request parameters. Unknown clustering.countmode="
                            + countMode
                            + ". Available are: ["
                            + COUNTMODE_REAL
                            + ","
                            + COUNTMODE_ESTIMATED
                            + ","
                            + COUNTMODE_MIXED
                            + "]!");
        }

        if (relResolution > 5) {
            throw new XyzErrorException(
                    XyzError.ILLEGAL_ARGUMENT,
                    "Invalid request parameters. clustering.relativeResolution="
                            + relResolution
                            + " to high. 5 is maximum!");
        }

        if (event.getPropertiesQuery() != null
                && event.getPropertiesQuery().get(0).size() != 1) {
            throw new XyzErrorException(
                    XyzError.ILLEGAL_ARGUMENT, "Invalid request parameters. Only one Property is allowed");
        }

        checkCanSearchFor(event);
    }

    @Override
    public @NotNull XyzResponse processGetFeaturesByTileEvent(@NotNull GetFeaturesByTileEvent event) throws Exception {
        return processGetFeaturesByBBoxEvent(event);
    }

    @Override
    public @NotNull XyzResponse processIterateFeaturesEvent(@NotNull IterateFeaturesEvent event) throws Exception {
        try {
            currentLogger().info("Received : {}", event.getClass().getSimpleName());
            checkCanSearchFor(event);

            if (isOrderByEvent(event)) {
                return IterateFeatures.findFeaturesSort(event, this);
            }
            if (event.getV() != null) {
                return iterateVersions(event);
            }

            return new IterateFeatures(event, this).run();
        } catch (SQLException e) {
            return checkSQLException(e);
        } finally {
            currentLogger().info("Finished {}", event.getClass().getSimpleName());
        }
    }

    /** Kept for backwards compatibility. Will be removed after refactoring. */
    @Deprecated
    public static boolean isOrderByEvent(IterateFeaturesEvent event) {
        return event.getSort() != null
                || event.getPropertiesQuery() != null
                || event.getPart() != null
                || event.getHandle() != null && event.getHandle().startsWith(IterateFeatures.HPREFIX);
    }

    @Override
    public @NotNull XyzResponse processSearchForFeaturesEvent(@NotNull SearchForFeaturesEvent event) throws Exception {
        try {
            currentLogger().info("Received {}", event.getClass().getSimpleName());
            checkCanSearchFor(event);

            // For testing purposes.
            if (event.getSpaceId().contains("illegal_argument")) // TODO: Remove testing code from the actual connector
            // implementation
            {
                return new ErrorResponse()
                        .withStreamId(streamId())
                        .withError(XyzError.ILLEGAL_ARGUMENT)
                        .withErrorMessage("Invalid request parameters.");
            }

            //noinspection unchecked,rawtypes
            return (XyzResponse) new SearchForFeatures<>((SearchForFeaturesEvent) event, this).run();
        } catch (SQLException e) {
            return checkSQLException(e);
        } finally {
            currentLogger().info("Finished {}", event.getClass().getSimpleName());
        }
    }

    private void checkCanSearchFor(@NotNull SearchForFeaturesEvent event) throws XyzErrorException {
        if (!Capabilities.canSearchFor(event.getPropertiesQuery(), this)) {
            throw new XyzErrorException(
                    XyzError.ILLEGAL_ARGUMENT,
                    "Invalid request parameters. Search for the provided properties is not supported for this"
                            + " space.");
        }
    }

    @Override
    public @NotNull XyzResponse processDeleteFeaturesByTagEvent(@NotNull DeleteFeaturesByTagEvent event)
            throws Exception {
        try {
            currentLogger().info("Received DeleteFeaturesByTagEvent");
            if (isReadOnly()) {
                return new ErrorResponse()
                        .withStreamId(streamId())
                        .withError(XyzError.NOT_IMPLEMENTED)
                        .withErrorMessage("ModifyFeaturesEvent is not supported by this storage connector.");
            }
            return executeDeleteFeaturesByTag(event);
        } catch (SQLException e) {
            return checkSQLException(e);
        } finally {
            currentLogger().info("Finished DeleteFeaturesByTagEvent");
        }
    }

    @Override
    public @NotNull XyzResponse processLoadFeaturesEvent(@NotNull LoadFeaturesEvent event) throws Exception {
        try {
            currentLogger().info("Received LoadFeaturesEvent");
            if (event.getIdsMap() == null || event.getIdsMap().size() == 0) {
                return new FeatureCollection();
            }

            return new LoadFeatures(event, this).run();
        } catch (SQLException e) {
            return checkSQLException(e);
        } finally {
            currentLogger().info("Finished LoadFeaturesEvent");
        }
    }

    @Override
    public @NotNull XyzResponse processModifyFeaturesEvent(@NotNull ModifyFeaturesEvent event) throws Exception {
        try {
            currentLogger().info("Received ModifyFeaturesEvent");
            if (isReadOnly()) {
                return new ErrorResponse()
                        .withStreamId(streamId())
                        .withError(XyzError.NOT_IMPLEMENTED)
                        .withErrorMessage("ModifyFeaturesEvent is not supported by this storage connector.");
            }

            final boolean addUUID = event.getEnableUUID() && event.getVersion().compareTo("0.2.0") < 0;
            // Update the features to insert
            final List<Feature> inserts =
                    Optional.ofNullable(event.getInsertFeatures()).orElse(Collections.emptyList());
            final List<Feature> updates =
                    Optional.ofNullable(event.getUpdateFeatures()).orElse(Collections.emptyList());
            final List<Feature> upserts =
                    Optional.ofNullable(event.getUpsertFeatures()).orElse(Collections.emptyList());

            // Generate feature ID
            Stream.of(inserts, upserts)
                    .flatMap(Collection::stream)
                    .filter(feature -> feature.getId() == null)
                    .forEach(feature -> feature.setId(RandomStringUtils.randomAlphanumeric(16)));

            // Call finalize feature
            return executeModifyFeatures(event);
        } catch (SQLException e) {
            return checkSQLException(e);
        } finally {
            currentLogger().info("Finished ModifyFeaturesEvent");
        }
    }

    @Override
    public @NotNull XyzResponse processModifySpaceEvent(@NotNull ModifySpaceEvent event) throws Exception {
        try {
            currentLogger().info("Received ModifySpaceEvent");

            if (connectorParams().isIgnoreCreateMse()) {
                return new SuccessResponse().withStatus("OK");
            }
            return executeModifySpace(event);
        } catch (SQLException e) {
            return checkSQLException(e);
        } finally {
            currentLogger().info("Finished ModifySpaceEvent");
        }
    }

    @Override
    public @NotNull XyzResponse processModifySubscriptionEvent(@NotNull ModifySubscriptionEvent event)
            throws Exception {
        try {
            currentLogger().info("Received ModifySpaceEvent");
            this.validateModifySubscriptionEvent(event);
            return executeModifySubscription(event);
        } catch (SQLException e) {
            return checkSQLException(e);
        } finally {
            currentLogger().info("Finished ModifySpaceEvent");
        }
    }

    private void validateModifySubscriptionEvent(@NotNull ModifySubscriptionEvent event) throws Exception {
        switch (event.getOperation()) {
            case CREATE:
            case UPDATE:
            case DELETE:
                break;
            default:
                throw new XyzErrorException(
                        XyzError.ILLEGAL_ARGUMENT,
                        "Modify Subscription - Operation (" + event.getOperation() + ") not supported");
        }
    }

    @Override
    public @NotNull XyzResponse processIterateHistoryEvent(@NotNull IterateHistoryEvent event) {
        currentLogger().info("Received IterateHistoryEvent");
        try {
            return executeIterateHistory(event);
        } catch (SQLException e) {
            return checkSQLException(e);
        } finally {
            currentLogger().info("Finished IterateHistoryEvent");
        }
    }

    @Override
    public @NotNull XyzResponse processGetStorageStatisticsEvent(@NotNull GetStorageStatisticsEvent event)
            throws Exception {
        try {
            currentLogger().info("Received {}", event.getClass().getSimpleName());
            return new GetStorageStatistics(event, this).run();
        } catch (SQLException e) {
            return checkSQLException(e);
        } finally {
            currentLogger().info("Finished {}", event.getClass().getSimpleName());
        }
    }

    protected @NotNull XyzResponse iterateVersions(@NotNull IterateFeaturesEvent event) throws SQLException {
        return executeIterateVersions(event);
    }

    private void validateModifySpaceEvent(@NotNull ModifySpaceEvent event) throws Exception {}

    private static final Pattern
            ERRVALUE_22P02 = Pattern.compile("invalid input syntax for type numeric:\\s+\"([^\"]*)\"\\s+Query:"),
            ERRVALUE_22P05 = Pattern.compile("ERROR:\\s+(.*)\\s+Detail:\\s+(.*)\\s+Where:");

    protected @NotNull XyzResponse checkSQLException(@NotNull SQLException e) {
        logger.warn("{}:{} - SQL Error ({}): {}", e.getSQLState(), e);

        final @NotNull String sqlState =
                (e.getSQLState() != null ? e.getSQLState().toUpperCase() : "SNULL");
        final String message = e.getMessage();
        switch (sqlState) {
            case "XX000": /* XX000 - internal error */
                if (message == null) {
                    break;
                }
                if (message.contains("interruptedException")) {
                    break;
                }
                if (message.contains("ERROR: stats for")) {
                    return new ErrorResponse()
                            .withStreamId(streamId())
                            .withError(XyzError.ILLEGAL_ARGUMENT)
                            .withErrorMessage("statistical data for this space is missing (analyze)");
                }
                if (message.contains("TopologyException")) {
                    return new ErrorResponse()
                            .withStreamId(streamId())
                            .withError(XyzError.ILLEGAL_ARGUMENT)
                            .withErrorMessage("geometry with irregular topology (self-intersection, clipping)");
                }
                if (message.contains("ERROR: transform: couldn't project point")) {
                    return new ErrorResponse()
                            .withStreamId(streamId())
                            .withError(XyzError.ILLEGAL_ARGUMENT)
                            .withErrorMessage("projection error");
                }
                if (message.contains("ERROR: encode_geometry: 'GeometryCollection'")) {
                    return new ErrorResponse()
                            .withStreamId(streamId())
                            .withError(XyzError.ILLEGAL_ARGUMENT)
                            .withErrorMessage("dataset contains invalid geometries");
                }
                // fall thru - timeout assuming timeout

            case "57014": /* 57014 - query_canceled */
            case "57P01": /* 57P01 - admin_shutdown */
                return new ErrorResponse()
                        .withStreamId(streamId())
                        .withError(XyzError.TIMEOUT)
                        .withErrorMessage("Database query timed out or got canceled.");

            case "54000":
                return new ErrorResponse()
                        .withStreamId(streamId())
                        .withError(XyzError.TIMEOUT)
                        .withErrorMessage("No time for retry left for database query.");

            case "22P02": // specific handling in case to H3 clustering.property
            {
                if (message == null || !message.contains("'H3'::text")) {
                    break;
                }
                final Matcher m = ERRVALUE_22P02.matcher(message);
                return new ErrorResponse()
                        .withStreamId(streamId())
                        .withError(XyzError.ILLEGAL_ARGUMENT)
                        .withErrorMessage(DhString.format(
                                "clustering.property: string(%s) can not be converted to numeric",
                                (m.find() ? m.group(1) : "")));
            }

            case "22P05": {
                if (message == null) {
                    break;
                }
                String eMsg = "untranslatable character in payload";
                final Matcher m = ERRVALUE_22P05.matcher(e.getMessage());
                if (m.find()) {
                    eMsg = DhString.format(eMsg + ": %s - %s", m.group(1), m.group(2));
                }
                return new ErrorResponse()
                        .withStreamId(streamId())
                        .withError(XyzError.ILLEGAL_ARGUMENT)
                        .withErrorMessage(eMsg);
            }

            case "42P01":
                return new ErrorResponse()
                        .withStreamId(streamId())
                        .withError(XyzError.TIMEOUT)
                        .withErrorMessage(e.getMessage());

            case "40P01": // Database -> deadlock detected e.g. "Process 9452 waits for ShareLock on
                // transaction 2383228826; blocked by process 9342."
                return new ErrorResponse()
                        .withStreamId(streamId())
                        .withError(XyzError.CONFLICT)
                        .withErrorMessage(e.getMessage());

            case "SNULL":
                if (message == null) {
                    break;
                }
                // handle some dedicated messages
                if (message.contains("An attempt by a client to checkout a connection has timed out.")) {
                    return new ErrorResponse()
                            .withStreamId(streamId())
                            .withError(XyzError.TIMEOUT)
                            .withErrorMessage("Cannot get a connection to the database.");
                }

                if (message.contains("Maxchar limit")) {
                    return new ErrorResponse()
                            .withStreamId(streamId())
                            .withError(XyzError.PAYLOAD_TO_LARGE)
                            .withErrorMessage("Database result - Maxchar limit exceed");
                }

                break; // others

            default:
                break;
        }

        return new ErrorResponse().withStreamId(streamId()).withError(EXCEPTION).withErrorMessage(e.getMessage());
    }

    private static final Pattern pattern =
            Pattern.compile("^BOX\\(([-\\d\\.]*)\\s([-\\d\\.]*),([-\\d\\.]*)\\s([-\\d\\.]*)\\)$");
    private static final String C3P0EXT_CONFIG_SCHEMA = "config.schema()";
    public static final String HISTORY_TABLE_SUFFIX = "_hst";

    public static final String APPLICATION_VND_MAPBOX_VECTOR_TILE = "application/vnd.mapbox-vector-tile";

    /**
     * Lambda Execution Time = 25s. We are actively canceling queries after STATEMENT_TIMEOUT_SECONDS
     * So if we receive a timeout prior 25s-STATEMENT_TIMEOUT_SECONDS the cancellation comes from
     * outside.
     */
    private static final int MIN_REMAINING_TIME_FOR_RETRY_SECONDS = 3;

    protected static final int STATEMENT_TIMEOUT_SECONDS = 23;

    private static final String INCLUDE_OLD_STATES = "includeOldStates"; // read from event params

    private final Map<String, String> replacements = new HashMap<>();

    private boolean retryAttempted;

    /** Executes the given query and returns the processed by the handler result. */
    protected <T> T executeQuery(SQLQuery query, ResultSetHandler<T> handler) throws SQLException {
        return executeQuery(query, handler, readDataSource());
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

    /** Executes the query and reattempt to execute the query, after */
    protected <T> T executeQueryWithRetry(SQLQuery query, ResultSetHandler<T> handler, boolean useReadReplica)
            throws SQLException {
        try {
            query.replaceUnnamedParameters();
            query.replaceFragments();
            query.replaceNamedParameters();
            return executeQuery(query, handler, useReadReplica ? readDataSource() : masterDataSource());
        } catch (Exception e) {
            try {
                if (retryCausedOnServerlessDB(e) || canRetryAttempt()) {
                    currentLogger().info("Retry Query permitted.");
                    return executeQuery(query, handler, useReadReplica ? readDataSource() : masterDataSource());
                }
            } catch (Exception e1) {
                if (retryCausedOnServerlessDB(e1)) {
                    currentLogger().info("Retry Query permitted.");
                    return executeQuery(query, handler, useReadReplica ? readDataSource() : masterDataSource());
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
                    currentLogger().info("Retry Update permitted.");
                    return executeUpdate(query);
                }
            } catch (Exception e1) {
                if (retryCausedOnServerlessDB(e)) {
                    currentLogger().info("Retry Update permitted.");
                    return executeUpdate(query);
                }
                throw e;
            }
            throw e;
        }
    }

    protected boolean retryCausedOnServerlessDB(Exception e) {
        /**
         * If a timeout comes directly after the invocation it could rely on serverless aurora scaling.
         * Then we should retry again. 57014 - query_canceled 57P01 - admin_shutdown
         */
        if (e instanceof SQLException
                && ((SQLException) e).getSQLState() != null
                && (((SQLException) e).getSQLState().equalsIgnoreCase("57014")
                        || ((SQLException) e).getSQLState().equalsIgnoreCase("57P01")
                        || ((SQLException) e).getSQLState().equalsIgnoreCase("08003")
                        || ((SQLException) e).getSQLState().equalsIgnoreCase("08006"))) {
            final long remainingSeconds = event.remaining(TimeUnit.SECONDS);
            if (!isRemainingTimeSufficient(remainingSeconds)) {
                return false;
            }
            if (!retryAttempted) {
                currentLogger()
                        .warn("Retry based on serverless scaling detected! RemainingTime: {} {}", remainingSeconds, e);
                return true;
            }
        }
        return false;
    }

    /**
     * Executes the given query and returns the processed by the handler result using the provided
     * dataSource.
     */
    private <T> T executeQuery(
            @NotNull SQLQuery query, @Nullable ResultSetHandler<T> handler, @NotNull DataSource dataSource)
            throws SQLException {
        final long start = System.nanoTime();
        try {
            final org.apache.commons.dbutils.QueryRunner run = new org.apache.commons.dbutils.QueryRunner(
                    dataSource, new StatementConfiguration(null, null, null, null, (int) calculateTimeout()));

            query.setText(SQLQuery.replaceVars(query.text(), spaceSchema(), spaceTable()));
            final String queryText = query.text();
            final List<Object> queryParameters = query.parameters();
            currentLogger().debug("executeQuery: {} - Parameter: {}", queryText, queryParameters);
            return run.query(queryText, handler, queryParameters.toArray());
        } finally {
            currentLogger().info("query time: {}ms", NanoTime.timeSince(start, TimeUnit.MICROSECONDS));
        }
    }

    /**
     * Executes the given update or delete query and returns the number of deleted or updated records.
     *
     * @param query the update or delete query.
     * @return the amount of updated or deleted records.
     * @throws SQLException if any error occurred.
     */
    int executeUpdate(@NotNull SQLQuery query) throws SQLException {
        final long start = System.nanoTime();
        try {
            final org.apache.commons.dbutils.QueryRunner run = new org.apache.commons.dbutils.QueryRunner(
                    masterDataSource(), new StatementConfiguration(null, null, null, null, (int) calculateTimeout()));

            query.setText(SQLQuery.replaceVars(query.text(), spaceSchema(), spaceTable()));
            final String queryText = query.text();
            final List<Object> queryParameters = query.parameters();
            currentLogger().debug("executeUpdate: {} - Parameter: {}", queryText, queryParameters);
            return run.update(queryText, queryParameters.toArray());
        } finally {
            currentLogger().info("query time: {}ms", NanoTime.timeSince(start, TimeUnit.MILLISECONDS));
        }
    }

    protected @NotNull XyzResponse executeModifySpace(@NotNull ModifySpaceEvent event) throws SQLException {
        // TODO: We need to fix this, we do not use this to compact history!
        if (event.getSpaceDefinition() != null && false) {
            boolean compactHistory = connectorParams().isCompactHistory();

            if (event.getOperation() == CREATE)
            // Create History Table
            {
                ensureHistorySpace(null, compactHistory, false);
            } else if (event.getOperation() == UPDATE)
            // Update HistoryTrigger to apply maxVersionCount.
            {
                updateHistoryTrigger(null, compactHistory, false);
            }
        }

        new ModifySpace(event, this).write();
        maintainSpace();

        // If we reach this point we are okay!
        return new SuccessResponse();
    }

    protected @NotNull XyzResponse executeModifySubscription(@NotNull ModifySubscriptionEvent event)
            throws SQLException {
        boolean bLastSubscriptionToDelete = event.getHasNoActiveSubscriptions();
        switch (event.getOperation()) {
            case CREATE:
            case UPDATE:
                final long rVal = executeUpdateWithRetry(
                        SQLQueryBuilder.buildAddSubscriptionQuery(spaceId(), spaceSchema(), spaceTable()));
                setReplicaIdentity();
                return new FeatureCollection().withCount(rVal);

            case DELETE:
                if (!bLastSubscriptionToDelete) {
                    return new FeatureCollection().withCount(1L);
                } else {
                    return new FeatureCollection().withCount((long) executeUpdateWithRetry(
                            SQLQueryBuilder.buildRemoveSubscriptionQuery(spaceId(), spaceSchema())));
                }

            default:
                break;
        }

        return new ErrorResponse()
                .withError(EXCEPTION)
                .withStreamId(streamId())
                .withErrorMessage("Unknown operation: " + event.getOperation().name());
    }

    protected XyzResponse executeIterateHistory(IterateHistoryEvent event) throws SQLException {
        if (event.isCompact()) {
            return executeQueryWithRetry(
                    SQLQueryBuilder.buildSquashHistoryQuery(event), this::compactHistoryResultSetHandler, true);
        }
        return executeQueryWithRetry(SQLQueryBuilder.buildHistoryQuery(event), this::historyResultSetHandler, true);
    }

    protected XyzResponse executeIterateVersions(IterateFeaturesEvent event) throws SQLException {
        SQLQuery query = SQLQueryBuilder.buildLatestHistoryQuery(event);
        return executeQueryWithRetry(query, this::iterateVersionsHandler, true);
    }

    /**
     * @param idsToFetch Ids of objects which should get fetched
     * @return List of Features which could get fetched
     * @throws Exception if any error occurred.
     */
    protected List<Feature> fetchOldStates(String[] idsToFetch) throws Exception {
        List<Feature> oldFeatures = null;
        FeatureCollection oldFeaturesCollection =
                executeQueryWithRetry(SQLQueryBuilder.generateLoadOldFeaturesQuery(idsToFetch));

        if (oldFeaturesCollection != null) {
            oldFeatures = oldFeaturesCollection.getFeatures();
        }

        return oldFeatures;
    }

    protected @NotNull XyzResponse executeModifyFeatures(@NotNull ModifyFeaturesEvent event) throws Exception {
        final boolean handleUUID = event.getEnableUUID() == Boolean.TRUE;
        final boolean transactional = event.getTransaction() == Boolean.TRUE;
        final boolean includeOldStates =
                event.getParams() != null && event.getParams().get(INCLUDE_OLD_STATES) == Boolean.TRUE;
        List<Feature> oldFeatures = null;

        final String schema = spaceSchema();
        final String table = spaceTable();

        Integer version = null;
        final FeatureCollection collection = new FeatureCollection();
        collection.setFeatures(new ArrayList<>());

        List<Feature> inserts = Optional.ofNullable(event.getInsertFeatures()).orElse(new ArrayList<>());
        List<Feature> updates = Optional.ofNullable(event.getUpdateFeatures()).orElse(new ArrayList<>());
        List<Feature> upserts = Optional.ofNullable(event.getUpsertFeatures()).orElse(new ArrayList<>());
        Map<String, String> deletes =
                Optional.ofNullable(event.getDeleteFeatures()).orElse(new HashMap<>());
        List<FeatureCollection.ModificationFailure> fails =
                Optional.ofNullable(event.getFailed()).orElse(new ArrayList<>());
        boolean forExtendingSpace = isForExtendingSpace(event);

        List<String> originalUpdates = updates.stream().map(f -> f.getId()).collect(Collectors.toList());
        List<String> originalDeletes = new ArrayList<>(deletes.keySet());
        // Handle deletes / updates on extended spaces
        if (forExtendingSpace) { // && event.getContext() == DEFAULT
            if (!deletes.isEmpty()) {
                // Transform the incoming deletes into upserts with deleted flag for features which don't
                // exist in the extended layer (base)
                List<String> existingIdsInBase = new FetchExistingIds(
                                new FetchIdsInput(ExtendedSpace.getExtendedTable(event, this), originalDeletes), this)
                        .run();

                for (String featureId : originalDeletes) {
                    if (existingIdsInBase.contains(featureId)) {
                        final Feature feature = new Feature(featureId);
                        feature.getProperties().getXyzNamespace().setDeleted(true);
                        upserts.add(feature);
                        deletes.remove(featureId);
                    }
                }
            }
            if (!updates.isEmpty()) {
                // Transform the incoming updates into upserts, because we don't know whether the object is
                // existing in the extension already
                upserts.addAll(updates);
                updates.clear();
            }
        }

        try {
            /** Include Old states */
            if (includeOldStates) {
                String[] idsToFetch = getAllIds(inserts, updates, upserts, deletes).stream()
                        .filter(Objects::nonNull)
                        .toArray(String[]::new);
                oldFeatures = fetchOldStates(idsToFetch);
                if (oldFeatures != null) {
                    collection.setOldFeatures(oldFeatures);
                }
            }

            /** Include Upserts */
            if (!upserts.isEmpty()) {
                List<String> upsertIds = upserts.stream()
                        .map(Feature::getId)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                List<String> existingIds = new FetchExistingIds(new FetchIdsInput(table, upsertIds), this).run();
                upserts.forEach(f -> (existingIds.contains(f.getId()) ? updates : inserts).add(f));
            }

            /** get next Version */
            if (event.isEnableGlobalVersioning()) {
                SQLQuery query = SQLQueryBuilder.buildGetNextVersionQuery(table);
                version = executeQueryWithRetry(
                        query,
                        rs -> {
                            if (rs.next()) {
                                return rs.getInt(1);
                            }
                            return -1;
                        },
                        false); // false -> not use readreplica due to sequence 'update' statement: SELECT
                // nextval("...._hst_seq"')
                collection.setVersion(version);
            }
        } catch (Exception e) {
            if (!retryAttempted) {
                canRetryAttempt();
                return executeModifyFeatures(event);
            }
        }

        try (final Connection connection = masterDataSource().getConnection()) {

            boolean previousAutoCommitState = connection.getAutoCommit();
            connection.setAutoCommit(!transactional);

            try {
                if (deletes.size() > 0) {
                    DatabaseWriter.deleteFeatures(this, fails, deletes, connection, transactional, handleUUID, version);
                }
                if (inserts.size() > 0) {
                    DatabaseWriter.insertFeatures(
                            this, collection, fails, inserts, connection, transactional, version, forExtendingSpace);
                }
                if (updates.size() > 0) {
                    DatabaseWriter.updateFeatures(
                            this,
                            collection,
                            fails,
                            updates,
                            connection,
                            transactional,
                            handleUUID,
                            version,
                            forExtendingSpace);
                }

                if (transactional) {
                    if (fails == null || fails.isEmpty()) {
                        /** Commit SQLS in one transaction */
                        connection.commit();
                    } else {
                        connection.rollback();
                    }
                }

            } catch (Exception e) {
                /** No time left for processing */
                if (e instanceof SQLException
                        && ((SQLException) e).getSQLState() != null
                        && ((SQLException) e).getSQLState().equalsIgnoreCase("54000")) {
                    throw e;
                }

                /** Add objects which are responsible for the failed operation */
                event.setFailed(fails);

                if (retryCausedOnServerlessDB(e) && !retryAttempted) {
                    retryAttempted = true;

                    if (!connection.isClosed()) {
                        connection.setAutoCommit(previousAutoCommitState);
                        connection.close();
                    }

                    return executeModifyFeatures(event);
                }

                if (transactional) {
                    connection.rollback();

                    if ((e instanceof SQLException
                            && ((SQLException) e).getSQLState() != null
                            && ((SQLException) e).getSQLState().equalsIgnoreCase("42P01"))) {
                        throw e; // Table does not exist yet - create it!
                    } else {

                        logger.warn("{}:{} - Transaction has failed.", e);
                        connection.close();

                        Map<String, Object> errorDetails = new HashMap<>();

                        if (e instanceof BatchUpdateException || fails.size() >= 1) {
                            // 23505 = Object already exists
                            if (e instanceof BatchUpdateException
                                    && !((BatchUpdateException) e).getSQLState().equalsIgnoreCase("23505")) {
                                throw e;
                            }

                            errorDetails.put("FailedList", fails);
                            return new ErrorResponse()
                                    .withErrorDetails(errorDetails)
                                    .withError(XyzError.CONFLICT)
                                    .withErrorMessage(DatabaseWriter.TRANSACTION_ERROR_GENERAL);
                        } else {
                            errorDetails.put(
                                    DatabaseWriter.TRANSACTION_ERROR_GENERAL,
                                    (e instanceof SQLException && ((SQLException) e).getSQLState() != null)
                                            ? "SQL-state: " + ((SQLException) e).getSQLState()
                                            : "Unexpected Error occurred");
                            return new ErrorResponse()
                                    .withErrorDetails(errorDetails)
                                    .withError(XyzError.BAD_GATEWAY)
                                    .withErrorMessage(DatabaseWriter.TRANSACTION_ERROR_GENERAL);
                        }
                    }
                }

                if (!retryAttempted) {
                    if (!connection.isClosed()) {
                        connection.setAutoCommit(previousAutoCommitState);
                        connection.close();
                    }
                    /** Retry */
                    canRetryAttempt();
                    return executeModifyFeatures(event);
                }
            } finally {
                if (!connection.isClosed()) {
                    connection.setAutoCommit(previousAutoCommitState);
                    connection.close();
                }
            }

            /** filter out failed ids */
            final List<String> failedIds = fails.stream()
                    .map(FeatureCollection.ModificationFailure::getId)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            final List<String> insertIds = inserts.stream()
                    .map(Feature::getId)
                    .filter(x -> !failedIds.contains(x) && !originalUpdates.contains(x) && !originalDeletes.contains(x))
                    .collect(Collectors.toList());
            final List<String> updateIds = originalUpdates.stream()
                    .filter(x -> !failedIds.contains(x) && !originalDeletes.contains(x))
                    .collect(Collectors.toList());
            final List<String> deleteIds =
                    originalDeletes.stream().filter(x -> !failedIds.contains(x)).collect(Collectors.toList());

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

    private List<String> getAllIds(
            List<Feature> inserts, List<Feature> updates, List<Feature> upserts, Map<String, ?> deletes) {
        List<String> ids = Stream.of(inserts, updates, upserts)
                .flatMap(Collection::stream)
                .map(Feature::getId)
                .collect(Collectors.toList());
        ids.addAll(deletes.keySet());

        return ids;
    }

    @Deprecated
    protected @NotNull XyzResponse executeDeleteFeaturesByTag(@NotNull DeleteFeaturesByTagEvent event)
            throws SQLException {
        boolean includeOldStates =
                event.getParams() != null && event.getParams().get(INCLUDE_OLD_STATES) == Boolean.TRUE;

        final SQLQuery searchQuery = SQLQueryBuilder.generateSearchQuery(event);
        final SQLQuery query = SQLQueryBuilder.buildDeleteFeaturesByTagQuery(includeOldStates, searchQuery);

        // TODO: check in detail what we want to return
        if (searchQuery != null && includeOldStates) {
            return executeQueryWithRetry(query, this::oldStatesResultSetHandler, false);
        }

        return new FeatureCollection().withCount((long) executeUpdateWithRetry(query));
    }

    private boolean canRetryAttempt() throws Exception {

        if (retryAttempted || !isRemainingTimeSufficient(event.remaining(TimeUnit.SECONDS))) {
            return false;
        }

        ensureSpace();
        retryAttempted = true;

        currentLogger().info("Retry the execution.");
        return true;
    }

    /**
     * A helper method that will test if the table for the space does exist.
     *
     * @return true if the table for the space exists; false otherwise.
     * @throws SQLException if the test fails due to any SQL error.
     */
    protected boolean hasTable() throws SQLException {
        if (event() instanceof HealthCheckEvent) {
            return true;
        }
        final long start = System.nanoTime();
        try (final Connection connection = masterDataSource().getConnection()) {
            Statement stmt = connection.createStatement();
            String query = "SELECT to_regclass('${schema}.${table}')";
            query = SQLQuery.replaceVars(query, spaceSchema(), spaceTable());
            ResultSet rs;

            stmt.setQueryTimeout(calculateTimeout());
            if ((rs = stmt.executeQuery(query)).next()) {
                currentLogger().debug("Time for table check: {}ms", NanoTime.timeSince(start, TimeUnit.MILLISECONDS));
                String oid = rs.getString(1);
                return oid != null;
            }
            return false;
        } catch (Exception e) {
            if (!retryAttempted) {
                retryAttempted = true;
                currentLogger().info("Retry table check.");
                return hasTable();
            } else {
                throw e;
            }
        }
    }

    /**
     * A helper method that will ensure that the tables for the space of this event do exist and is up
     * to date, if not it will alter the table.
     *
     * @throws SQLException if the table does not exist and can't be created or alter failed.
     */
    private static String lockSql = "select pg_advisory_lock( ('x' || left(md5('%s'),15) )::bit(60)::bigint )",
            unlockSql = "select pg_advisory_unlock( ('x' || left(md5('%s'),15) )::bit(60)::bigint )";

    private static void _advisory(String tablename, Connection connection, boolean lock) throws SQLException {
        boolean cStateFlag = connection.getAutoCommit();
        connection.setAutoCommit(true);

        try (Statement stmt = connection.createStatement()) {
            stmt.executeQuery(DhString.format(lock ? lockSql : unlockSql, tablename));
        }

        connection.setAutoCommit(cStateFlag);
    }

    private static void advisoryLock(String tablename, Connection connection) throws SQLException {
        _advisory(tablename, connection, true);
    }

    private static void advisoryUnlock(String tablename, Connection connection) throws SQLException {
        _advisory(tablename, connection, false);
    }

    private static boolean isForExtendingSpace(Event event) {
        return event.getParams() != null && event.getParams().containsKey("extends");
    }

    protected void ensureSpace() throws SQLException {
        // Note: We can assume that when the table exists, the postgis extensions are installed.
        if (hasTable()) {
            return;
        }
        final String tableName = spaceTable();
        try (final Connection connection = masterDataSource().getConnection()) {
            advisoryLock(tableName, connection);
            boolean cStateFlag = connection.getAutoCommit();
            try {

                if (cStateFlag) {
                    connection.setAutoCommit(false);
                }

                try (Statement stmt = connection.createStatement()) {
                    createSpaceStatement(stmt, tableName, isForExtendingSpace(event()));

                    stmt.setQueryTimeout(calculateTimeout());
                    stmt.executeBatch();
                    connection.commit();
                    currentLogger().debug("Successfully created table '{}' for space id '{}'", tableName, spaceId());
                }
            } catch (Exception e) {
                currentLogger().error("Failed to create table '{}' for space id: '{}': {}", tableName, spaceId(), e);
                connection.rollback();
                // check if the table was created in the meantime, by another instance.
                if (hasTable()) {
                    return;
                }
                throw new SQLException("Missing table \"" + tableName + "\" and creation failed: " + e.getMessage(), e);
            } finally {
                advisoryUnlock(tableName, connection);
                if (cStateFlag) {
                    connection.setAutoCommit(true);
                }
            }
        }
    }

    private void createSpaceStatement(Statement stmt, String tableName) throws SQLException {
        createSpaceStatement(stmt, tableName, false);
    }

    private void createSpaceStatement(Statement stmt, String tableName, boolean withDeletedColumn) throws SQLException {
        String query = "CREATE TABLE IF NOT EXISTS ${schema}.${table} (jsondata jsonb, geo"
                + " geometry(GeometryZ,4326), i BIGSERIAL"
                + (withDeletedColumn ? ", deleted BOOLEAN DEFAULT FALSE" : "")
                + ")";

        query = SQLQuery.replaceVars(query, spaceSchema(), tableName);
        stmt.addBatch(query);

        if (withDeletedColumn) {
            query = "CREATE INDEX IF NOT EXISTS ${idx_deleted} ON ${schema}.${table} USING btree (deleted ASC"
                    + " NULLS LAST) WHERE deleted = TRUE";
            query = SQLQuery.replaceVars(query, replacements, spaceSchema(), tableName);
            stmt.addBatch(query);
        }

        query = "CREATE UNIQUE INDEX IF NOT EXISTS ${idx_id} ON ${schema}.${table} ((jsondata->>'id'))";
        query = SQLQuery.replaceVars(query, replacements, spaceSchema(), tableName);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_tags} ON ${schema}.${table} USING gin"
                + " ((jsondata->'properties'->'@ns:com:here:xyz'->'tags') jsonb_ops)";
        query = SQLQuery.replaceVars(query, replacements, spaceSchema(), tableName);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_geo} ON ${schema}.${table} USING gist ((geo))";
        query = SQLQuery.replaceVars(query, replacements, spaceSchema(), tableName);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_serial} ON ${schema}.${table}  USING btree ((i))";
        query = SQLQuery.replaceVars(query, replacements, spaceSchema(), tableName);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_updatedAt} ON ${schema}.${table} USING btree"
                + " ((jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt'), (jsondata->>'id'))";
        query = SQLQuery.replaceVars(query, replacements, spaceSchema(), tableName);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_createdAt} ON ${schema}.${table} USING btree"
                + " ((jsondata->'properties'->'@ns:com:here:xyz'->'createdAt'), (jsondata->>'id'))";
        query = SQLQuery.replaceVars(query, replacements, spaceSchema(), tableName);
        stmt.addBatch(query);

        query = "CREATE INDEX IF NOT EXISTS ${idx_viz} ON ${schema}.${table} USING btree (left(" + " md5(''||i),5))";
        query = SQLQuery.replaceVars(query, replacements, spaceSchema(), tableName);
        stmt.addBatch(query);

        stmt.setQueryTimeout(calculateTimeout());
    }

    protected void ensureHistorySpace(Integer maxVersionCount, boolean compactHistory, boolean isEnableGlobalVersioning)
            throws SQLException {
        final String tableName = spaceTable();
        try (final Connection connection = masterDataSource().getConnection()) {
            advisoryLock(tableName, connection);
            boolean cStateFlag = connection.getAutoCommit();
            try {
                if (cStateFlag) {
                    connection.setAutoCommit(false);
                }

                try (Statement stmt = connection.createStatement()) {
                    /** Create Space-Table */
                    createSpaceStatement(stmt, tableName);

                    String query = "CREATE TABLE IF NOT EXISTS ${schema}.${hsttable} (uuid text NOT NULL, jsondata"
                            + " jsonb, geo geometry(GeometryZ,4326),"
                            + (isEnableGlobalVersioning ? " vid text ," : "")
                            + " CONSTRAINT \""
                            + tableName
                            + "_pkey\" PRIMARY KEY (uuid))";
                    query = SQLQuery.replaceVars(query, spaceSchema(), tableName);
                    stmt.addBatch(query);

                    query = "CREATE INDEX IF NOT EXISTS ${idx_hst_uuid} ON ${schema}.${hsttable} USING btree"
                            + " (uuid)";
                    query = SQLQuery.replaceVars(query, replacements, spaceSchema(), tableName);
                    stmt.addBatch(query);

                    query = "CREATE INDEX IF NOT EXISTS ${idx_hst_id} ON ${schema}.${hsttable}"
                            + " ((jsondata->>'id'))";
                    query = SQLQuery.replaceVars(query, replacements, spaceSchema(), tableName);
                    stmt.addBatch(query);

                    query = "CREATE INDEX IF NOT EXISTS ${idx_hst_updatedAt} ON ${schema}.${hsttable} USING btree"
                            + " ((jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt'))";
                    query = SQLQuery.replaceVars(query, replacements, spaceSchema(), tableName);
                    stmt.addBatch(query);

                    if (isEnableGlobalVersioning) {
                        query = "CREATE INDEX IF NOT EXISTS ${idx_hst_deleted} ON ${schema}.${hsttable} USING btree"
                                + " (((jsondata->'properties'->'@ns:com:here:xyz'->'deleted')::jsonb))";
                        query = SQLQuery.replaceVars(query, replacements, spaceSchema(), tableName);
                        stmt.addBatch(query);

                        query = "CREATE INDEX IF NOT EXISTS ${idx_hst_version} ON ${schema}.${hsttable} USING btree"
                                + " (((jsondata->'properties'->'@ns:com:here:xyz'->'version')::jsonb))";
                        query = SQLQuery.replaceVars(query, replacements, spaceSchema(), tableName);
                        stmt.addBatch(query);

                        query = "CREATE INDEX IF NOT EXISTS ${idx_hst_lastVersion} ON ${schema}.${hsttable} USING"
                                + " btree"
                                + " (((jsondata->'properties'->'@ns:com:here:xyz'->'lastVersion')::jsonb))";
                        query = SQLQuery.replaceVars(query, replacements, spaceSchema(), tableName);
                        stmt.addBatch(query);

                        query = "CREATE INDEX IF NOT EXISTS ${idx_hst_idvsort} ON ${schema}.${hsttable} USING btree"
                                + " ((jsondata ->> 'id'::text),"
                                + " ((jsondata->'properties'->'@ns:com:here:xyz'->'version')::jsonb) DESC )";
                        query = SQLQuery.replaceVars(query, replacements, spaceSchema(), tableName);
                        stmt.addBatch(query);

                        query = "CREATE INDEX IF NOT EXISTS ${idx_hst_vidsort} ON ${schema}.${hsttable} USING btree"
                                + " (((jsondata->'properties'->'@ns:com:here:xyz'->'version')::jsonb) ,"
                                + " (jsondata ->> 'id'::text))";
                        query = SQLQuery.replaceVars(query, replacements, spaceSchema(), tableName);
                        stmt.addBatch(query);

                        query = "CREATE SEQUENCE  IF NOT EXISTS "
                                + spaceSchema()
                                + ".\""
                                + tableName.replaceAll("-", "_")
                                + "_hst_seq\"";
                        query = SQLQuery.replaceVars(query, replacements, spaceSchema(), tableName);
                        stmt.addBatch(query);
                    }

                    if (!isEnableGlobalVersioning) {
                        /** old naming */
                        query = SQLQueryBuilder.deleteHistoryTriggerSQL(spaceSchema(), tableName)[0];
                        stmt.addBatch(query);
                        /** new naming */
                        query = SQLQueryBuilder.deleteHistoryTriggerSQL(spaceSchema(), tableName)[1];
                        stmt.addBatch(query);
                    }

                    query = SQLQueryBuilder.addHistoryTriggerSQL(
                            spaceSchema(), tableName, maxVersionCount, compactHistory, isEnableGlobalVersioning);
                    stmt.addBatch(query);

                    stmt.setQueryTimeout(calculateTimeout());
                    stmt.executeBatch();
                    connection.commit();
                    currentLogger()
                            .debug("Successfully created history table '{}' for space id '{}'", tableName, spaceId());
                }
            } catch (Exception e) {
                throw new SQLException("Creation of history table has failed: " + tableName, e);
            } finally {
                advisoryUnlock(tableName, connection);
                if (cStateFlag) {
                    connection.setAutoCommit(true);
                }
            }
        }
    }

    protected void updateHistoryTrigger(
            Integer maxVersionCount, boolean compactHistory, boolean isEnableGlobalVersioning) throws SQLException {
        final String tableName = spaceTable();

        try (final Connection connection = masterDataSource().getConnection()) {
            advisoryLock(tableName, connection);
            boolean cStateFlag = connection.getAutoCommit();
            try {
                if (cStateFlag) {
                    connection.setAutoCommit(false);
                }

                try (Statement stmt = connection.createStatement()) {
                    /** Create Space-Table */
                    createSpaceStatement(stmt, tableName);

                    /** old naming */
                    String query = SQLQueryBuilder.deleteHistoryTriggerSQL(spaceSchema(), tableName)[0];
                    stmt.addBatch(query);
                    /** new naming */
                    query = SQLQueryBuilder.deleteHistoryTriggerSQL(spaceSchema(), tableName)[1];
                    stmt.addBatch(query);

                    query = SQLQueryBuilder.addHistoryTriggerSQL(
                            spaceSchema(), tableName, maxVersionCount, compactHistory, isEnableGlobalVersioning);
                    stmt.addBatch(query);

                    stmt.setQueryTimeout(calculateTimeout());
                    stmt.executeBatch();
                    connection.commit();
                }
            } catch (Exception e) {
                throw new SQLException("Update of trigger has failed: " + tableName, e);
            } finally {
                advisoryUnlock(tableName, connection);
                if (cStateFlag) {
                    connection.setAutoCommit(true);
                }
            }
        }
    }

    protected void setReplicaIdentity() throws SQLException {
        final String tableName = spaceTable();

        try (final Connection connection = masterDataSource().getConnection()) {
            advisoryLock(tableName, connection);
            boolean cStateFlag = connection.getAutoCommit();
            try {
                if (cStateFlag) {
                    connection.setAutoCommit(false);
                }

                String infoSql = SQLQueryBuilder.getReplicaIdentity(spaceSchema(), tableName),
                        setReplIdSql = SQLQueryBuilder.setReplicaIdentity(spaceSchema(), tableName);

                try (Statement stmt = connection.createStatement();
                        ResultSet rs = stmt.executeQuery(infoSql); ) {
                    if (!rs.next()) {
                        createSpaceStatement(stmt, tableName);
                        /** Create Space-Table */
                        stmt.addBatch(setReplIdSql);
                    } else if (!"f".equals(rs.getString(1)))
                    /** Table exists, but wrong replic identity */
                    {
                        stmt.addBatch(setReplIdSql);
                    } else {
                        return;
                    }
                    /** Table exists with propper replic identity */
                    stmt.setQueryTimeout(calculateTimeout());
                    stmt.executeBatch();
                    connection.commit();
                }
            } catch (Exception e) {
                throw new SQLException("set replica identity to full failed: " + tableName, e);
            } finally {
                advisoryUnlock(tableName, connection);
                if (cStateFlag) {
                    connection.setAutoCommit(true);
                }
            }
        }
    }

    /**
     * #################################### Resultset Handlers ####################################
     */
    /**
     * The default handler for the most results.
     *
     * @param rs the result set.
     * @return the generated feature collection from the result set.
     * @throws SQLException when any unexpected error happened.
     */
    private final long MAX_RESULT_CHARS = 100 * 1024 * 1024;

    protected FeatureCollection _defaultFeatureResultSetHandler(ResultSet rs, boolean skipNullGeom)
            throws SQLException {
        String nextIOffset = "";
        String nextDataset = null;

        StringBuilder sb = new StringBuilder();
        String prefix = "[";
        sb.append(prefix);
        int numFeatures = 0;

        while (rs.next() && MAX_RESULT_CHARS > sb.length()) {
            String geom = rs.getString(2);
            if (skipNullGeom && (geom == null)) {
                continue;
            }
            sb.append(rs.getString(1));
            sb.setLength(sb.length() - 1);
            sb.append(",\"geometry\":");
            sb.append(geom == null ? "null" : geom);
            sb.append("}");
            sb.append(",");

            if (event() instanceof IterateFeaturesEvent) {
                numFeatures++;
                nextIOffset = rs.getString(3);
                if (rs.getMetaData().getColumnCount() >= 4) {
                    nextDataset = rs.getString(4);
                }
            }
        }

        if (sb.length() > prefix.length()) {
            sb.setLength(sb.length() - 1);
        }
        sb.append("]");

        final FeatureCollection featureCollection = new FeatureCollection();
        featureCollection.setLazyParsableFeatureList(sb.toString());

        if (sb.length() > MAX_RESULT_CHARS) {
            throw new SQLException(DhString.format("Maxchar limit(%d) reached", MAX_RESULT_CHARS));
        }

        if (event() instanceof IterateFeaturesEvent
                && numFeatures > 0
                && numFeatures == ((SearchForFeaturesEvent) event()).getLimit()) {
            String nextHandle = (nextDataset != null ? nextDataset + "_" : "") + nextIOffset;
            featureCollection.setHandle(nextHandle);
            featureCollection.setNextPageToken(nextHandle);
        }

        return featureCollection;
    }

    public FeatureCollection defaultFeatureResultSetHandler(ResultSet rs) throws SQLException {
        return _defaultFeatureResultSetHandler(rs, false);
    }

    protected FeatureCollection defaultFeatureResultSetHandlerSkipIfGeomIsNull(ResultSet rs) throws SQLException {
        return _defaultFeatureResultSetHandler(rs, true);
    }

    protected @NotNull BinaryResponse defaultBinaryResultSetHandler(ResultSet rs) throws SQLException {
        if (rs.next()) {
            final byte[] bytes = rs.getBytes(1);
            return new BinaryResponse(bytes, APPLICATION_VND_MAPBOX_VECTOR_TILE);
        }
        throw new SQLException("No result");
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
        long limit = ((IterateHistoryEvent) event()).getLimit();
        String id = "";

        CompactChangeset cc = new CompactChangeset();

        List<Feature> inserts = new ArrayList<>();
        List<Feature> updates = new ArrayList<>();
        List<Feature> deletes = new ArrayList<>();

        while (rs.next()) {
            Feature feature;
            String operation = rs.getString("Operation");
            try {
                feature = new ObjectMapper().readValue(rs.getString("Feature"), Feature.class);
            } catch (JsonProcessingException e) {
                currentLogger().error("Error in compactHistoryResultSetHandler for space id '{}': {}", spaceId(), e);
                throw new SQLException("Cant read json from database!");
            }

            switch (operation) {
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
        long limit = ((IterateHistoryEvent) event()).getLimit();
        String npt = ((IterateHistoryEvent) event()).getPageToken();

        ChangesetCollection ccol = new ChangesetCollection();
        Map<Integer, Changeset> versions = new HashMap<>();
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

            if (!wroteStart) {
                startVersion = version;
                wroteStart = true;
            }

            if (lastVersion != null && version > lastVersion) {
                Changeset cs = new Changeset()
                        .withInserted(new FeatureCollection().withFeatures(inserts))
                        .withUpdated(new FeatureCollection().withFeatures(updates))
                        .withDeleted(new FeatureCollection().withFeatures(deletes));
                versions.put(lastVersion, cs);
                inserts = new ArrayList<>();
                updates = new ArrayList<>();
                deletes = new ArrayList<>();
            }

            try {
                feature = new ObjectMapper().readValue(rs.getString("Feature"), Feature.class);
            } catch (JsonProcessingException e) {
                currentLogger().error("Error in historyResultSetHandler for space id '{}': {}", spaceId(), e);
                throw new SQLException("Cant read json from database!");
            }

            switch (operation) {
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

        if (wroteStart) {
            Changeset cs = new Changeset()
                    .withInserted(new FeatureCollection().withFeatures(inserts))
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
        String id = "";

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
        featureCollection.setLazyParsableFeatureList(sb.toString());

        if (MAX_RESULT_CHARS <= sb.length()) {
            throw new SQLException(DhString.format("Maxchar limit(%d) reached", MAX_RESULT_CHARS));
        }

        if (numFeatures > 0 && numFeatures == ((IterateFeaturesEvent) event()).getLimit()) {
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
        featureCollection.setLazyParsableFeatureList(sb.toString());
        return featureCollection;
    }

    protected int calculateTimeout() throws SQLException {
        final long remainingSeconds = event.remaining(TimeUnit.SECONDS);
        if (!isRemainingTimeSufficient(remainingSeconds)) {
            throw new SQLException("No time left to execute query.", "54000");
        }
        final long timeout =
                remainingSeconds >= STATEMENT_TIMEOUT_SECONDS ? STATEMENT_TIMEOUT_SECONDS : (remainingSeconds - 2);
        logger.debug("{}:{} - New timeout for query set to '{}'", timeout);
        return (int) timeout;
    }

    protected boolean isRemainingTimeSufficient(long remainingSeconds) {
        if (remainingSeconds <= MIN_REMAINING_TIME_FOR_RETRY_SECONDS) {
            logger.warn("{}:{} - No time left to execute query '{}' s", remainingSeconds);
            return false;
        }
        return true;
    }

    /**
     * The result handler for a getHistoryStatisticsEvent.
     *
     * @param rs the result set.
     * @return the feature collection generated from the result.
     */
    protected XyzResponse getHistoryStatisticsResultSetHandler(ResultSet rs) {
        try {
            rs.next();
            StatisticsResponse.Value<Long> tablesize =
                    JsonSerializable.deserialize(rs.getString("tablesize"), new TypeReference<Value<Long>>() {});
            StatisticsResponse.Value<Long> count = JsonSerializable.deserialize(
                    rs.getString("count"), new TypeReference<StatisticsResponse.Value<Long>>() {});
            StatisticsResponse.Value<Integer> maxversion = JsonSerializable.deserialize(
                    rs.getString("maxversion"), new TypeReference<StatisticsResponse.Value<Integer>>() {});
            StatisticsResponse.Value<Integer> minversion = JsonSerializable.deserialize(
                    rs.getString("minversion"), new TypeReference<StatisticsResponse.Value<Integer>>() {});

            return new HistoryStatisticsResponse()
                    .withByteSize(tablesize)
                    .withDataSize(tablesize)
                    .withCount(count)
                    .withMinVersion(minversion)
                    .withMaxVersion(maxversion);
        } catch (Exception e) {
            return new ErrorResponse()
                    .withStreamId(streamId())
                    .withError(XyzError.EXCEPTION)
                    .withErrorMessage(e.getMessage());
        }
    }

    /**
     * The result handler for a getStatistics event.
     *
     * @param rs the result set.
     * @return the feature collection generated from the result.
     */
    protected XyzResponse getStatisticsResultSetHandler(ResultSet rs) {
        try {
            rs.next();

            StatisticsResponse.Value<Long> tablesize = JsonSerializable.deserialize(
                    rs.getString("tablesize"), new TypeReference<StatisticsResponse.Value<Long>>() {});
            StatisticsResponse.Value<List<String>> geometryTypes = JsonSerializable.deserialize(
                    rs.getString("geometryTypes"), new TypeReference<StatisticsResponse.Value<List<String>>>() {});
            StatisticsResponse.Value<List<StatisticsResponse.PropertyStatistics>> tags = JsonSerializable.deserialize(
                    rs.getString("tags"),
                    new TypeReference<StatisticsResponse.Value<List<StatisticsResponse.PropertyStatistics>>>() {});
            StatisticsResponse.PropertiesStatistics properties = JsonSerializable.deserialize(
                    rs.getString("properties"), StatisticsResponse.PropertiesStatistics.class);
            StatisticsResponse.Value<Long> count = JsonSerializable.deserialize(
                    rs.getString("count"), new TypeReference<StatisticsResponse.Value<Long>>() {});
            Map<String, Object> bboxMap =
                    JsonSerializable.deserialize(rs.getString("bbox"), new TypeReference<Map<String, Object>>() {});

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
                        Math.max(-90, Math.min(90, Double.parseDouble(matcher.group(4)))));
            }

            return new StatisticsResponse()
                    .withBBox(new StatisticsResponse.Value<BBox>()
                            .withValue(bbox)
                            .withEstimated(bboxMap.get("estimated") == Boolean.TRUE))
                    .withByteSize(tablesize)
                    .withDataSize(tablesize)
                    .withCount(count)
                    .withGeometryTypes(geometryTypes)
                    .withTags(tags)
                    .withProperties(properties);
        } catch (Exception e) {
            return new ErrorResponse()
                    .withStreamId(streamId())
                    .withError(XyzError.EXCEPTION)
                    .withErrorMessage(e.getMessage());
        }
    }

    public static class XyzConnectionCustomizer
            extends AbstractConnectionCustomizer { // handle initialization per db connection

        private String getSchema(String parentDataSourceIdentityToken) {
            return (String) extensionsForToken(parentDataSourceIdentityToken).get(C3P0EXT_CONFIG_SCHEMA);
        }

        public void onAcquire(Connection c, String pdsIdt) {
            String schema = getSchema(pdsIdt); // config.schema();
            org.apache.commons.dbutils.QueryRunner runner = new QueryRunner();
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
