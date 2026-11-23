/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.jobs.steps.impl;

import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.READER;
import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.WRITER;
import static com.here.xyz.jobs.steps.execution.db.Database.loadDatabase;
import static com.here.xyz.models.hub.Space.LAZY_TABLE;
import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static com.here.xyz.util.db.pg.LazySpaceTables.TEMPLATE_TABLE;
import static com.here.xyz.util.db.pg.LazySpaceTables.buildCheckLevelsQuery;
import static com.here.xyz.util.db.pg.LazySpaceTables.checkLevelsHandler;
import static com.here.xyz.util.db.pg.LazySpaceTables.requireTemplate;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.getTableNameFromSpaceParamsOrSpaceId;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.execution.StepException;
import com.here.xyz.jobs.steps.execution.db.Database;
import com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole;
import com.here.xyz.jobs.steps.execution.db.DatabaseBasedStep;
import com.here.xyz.jobs.steps.impl.maintenance.MarkForMaintenance;
import com.here.xyz.jobs.steps.impl.maintenance.SpawnMaintenanceJobs;
import com.here.xyz.jobs.steps.impl.transport.CopySpace;
import com.here.xyz.jobs.steps.impl.transport.CopySpacePost;
import com.here.xyz.jobs.steps.impl.transport.CopySpacePre;
import com.here.xyz.jobs.steps.impl.transport.CountSpace;
import com.here.xyz.jobs.steps.impl.transport.ExportChangedTiles;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.jobs.steps.impl.transport.GetNextSpaceVersion;
import com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.IndexHelper.SystemIndex;
import com.here.xyz.util.db.pg.LazySpaceTables;
import com.here.xyz.util.db.pg.LazySpaceTables.Level;
import com.here.xyz.util.db.pg.LazySpaceTables.LevelCheck;
import com.here.xyz.util.db.pg.LazySpaceTables.LevelState;
import com.here.xyz.util.db.pg.LazySpaceTables.MaterializationResult;
import com.here.xyz.util.db.pg.LazySpaceTables.Profile;
import com.here.xyz.util.runtime.FunctionRuntime;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.ErrorResponseException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@JsonSubTypes({
    @JsonSubTypes.Type(value = CreateIndex.class),
    @JsonSubTypes.Type(value = ExportSpaceToFiles.class),
    @JsonSubTypes.Type(value = ExportChangedTiles.class),
    @JsonSubTypes.Type(value = TaskedImportFilesToSpace.class),
    @JsonSubTypes.Type(value = DropIndexes.class),
    @JsonSubTypes.Type(value = AnalyzeSpaceTable.class),
    @JsonSubTypes.Type(value = MarkForMaintenance.class),
    @JsonSubTypes.Type(value = GetNextSpaceVersion.class),
    @JsonSubTypes.Type(value = CopySpace.class),
    @JsonSubTypes.Type(value = CopySpacePre.class),
    @JsonSubTypes.Type(value = CopySpacePost.class),
    @JsonSubTypes.Type(value = CountSpace.class),
    @JsonSubTypes.Type(value = SpawnMaintenanceJobs.class)
})
public abstract class SpaceBasedStep<T extends SpaceBasedStep> extends DatabaseBasedStep<T> {
  private static final Logger logger = LogManager.getLogger();
  private static final long MAX_MATERIALIZATION_BUDGET_MILLIS = 120_000;
  private static final long MATERIALIZATION_RESERVE_MILLIS = 10_000;
  private static final String NO_WRITER_DATABASE = "XX000";
  private static final String QUERY_CANCELED = "57014";
  private static final String LOCK_NOT_AVAILABLE = "55P03";
  private static final String CONNECTION_EXCEPTION_CLASS = "08";

  @JsonView({Internal.class, Static.class})
  private String spaceId;
  @JsonView({Internal.class, Static.class})
  private Ref versionRef;

  /**
   * Persisted snapshots of all spaces resolved by this step. Keeping these snapshots in the serialized step prevents an asynchronous or
   * resumed execution from switching to a different physical table when the same logical space ID is recreated in the meantime.
   */
  @JsonView({Internal.class, Static.class})
  private Map<String, Space> cachedSpaces = new ConcurrentHashMap<>();

  @JsonIgnore
  protected Space superSpace;

  @JsonIgnore
  protected StatisticsResponse spaceStatistics;

  /**
   * The lazy tables this invocation found or created, as "storageId/schema.table". Never persisted, so a resumed or later
   * invocation checks again.
   */
  @JsonIgnore
  private transient Set<String> existingLazyTables = ConcurrentHashMap.newKeySet();

  /**
   * The spaces whose lazyTable flag this invocation asked the Hub to clear. Never persisted.
   */
  @JsonIgnore
  private transient Set<String> lazyFlagClearRequested = ConcurrentHashMap.newKeySet();

  /**
   * The state of the own root table of a space, as seen by a step.
   */
  public enum TableState {
    /**
     * The space itself has no lazyTable flag.
     */
    NOT_LAZY,
    /**
     * The space carries lazyTable and its table exists.
     */
    EXISTS,
    /**
     * No table on a space that still carries lazyTable: never written, or lost while its flag was not cleared yet;
     * the level reads as the template and the next write materializes it.
     */
    NEVER_MATERIALIZED
  }

  @FunctionalInterface
  private interface LazyTableCall<R> {
    R call() throws SQLException, TooManyResourcesClaimed, WebClientException;
  }

  /**
   * The lazy levels of a space's chain with their states.
   *
   * @param schema The connector schema of the space's writer database, null if there is no lazy level
   */
  private record LazyChain(List<Level> levels, List<LevelState> states, boolean templatePresent, String schema) {
    LevelState stateAt(int depth) {
      for (int i = 0; i < levels.size(); i++)
        if (levels.get(i).depth() == depth)
          return states.get(i);
      return null;
    }

    List<Level> neverMaterialized() {
      List<Level> missing = new ArrayList<>();
      for (int i = 0; i < levels.size(); i++)
        if (states.get(i) == LevelState.NEVER_MATERIALIZED)
          missing.add(levels.get(i));
      return missing;
    }

    void requireTemplateFor(List<Level> substituted) throws SQLException {
      Set<Integer> depths = new TreeSet<>();
      substituted.forEach(level -> depths.add(level.depth()));
      requireTemplate(schema, new LevelCheck(states, templatePresent), depths);
    }
  }

  public void setSpaceId(String spaceId) {
    this.spaceId = spaceId;
    setOutputSetGroup(spaceId);
  }

  public String getSpaceId() {
    return spaceId;
  }

  public T withSpaceId(String spaceId) {
    setSpaceId(spaceId);
    setOutputSetGroup(spaceId);
    return (T) this;
  }

  public Ref getVersionRef() {
    return versionRef;
  }

  public void setVersionRef(Ref versionRef) {
    this.versionRef = versionRef;
  }

  public T withVersionRef(Ref versionRef) {
    setVersionRef(versionRef);
    return (T) this;
  }

  @Override
  public String getOutputSetGroup() {
    if (super.getOutputSetGroup() == null || super.getOutputSetGroup().isEmpty()) {
      return getSpaceId();
    } else {
      return super.getOutputSetGroup();
    }
  }

  protected final String getRootTableName(Space space) throws WebClientException {
    return getTableNameFromSpaceParamsOrSpaceId(space.getStorage().getParams(), space.getId(),
        ConnectorParameters.fromMap(loadConnector(space).params).isEnableHashedSpaceId());
  }

  protected final boolean isEnableHashedSpaceIdActivated(Space space) throws WebClientException {
    return ConnectorParameters.fromMap(loadConnector(space).params).isEnableHashedSpaceId();
  }

  protected Connector loadConnector(Space space) throws WebClientException {
    try {
      return hubWebClient().loadConnector(space.getStorage().getId());
    }
    catch (ErrorResponseException e) {
      return handleErrorResponse(e);
    }
  }

  protected void validateSpaceExists() throws ValidationException {
    try {
      //Check if the space is actually existing
      if (getSpaceId() == null)
        throw new ValidationException("SpaceId is missing!");
      resolveSpaceParams(space());
    }
    catch (WebClientException e) {
      throw new ValidationException("Error loading resource " + getSpaceId(), e);
    }
  }

  private Space loadSpace(String spaceId, boolean skipLocalCache) throws WebClientException {
    try {
      logger.info("[{}] Loading space config for space {} ...", getGlobalStepId(), spaceId);
      return hubWebClient().loadSpace(spaceId, skipLocalCache);
    }
    catch (ErrorResponseException e) {
      return handleErrorResponse(e);
    }
  }

  protected StatisticsResponse loadSpaceStatistics(String spaceId, SpaceContext context) throws WebClientException {
    return loadSpaceStatistics(spaceId, context, false, true);
  }

  protected StatisticsResponse loadSpaceStatistics(String spaceId, SpaceContext context, boolean fastMode) throws WebClientException {
    return loadSpaceStatistics(spaceId, context, false, fastMode);
  }

  protected StatisticsResponse loadSpaceStatistics(String spaceId, SpaceContext context, boolean skipCache, boolean fastMode) throws WebClientException {
    try {
      logger.info("[{}] Loading statistics for space {} ...", getGlobalStepId(), getSpaceId());
      return hubWebClient().loadSpaceStatistics(spaceId, context, skipCache, fastMode);
    }
    catch (ErrorResponseException e) {
      return handleErrorResponse(e);
    }
  }

  protected Tag loadTag(String spaceId, String tagId) throws WebClientException {
    try {
      return hubWebClient().loadTag(spaceId, tagId);
    }
    catch (ErrorResponseException e) {
      return handleErrorResponse(e);
    }
  }

  protected HubWebClient hubWebClient() {
    return HubWebClient.getInstance(Config.instance.HUB_ENDPOINT);
  }

  /**
   * Provides the {@link Database} instance for the pre-defined space ID of this step. See: {@link #getSpaceId()}
   * The loading calls are cached; that means that later calls will not induce an actual REST request to Hub.
   * Also, the Database objects are cached. See: {@link Database#loadDatabase(String, DatabaseRole)}
   * @return The Database for the pre-defined space of this step and for the provided role
   * @throws WebClientException
   */
  protected Database db(DatabaseRole role) throws WebClientException {
    return loadDatabase(space().getStorage().getId(), role);
  }

  /**
   * Provides the default {@link Database} instance (WRITER) for the pre-defined space ID of this step. See: {@link #getSpaceId()}
   * The loading calls are cached; that means that later calls will not induce an actual REST request to Hub.
   * Also, the Database objects are cached. See: {@link Database#loadDatabase(String, DatabaseRole)}
   * @return The WRITER Database for the pre-defined space of this step
   * @throws WebClientException
   */
  protected Database db() throws WebClientException {
    return db(WRITER);
  }

  /**
   * Provides the READER {@link Database} instance for the pre-defined space ID of this step. See: {@link #getSpaceId()}
   * The loading calls are cached; that means that later calls will not induce an actual REST request to Hub.
   * Also, the Database objects are cached. See: {@link Database#loadDatabase(String, DatabaseRole)}
   * If no READER Database is found, the writer will be returned.
   * @return The READER Database for the pre-defined space of this step, or the WRITER Database if there is no READER
   * @throws WebClientException
   */
  protected Database dbReader() throws WebClientException {
    try {
      return db(READER);
    }
    catch (RuntimeException rt) {
      //TODO: Ensure that we always have a reader for all Databases and then - if there is none - it would be for a good reason, so we should not ignore that exception anymore
      if (!(rt.getCause() instanceof NoSuchElementException))
        throw rt;
    }

    return db(WRITER);
  }

  protected Database dbWriter() throws WebClientException {
    return db(WRITER);
  }

  /**
   * Provides the space instance for the provided space ID.
   * The loading calls are cached; that means that later calls will not induce an actual REST request to Hub.
   * @param spaceId
   * @return
   * @throws WebClientException
   */
  protected Space space(String spaceId) throws WebClientException {
    Space space = cachedSpaces.get(spaceId);
    if (space == null)
      cachedSpaces.put(spaceId, space = loadSpace(spaceId, true));
    return space;
  }

  /**
   * Provides the space instance for the pre-defined space ID of this step. See: {@link #getSpaceId()}
   * The loading calls are cached; that means that later calls will not induce an actual REST request to Hub.
   * @return
   * @throws WebClientException
   */
  protected Space space() throws WebClientException {
    return space(getSpaceId());
  }

  protected Space space(boolean skipLocalCache) throws WebClientException {
    return loadSpace(spaceId, skipLocalCache);
  }

  protected StatisticsResponse spaceStatistics(SpaceContext context, boolean skipCache) throws WebClientException {
    if (spaceStatistics == null)
      spaceStatistics = loadSpaceStatistics(getSpaceId(), context, skipCache, true);
    return spaceStatistics;
  }

  protected Space superSpace() throws WebClientException {
    return resolveExtendedSpace(space());
  }

  /**
   * Builds the storage parameters for connector-side queries against the provided space.
   *
   * <p>The current space's storage parameters contain its persisted physical table name. Composite parameters are
   * merged on top and carry the physical table names of the extended spaces. Legacy spaces without explicit table
   * names remain unchanged and are resolved by the connector through {@code enableHashedSpaceId}.</p>
   */
  protected final Map<String, Object> resolveSpaceParams(Space targetSpace) throws WebClientException {
    Map<String, Object> params = targetSpace.getStorage().getParams() == null
        ? new HashMap<>()
        : new HashMap<>(targetSpace.getStorage().getParams());

    Space extendedSpace = resolveExtendedSpace(targetSpace);
    if (extendedSpace != null)
      params.putAll(targetSpace.resolveCompositeParams(extendedSpace));

    return params;
  }

  private Space resolveExtendedSpace(Space targetSpace) throws WebClientException {
    if (targetSpace.getExtension() == null)
      return null;

    Space extendedSpace = space(targetSpace.getExtension().getSpaceId());
    if (extendedSpace.getExtension() != null)
      extendedSpace.getExtension().resolvedSpace = space(extendedSpace.getExtension().getSpaceId());
    return extendedSpace;
  }

  /**
   * Whether this step's space extends a space which is itself a composite space, i.e. whether it is a composite space with more than
   * one extension level. See {@link #superSpace()} for why that matters.
   */
  protected boolean isNestedComposite() throws WebClientException {
    Space superSpace = superSpace();
    return superSpace != null && superSpace.getExtension() != null;
  }

  /*
  Lazy space tables: a space with the lazyTable flag gets its table set with its first write; until then, readers use the
  empty-space template of the connector schema. The following helpers work on the database of the space they get, never on
  the one of this step's own space (db(), dbWriter(), dbReader()), and load configs only through hubWebClient() directly, so
  that a Hub error surfaces as a WebClientException. A level with another storage ID than its composite is not lazy.
   */

  /**
   * @return Whether the space itself carries the lazyTable flag; no I/O
   */
  protected final boolean isLazy(Space space) {
    return space != null && space.getStorage() != null && Space.isLazyTable(space.getStorage().getParams());
  }

  /**
   * The state of the space's own root table: NOT_LAZY if the space itself has no lazyTable flag.
   * The same check covers every lazy level of the chain (bases with another storage ID are not lazy), so the readable
   * helpers find them in the cache. A never-materialized or materialized lazy base does not change the result.
   */
  protected final TableState tableState(Space space) throws SQLException, TooManyResourcesClaimed, WebClientException {
    LazyChain chain = checkChain(space);
    if (!isLazy(space))
      return TableState.NOT_LAZY;
    return chain.stateAt(0) == LevelState.EXISTS ? TableState.EXISTS : TableState.NEVER_MATERIALIZED;
  }

  /**
   * @return The table to read the space from: its physical table, or the empty-space template if the space is lazy and was never
   *  materialized
   * @throws com.here.xyz.util.db.pg.TemplateUnavailableException If the template is needed but missing
   */
  protected final String getReadableRootTableName(Space space) throws SQLException, TooManyResourcesClaimed, WebClientException {
    return getReadableRootTableName(space, space);
  }

  /**
   * @param space The space whose table is read
   * @param reader The space on whose database the query runs. If its storage ID differs from the one of the space, the
   *  physical name is returned without a check, so a query fails for a missing table as for an eager space.
   * @return See {@link #getReadableRootTableName(Space)}
   */
  protected final String getReadableRootTableName(Space space, Space reader) throws SQLException, TooManyResourcesClaimed,
      WebClientException {
    if (!sameStorage(space, reader) || !isLazy(space))
      return physicalName(space);
    LazyChain chain = checkChain(space);
    if (chain.stateAt(0) == LevelState.EXISTS)
      return chain.levels().get(0).table();
    chain.requireTemplateFor(List.of(chain.levels().get(0)));
    return TEMPLATE_TABLE;
  }

  /**
   * @return The storage params of {@link #resolveSpaceParams(Space)}, in which every never-materialized lazy level reads from the
   *  empty-space template
   * @throws com.here.xyz.util.db.pg.TemplateUnavailableException If the template is needed but missing
   */
  protected final Map<String, Object> resolveReadableSpaceParams(Space space) throws SQLException, TooManyResourcesClaimed,
      WebClientException {
    return resolveReadableSpaceParams(space, space);
  }

  /**
   * @param space The space whose params are built
   * @param reader The space on whose database the query runs. If its storage ID differs from the one of the space, the params
   *  are returned with every lazyTable flag removed and without a check.
   * @return See {@link #resolveReadableSpaceParams(Space)}
   */
  protected final Map<String, Object> resolveReadableSpaceParams(Space space, Space reader) throws SQLException,
      TooManyResourcesClaimed, WebClientException {
    snapshotChain(space);
    Map<String, Object> params = resolveSpaceParams(space);
    if (!sameStorage(space, reader))
      return withoutLazyTableFlags(params);
    if (!LazySpaceTables.hasLazyLevel(params))
      return params;
    LazyChain chain = checkChain(space);
    List<Level> missing = chain.neverMaterialized();
    if (missing.isEmpty())
      return params;
    chain.requireTemplateFor(missing);
    return LazySpaceTables.substitute(params, missing);
  }

  /**
   * Creates the tables of a lazy space if they do not exist, on the space's own writer database. It needs no template.
   * The on-demand indexes of the FULL profile come from fresh configs, loaded only if the table is missing.
   *
   * @return NOT_LAZY without any Hub or database access for a space without the lazyTable flag, otherwise EXISTED or CREATED
   * @throws SQLException Also a {@link com.here.xyz.util.db.pg.MaterializationTimeoutException} or a
   *  {@link com.here.xyz.util.db.pg.ReservedTableNameException}
   */
  protected final MaterializationResult materializeTables(Space space, Profile profile) throws SQLException, TooManyResourcesClaimed,
      WebClientException {
    return materializeTables(space, profile, List.of());
  }

  /**
   * @param bareSystemIndexes The system indexes the BARE profile creates; FULL ignores them
   * @see #materializeTables(Space, Profile)
   */
  protected final MaterializationResult materializeTables(Space space, Profile profile, List<SystemIndex> bareSystemIndexes)
      throws SQLException, TooManyResourcesClaimed, WebClientException {
    if (!isLazy(space))
      return MaterializationResult.NOT_LAZY;

    snapshotChain(space);
    String table = physicalName(space);
    Database db = writerOf(space);
    DataSourceProvider dataSources = requestResource(db, 0);
    String schema = getSchema(db);
    String cacheKey = cacheKey(space, schema, table);

    if (existingLazyTables.contains(cacheKey))
      return MaterializationResult.EXISTED;
    List<Level> levels = List.of(new Level(0, space.getId(), table));
    if (runReadQuerySync(buildCheckLevelsQuery(schema, levels), db, 0, checkLevelsHandler(levels)).states().get(0)
        == LevelState.EXISTS) {
      existingLazyTables.add(cacheKey);
      return MaterializationResult.EXISTED;
    }

    List<String> onDemandIndexes = profile == Profile.FULL ? freshOnDemandIndexes(space) : List.of();
    MaterializationResult result = LazySpaceTables.materialize(dataSources, schema, table, space.getId(), profile, onDemandIndexes,
        bareSystemIndexes, materializationBudgetMillis(FunctionRuntime.getInstance()));
    existingLazyTables.add(cacheKey);
    return result;
  }

  /**
   * Maps an SQL error of the lazy table helpers: a timeout, a lock wait or a connection error can be retried, everything else
   * (e.g. a missing template, a reserved table name or a connector without a writer database) cannot.
   */
  protected final StepException toStepException(SQLException e) {
    String sqlState = e.getSQLState();
    boolean retryable = QUERY_CANCELED.equals(sqlState) || LOCK_NOT_AVAILABLE.equals(sqlState)
        || sqlState != null && sqlState.startsWith(CONNECTION_EXCEPTION_CLASS);
    return new StepException("Lazy table handling of space " + getSpaceId() + " failed: " + e.getMessage(), e)
        .withRetryable(retryable);
  }

  /**
   * {@link #materializeTables(Space, Profile)} with the error mapping of the steps: SQL errors through
   * {@link #toStepException(SQLException)}, Hub errors through {@link #handleErrorResponse(ErrorResponseException)}.
   */
  protected final MaterializationResult materializeTablesOrFail(Space space, Profile profile) throws WebClientException,
      TooManyResourcesClaimed {
    return orFail(() -> materializeTables(space, profile));
  }

  /**
   * @see #materializeTablesOrFail(Space, Profile)
   * @see #materializeTables(Space, Profile, List)
   */
  protected final MaterializationResult materializeTablesOrFail(Space space, Profile profile, List<SystemIndex> bareSystemIndexes)
      throws WebClientException, TooManyResourcesClaimed {
    return orFail(() -> materializeTables(space, profile, bareSystemIndexes));
  }

  /**
   * {@link #tableState(Space)} with the error mapping of {@link #materializeTablesOrFail(Space, Profile)}.
   */
  protected final TableState tableStateOrFail(Space space) throws WebClientException, TooManyResourcesClaimed {
    return orFail(() -> tableState(space));
  }

  /**
   * {@link #getReadableRootTableName(Space)} with the error mapping of {@link #materializeTablesOrFail(Space, Profile)}.
   */
  protected final String getReadableRootTableNameOrFail(Space space) throws WebClientException, TooManyResourcesClaimed {
    return orFail(() -> getReadableRootTableName(space));
  }

  /**
   * {@link #getReadableRootTableName(Space, Space)} with the error mapping of {@link #materializeTablesOrFail(Space, Profile)}.
   */
  protected final String getReadableRootTableNameOrFail(Space space, Space reader) throws WebClientException, TooManyResourcesClaimed {
    return orFail(() -> getReadableRootTableName(space, reader));
  }

  /**
   * {@link #resolveReadableSpaceParams(Space)} with the error mapping of {@link #materializeTablesOrFail(Space, Profile)}.
   */
  protected final Map<String, Object> resolveReadableSpaceParamsOrFail(Space space) throws WebClientException,
      TooManyResourcesClaimed {
    return orFail(() -> resolveReadableSpaceParams(space));
  }

  /**
   * {@link #resolveReadableSpaceParams(Space, Space)} with the error mapping of {@link #materializeTablesOrFail(Space, Profile)}.
   */
  protected final Map<String, Object> resolveReadableSpaceParamsOrFail(Space space, Space reader) throws WebClientException,
      TooManyResourcesClaimed {
    return orFail(() -> resolveReadableSpaceParams(space, reader));
  }

  /**
   * Asks the Hub to clear lazyTable of a space this step has written. Never throws. Call only after the step took a version
   * from the space's version sequence or committed a write to it. The step keeps its snapshot with the flag.
   */
  protected final void clearLazyTableFlag(Space space) {
    if (space == null || !isLazy(space) || !lazyFlagClearRequested.add(space.getId()))
      return;
    //null for a space without an explicit table name
    String table = (String) space.getStorage().getParams().get(TABLE_NAME);
    try {
      hubWebClient().clearLazyTable(space.getId(), space.getStorage().getId(), table);
      logger.info("lazyTable clear requested space={} connector={}", space.getId(), space.getStorage().getId());
    }
    catch (WebClientException | RuntimeException e) {
      logger.warn("lazyTable not cleared space={}: {}", space.getId(), e.getMessage());
    }
  }

  private <R> R orFail(LazyTableCall<R> call) throws WebClientException, TooManyResourcesClaimed {
    try {
      return call.call();
    }
    catch (SQLException e) {
      throw toStepException(e);
    }
    catch (ErrorResponseException e) {
      return handleErrorResponse(e);
    }
  }

  /**
   * Checks the lazy levels of the space's chain that are not known to exist, with one statement on the space's writer.
   */
  private LazyChain checkChain(Space space) throws SQLException, TooManyResourcesClaimed, WebClientException {
    snapshotChain(space);
    List<Level> levels = new ArrayList<>();
    for (Level level : LazySpaceTables.lazyLevels(resolveSpaceParams(space))) {
      Space levelSpace = levelSpace(space, level.depth());
      levels.add(new Level(level.depth(), levelSpace.getId(), level.table() != null ? level.table() : physicalName(levelSpace)));
    }
    if (levels.isEmpty())
      return new LazyChain(List.of(), List.of(), false, null);

    Database db = writerOf(space);
    String schema = getSchema(db);
    List<Level> unknown = levels.stream()
        .filter(level -> !existingLazyTables.contains(cacheKey(space, schema, level.table())))
        .toList();
    LevelCheck check = unknown.isEmpty() ? null : runReadQuerySync(buildCheckLevelsQuery(schema, unknown), db, 0,
        checkLevelsHandler(unknown));

    List<LevelState> states = new ArrayList<>();
    for (Level level : levels) {
      int index = unknown.indexOf(level);
      LevelState state = index < 0 ? LevelState.EXISTS : check.states().get(index);
      if (state == LevelState.EXISTS)
        existingLazyTables.add(cacheKey(space, schema, level.table()));
      states.add(state);
    }
    //Without a check, every level exists and nothing is read from the template
    return new LazyChain(levels, List.copyOf(states), check != null && check.templatePresent(), schema);
  }

  private Database writerOf(Space space) throws WebClientException, SQLException {
    try {
      return loadDatabase(space.getStorage().getId(), WRITER);
    }
    catch (RuntimeException e) { //Database.loadDatabase wraps Hub errors and a missing database
      if (e.getCause() instanceof WebClientException webClientException)
        throw webClientException;
      throw new SQLException("No writer database for connector " + space.getStorage().getId(), NO_WRITER_DATABASE, e);
    }
  }

  /**
   * The persisted snapshot of a space, loaded without the error mapping of {@link #space(String)}.
   */
  private Space snapshot(String spaceId) throws WebClientException {
    Space space = cachedSpaces.get(spaceId);
    if (space == null)
      cachedSpaces.put(spaceId, space = hubWebClient().loadSpace(spaceId, true));
    return space;
  }

  /**
   * Loads the snapshots of the spaces the space extends, so that {@link #resolveSpaceParams(Space)} only hits the cache.
   */
  private void snapshotChain(Space space) throws WebClientException {
    if (space.getExtension() == null)
      return;
    Space extendedSpace = snapshot(space.getExtension().getSpaceId());
    if (extendedSpace.getExtension() != null)
      snapshot(extendedSpace.getExtension().getSpaceId());
  }

  private Space levelSpace(Space space, int depth) throws WebClientException {
    Space level = space;
    for (int i = 0; i < depth; i++)
      level = snapshot(level.getExtension().getSpaceId());
    return level;
  }

  private String physicalName(Space space) throws WebClientException {
    return getTableNameFromSpaceParamsOrSpaceId(space.getStorage().getParams(), space.getId(),
        ConnectorParameters.fromMap(hubWebClient().loadConnector(space.getStorage().getId()).params).isEnableHashedSpaceId());
  }

  /**
   * The effective searchable properties from fresh configs of the space and, for an extension, of its bases.
   */
  private List<String> freshOnDemandIndexes(Space space) throws WebClientException {
    Space freshSpace = hubWebClient().loadSpace(space.getId(), true);
    Space extendedSpace = null;
    if (freshSpace.getExtension() != null) {
      extendedSpace = hubWebClient().loadSpace(freshSpace.getExtension().getSpaceId(), true);
      if (extendedSpace.getExtension() != null)
        extendedSpace.getExtension().resolvedSpace = hubWebClient().loadSpace(extendedSpace.getExtension().getSpaceId(), true);
    }
    Map<String, Boolean> searchableProperties = freshSpace.getEffectiveSearchableProperties(extendedSpace);
    if (searchableProperties == null) {
      logger.warn("[{}] The root base of space {} is not resolved, materializing it without on-demand indexes", getGlobalStepId(),
          space.getId());
      return List.of();
    }
    return searchableProperties.entrySet().stream()
        .filter(property -> Boolean.TRUE.equals(property.getValue()))
        .map(Map.Entry::getKey)
        .toList();
  }

  static long materializationBudgetMillis(FunctionRuntime runtime) {
    long remaining = runtime == null ? Integer.MAX_VALUE : runtime.getRemainingTime();
    return Math.min(MAX_MATERIALIZATION_BUDGET_MILLIS, remaining - MATERIALIZATION_RESERVE_MILLIS);
  }

  private static boolean sameStorage(Space space, Space reader) {
    return Objects.equals(space.getStorage().getId(), reader.getStorage().getId());
  }

  private static String cacheKey(Space space, String schema, String table) {
    return space.getStorage().getId() + "/" + schema + "." + table;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> withoutLazyTableFlags(Map<String, Object> params) {
    Map<String, Object> copy = new LinkedHashMap<>();
    params.forEach((key, value) -> {
      if (!LAZY_TABLE.equals(key))
        copy.put(key, value instanceof Map<?, ?> map ? withoutLazyTableFlags((Map<String, Object>) map) : value);
    });
    return copy;
  }

  @Override
  public boolean validate() throws ValidationException {
    validateSpaceExists();
    //Return true as no user inputs are necessary
    return true;
  }

  //This needs to be validated here because "Ref" itself allows to have an empty range (i.e. start=end)
  protected void validateRef(Ref ref) throws ValidationException {
    //TODO: Move "versionRef" property to SpaceBasedStep
    if (ref != null && ref.isRange() && ref.getStart().getVersion() == ref.getEnd().getVersion())
      throw new ValidationException("Start version must be smaller than the end version in ref " + ref);
  }

  protected <T> T handleErrorResponse(ErrorResponseException e) throws ErrorResponseException {
    if (e.getStatusCode() >= 500 || e.getStatusCode() == 429 || e.getStatusCode() == 403 || e.getStatusCode() == 401
        || e.getStatusCode() == 404)
      throw new StepException("Error requesting Hub Service" + (e.getParsedErrorResponse() != null ? ": " + e.getParsedErrorResponse().getError() + ": " + e.getParsedErrorResponse().getErrorMessage() : ""), e)
          .withCode("HTTP-" + e.getStatusCode())
          .withRetryable(true);
    throw e;
  }

  protected void infoLog(LogPhase phase, String... messages) {
    logger.info("{} [{}@{}] ON '{}' {}", getClass().getSimpleName(), getGlobalStepId(),
            phase.name(), spaceId, messages.length > 0 ? messages : "");
  }

  protected void warnLog(LogPhase phase, String... messages) {
    logger.warn("{} [{}@{}] ON '{}' {}", getClass().getSimpleName(), getGlobalStepId(),
            phase.name(), spaceId, messages.length > 0 ? messages : "");
  }

  protected void errorLog(LogPhase phase, Exception e, String... message) {
    logger.error("{} [{}@{}] ON '{}' {}", getClass().getSimpleName(), getGlobalStepId(),
            phase.name(), spaceId, message, e);
  }

  public enum LogPhase {
    GRAPH_TRANSFORMER,
    JOB_EXECUTOR,
    STEP_EXECUTE,
    STEP_RESUME,
    STEP_CANCEL,
    STEP_ON_STATE_CHECK,
    STEP_ON_ASYNC_FAILURE,
    STEP_ON_ASYNC_UPDATE,
    STEP_ON_ASYNC_SUCCESS,
    JOB_DELETE,
    JOB_VALIDATE,
    UNKNOWN
  }
}
