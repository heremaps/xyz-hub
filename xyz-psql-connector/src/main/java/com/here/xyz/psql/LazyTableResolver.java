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

package com.here.xyz.psql;

import static com.here.xyz.util.db.pg.LazySpaceTables.LevelState.EXISTS;
import static com.here.xyz.util.db.pg.LazySpaceTables.LevelState.NEVER_MATERIALIZED;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.DeleteChangesetsEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.pg.LazySpaceTables;
import com.here.xyz.util.db.pg.LazySpaceTables.Level;
import com.here.xyz.util.db.pg.LazySpaceTables.LevelCheck;
import com.here.xyz.util.db.pg.LazySpaceTables.LevelState;
import com.here.xyz.util.db.pg.ScriptInstaller.TemplateCheck;
import com.here.xyz.util.db.pg.TemplateUnavailableException;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper;
import com.here.xyz.util.runtime.FunctionRuntime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Resolves the lazy levels of an event before it is processed: the levels of the space and its bases that carry the lazyTable
 * flag and may have no table yet. A level without a table is read from the empty-space template.
 *
 * <p>A connector handler is created per event, so the cache of existing tables is static, while the state of the event is
 * kept in the instance.</p>
 */
public class LazyTableResolver {
  private static final Logger logger = LogManager.getLogger(LazyTableResolver.class);
  /**
   * The time the request needs after a materialization, for the write itself and the response.
   */
  static final long RESERVE_MILLIS = 10_000;
  /**
   * The tables that were found to exist, per database, schema and table. Missing tables are never cached: another container
   * may create the table with the next write.
   */
  private static final Map<String, Boolean> EXISTING_TABLES = ExpiringMap.builder()
      .maxSize(10_000)
      .expirationPolicy(ExpirationPolicy.CREATED)
      .expiration(10, TimeUnit.MINUTES)
      .build();

  /**
   * How an event deals with lazy levels without a table.
   */
  enum EventClass {
    /**
     * Reads every missing lazy level from the template.
     */
    READ,
    /**
     * Materializes its write target and reads its missing lazy bases from the template.
     */
    WRITE,
    /**
     * Has nothing to do for a missing target: an erase answers an empty result, a changeset deletion reads the template.
     */
    NOOP_IF_MISSING,
    /**
     * Is not affected by lazy levels.
     */
    PASS
  }

  /**
   * Checks the existence of the tables of lazy levels and of the template.
   */
  @FunctionalInterface
  interface LevelChecker {
    LevelCheck check(DataSourceProvider dataSourceProvider, String schema, List<Level> levels) throws SQLException;
  }

  private final Supplier<DataSourceProvider> dataSourceProvider;
  private final BooleanSupplier lazyTablesSupported;
  private final Supplier<String> connectorId;
  private LevelChecker writerChecker = LazySpaceTables::checkLevels;

  //The state of the current event
  private Map<String, Object> originalParams;

  LazyTableResolver(Supplier<DataSourceProvider> dataSourceProvider, BooleanSupplier lazyTablesSupported,
      Supplier<String> connectorId) {
    this.dataSourceProvider = dataSourceProvider;
    this.lazyTablesSupported = lazyTablesSupported;
    this.connectorId = connectorId;
  }

  LazyTableResolver withWriterChecker(LevelChecker writerChecker) {
    this.writerChecker = writerChecker;
    return this;
  }

  static EventClass classify(Event event) {
    if (event instanceof ModifyFeaturesEvent modifyFeaturesEvent)
      return modifyFeaturesEvent.isEraseContent() ? EventClass.NOOP_IF_MISSING : EventClass.WRITE;
    if (event instanceof WriteFeaturesEvent)
      return EventClass.WRITE;
    if (event instanceof DeleteChangesetsEvent)
      return EventClass.NOOP_IF_MISSING;
    //SearchForFeaturesEvent covers the spatial reads, the iteration and the iteration of changesets
    if (event instanceof SearchForFeaturesEvent || event instanceof GetFeaturesByIdEvent || event instanceof LoadFeaturesEvent
        || event instanceof GetStatisticsEvent || event instanceof GetChangesetStatisticsEvent)
      return EventClass.READ;
    return EventClass.PASS;
  }

  /**
   * Decides without any database access whether the event needs the resolver: it must have a lazy level, an event class other
   * than PASS, and the connector must support lazy tables.
   */
  boolean isActive(Event event) {
    return event != null && lazyTablesSupported.getAsBoolean() && classify(event) != EventClass.PASS
        && LazySpaceTables.hasLazyLevel(event.getParams());
  }

  /**
   * Prepares the event: checks the lazy levels without a cached table and replaces the missing ones by the template in a copy of
   * the params. The original params are kept for a retry.
   *
   * @return A response that answers the event without processing it, or null if the event is to be processed
   */
  XyzResponse prepare(Event event) throws SQLException, ErrorResponseException {
    originalParams = event.getParams();
    EventClass eventClass = classify(event);
    //Writes are not resolved yet
    if (eventClass == EventClass.WRITE || eventClass == EventClass.PASS)
      return null;

    DataSourceProvider dsp = dataSourceProvider.get();
    DatabaseSettings settings = dsp.getDatabaseSettings();
    String schema = settings.getSchema();
    List<Level> levels = resolveTables(event, LazySpaceTables.lazyLevels(originalParams));
    LevelResult result = checkLevels(dsp, settings, schema, levels);

    List<Level> missing = new ArrayList<>();
    for (int i = 0; i < levels.size(); i++)
      if (result.states().get(i) == NEVER_MATERIALIZED)
        missing.add(levels.get(i));

    if (event instanceof ModifyFeaturesEvent) {
      //An erase of a never written space has nothing to erase
      boolean targetMissing = missing.stream().anyMatch(level -> level.depth() == 0);
      return targetMissing ? new FeatureCollection() : null;
    }

    substitute(event, settings, schema, result, missing);
    return null;
  }

  /**
   * Replaces the provided levels by the template, after the template checks.
   */
  private void substitute(Event event, DatabaseSettings settings, String schema, LevelResult result, List<Level> substituted)
      throws TemplateUnavailableException {
    if (substituted.isEmpty())
      return;
    //The installer of this container has found a foreign or damaged set, which must not be read
    if (settings.getEmptySpaceTemplateCheck() == TemplateCheck.NOT_IDENTICAL)
      throw TemplateUnavailableException.notIdentical(schema);
    Set<Integer> depths = new LinkedHashSet<>();
    substituted.forEach(level -> depths.add(level.depth()));
    LazySpaceTables.requireTemplate(schema, new LevelCheck(result.states(), result.templatePresent()), depths);
    event.setParams(LazySpaceTables.substitute(originalParams, substituted));
  }

  /**
   * The states of the levels, in their order, and whether the template exists. The template is only known to exist if a check
   * ran; levels found in the cache need no template.
   */
  record LevelResult(List<LevelState> states, boolean templatePresent) {}

  private LevelResult checkLevels(DataSourceProvider dsp, DatabaseSettings settings, String schema, List<Level> levels)
      throws SQLException {
    List<LevelState> states = new ArrayList<>();
    List<Level> uncached = new ArrayList<>();
    for (Level level : levels) {
      boolean cached = EXISTING_TABLES.containsKey(cacheKey(settings, schema, level.table()));
      states.add(cached ? EXISTS : null);
      if (!cached)
        uncached.add(level);
    }
    if (uncached.isEmpty())
      return new LevelResult(states, true);

    long start = System.currentTimeMillis();
    LevelCheck check = writerChecker.check(dsp, schema, uncached);
    long elapsed = System.currentTimeMillis() - start;
    if (!check.templatePresent())
      //This container stops creating lazy spaces, a read on the writer found no template
      settings.markEmptySpaceTemplateMissing();

    int missing = 0;
    for (int i = 0, u = 0; i < states.size(); i++) {
      if (states.get(i) != null)
        continue;
      LevelState state = check.states().get(u++);
      states.set(i, state);
      if (state == EXISTS)
        EXISTING_TABLES.put(cacheKey(settings, schema, levels.get(i).table()), Boolean.TRUE);
      else
        missing++;
    }
    logger.info("lazyCheck connector={} levels={} missing={} ms={} source=writer", connectorId.get(), uncached.size(), missing,
        elapsed);
    return new LevelResult(states, check.templatePresent());
  }

  /**
   * Gives each level its physical table name: the explicit one, or the one derived from the space ID for a level without one.
   */
  private static List<Level> resolveTables(Event event, List<Level> levels) {
    boolean hashed = ConnectorParameters.fromEvent(event).isEnableHashedSpaceId();
    List<Level> resolved = new ArrayList<>();
    for (Level level : levels) {
      String spaceId = level.depth() == 0 ? event.getSpace() : level.spaceId();
      String table = level.table() != null ? level.table() : XyzSpaceTableHelper.getTableNameForSpaceId(spaceId, hashed);
      resolved.add(new Level(level.depth(), spaceId, table));
    }
    return resolved;
  }

  private static String cacheKey(DatabaseSettings settings, String schema, String table) {
    return settings.getCacheKey() + "/" + schema + "." + table;
  }

  /**
   * @return The time a materialization may take: the remaining time of the invocation minus the reserve for the request, at most
   *  the connector's cap. A result below the minimum of {@link LazySpaceTables#materialize} makes it fail without a database access.
   */
  static long materializationBudget(FunctionRuntime runtime, long capMillis) {
    long remaining = runtime == null ? Integer.MAX_VALUE : runtime.getRemainingTime();
    return Math.min(remaining - RESERVE_MILLIS, capMillis);
  }

  static void clearCache() {
    EXISTING_TABLES.clear();
  }
}
