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

import static com.here.xyz.psql.LazyTableResolver.EventClass.NOOP_IF_MISSING;
import static com.here.xyz.psql.LazyTableResolver.EventClass.PASS;
import static com.here.xyz.psql.LazyTableResolver.EventClass.READ;
import static com.here.xyz.psql.LazyTableResolver.EventClass.WRITE;
import static com.here.xyz.util.db.pg.LazySpaceTables.LevelState.EXISTS;
import static com.here.xyz.util.db.pg.LazySpaceTables.LevelState.NEVER_MATERIALIZED;
import static com.here.xyz.util.db.pg.LazySpaceTables.TEMPLATE_TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.DeleteChangesetsEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.GetStorageStatisticsEvent;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySubscriptionEvent;
import com.here.xyz.events.OneTimeActionEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.Random;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.pg.LazySpaceTables;
import com.here.xyz.util.db.pg.LazySpaceTables.Level;
import com.here.xyz.util.db.pg.LazySpaceTables.LevelCheck;
import com.here.xyz.util.db.pg.LazySpaceTables.LevelState;
import com.here.xyz.util.db.pg.LazySpaceTables.MaterializationResult;
import com.here.xyz.util.db.pg.MaterializationTimeoutException;
import com.here.xyz.util.db.pg.MaterializationTimeoutException.Phase;
import com.here.xyz.util.db.pg.ReservedTableNameException;
import com.here.xyz.util.db.pg.ScriptInstaller.TemplateCheck;
import com.here.xyz.util.db.pg.TemplateUnavailableException;
import com.here.xyz.util.runtime.FunctionRuntime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The decisions of {@link LazyTableResolver} without a database: the database calls are stubs.
 */
class LazyTableResolverTest {
  private static final String SCHEMA = "lazy_test";

  /**
   * Settings whose template check result is set by the test.
   */
  static class StubSettings extends DatabaseSettings {
    TemplateCheck check;
    int markedMissing;

    StubSettings(TemplateCheck check) {
      super("LazyTableResolverTest_" + Random.randomAlpha(5));
      this.check = check;
      withSchema(SCHEMA);
    }

    @Override
    public TemplateCheck getEmptySpaceTemplateCheck() {
      return check;
    }

    @Override
    public void markEmptySpaceTemplateMissing() {
      markedMissing++;
    }
  }

  /**
   * A data source provider without data sources, it fails on any database access.
   */
  static class StubDataSourceProvider extends DataSourceProvider {
    private final DatabaseSettings settings;

    StubDataSourceProvider(DatabaseSettings settings) {
      super(null);
      this.settings = settings;
    }

    @Override
    public DataSource getReader() {
      throw new AssertionError("No database access expected");
    }

    @Override
    public DataSource getWriter() {
      throw new AssertionError("No database access expected");
    }

    @Override
    public DatabaseSettings getDatabaseSettings() {
      return settings;
    }

    @Override
    public void close() {}
  }

  /**
   * A level checker that answers from a list of states, one per checked level, and counts its calls.
   */
  static class StubChecker implements LazyTableResolver.LevelChecker {
    final AtomicInteger calls = new AtomicInteger();
    final List<List<Level>> checked = new ArrayList<>();
    LevelState state;
    boolean templatePresent;
    SQLException failure;

    StubChecker(LevelState state, boolean templatePresent) {
      this.state = state;
      this.templatePresent = templatePresent;
    }

    @Override
    public LevelCheck check(DataSourceProvider dataSourceProvider, String schema, List<Level> levels) throws SQLException {
      calls.incrementAndGet();
      checked.add(levels);
      if (failure != null)
        throw failure;
      return new LevelCheck(levels.stream().map(level -> state).toList(), templatePresent);
    }
  }

  private static LazyTableResolver resolver(DatabaseSettings settings, StubChecker checker) {
    return new LazyTableResolver(() -> new StubDataSourceProvider(settings), () -> true, () -> "test-connector")
        .withWriterChecker(checker);
  }

  private static Map<String, Object> lazyParams() {
    return new HashMap<>(Map.of(Space.TABLE_NAME, "t_" + Random.randomAlpha(8), Space.LAZY_TABLE, true));
  }

  private static <E extends Event<?>> E lazy(E event) {
    event.setSpace("space_" + Random.randomAlpha(5));
    event.setParams(lazyParams());
    event.setConnectorParams(new HashMap<>(Map.of("connectorId", "test-connector")));
    return event;
  }

  @Test
  void classifiesTheEvents() {
    for (Event<?> event : List.of(new GetFeaturesByIdEvent(), new GetFeaturesByBBoxEvent<>(), new GetFeaturesByTileEvent(),
        new GetFeaturesByGeometryEvent(), new IterateFeaturesEvent<>(), new SearchForFeaturesEvent<>(), new LoadFeaturesEvent(),
        new GetStatisticsEvent(), new IterateChangesetsEvent(), new GetChangesetStatisticsEvent()))
      assertEquals(READ, LazyTableResolver.classify(event), event.getClass().getSimpleName());

    assertEquals(WRITE, LazyTableResolver.classify(new WriteFeaturesEvent()));
    assertEquals(WRITE, LazyTableResolver.classify(new ModifyFeaturesEvent()));
    assertEquals(WRITE, LazyTableResolver.classify(new ModifyFeaturesEvent().withInsertFeatures(new ArrayList<>())));
    assertEquals(NOOP_IF_MISSING, LazyTableResolver.classify(new ModifyFeaturesEvent().withEraseContent(true)));
    assertEquals(NOOP_IF_MISSING, LazyTableResolver.classify(new DeleteChangesetsEvent()));

    for (Event<?> event : List.of(new ModifySpaceEvent(), new GetStorageStatisticsEvent(), new HealthCheckEvent(),
        new ModifySubscriptionEvent(), new OneTimeActionEvent()))
      assertEquals(PASS, LazyTableResolver.classify(event), event.getClass().getSimpleName());
  }

  @Test
  void inactiveWithoutLazyLevelAndWithoutDatabaseAccess() {
    AtomicInteger providerCalls = new AtomicInteger();
    LazyTableResolver resolver = new LazyTableResolver(() -> {
      providerCalls.incrementAndGet();
      return null;
    }, () -> true, () -> "c");

    assertFalse(resolver.isActive(null));
    assertFalse(resolver.isActive(new GetFeaturesByIdEvent().withSpace("s")));
    assertFalse(resolver.isActive(new GetFeaturesByIdEvent().withSpace("s").withParams(Map.of(Space.TABLE_NAME, "t"))));
    assertFalse(resolver.isActive(new GetFeaturesByIdEvent().withSpace("s").withParams(Map.of(Space.LAZY_TABLE, "true"))));
    //The space creation handles the flag itself
    assertFalse(resolver.isActive(lazy(new ModifySpaceEvent())));
    assertTrue(resolver.isActive(lazy(new GetFeaturesByIdEvent())));
    assertTrue(resolver.isActive(new GetFeaturesByIdEvent().withSpace("s")
        .withParams(Map.of(Space.TABLE_NAME, "t", "extends", Map.of("spaceId", "b", Space.LAZY_TABLE, true)))));

    LazyTableResolver unsupported = new LazyTableResolver(() -> null, () -> false, () -> "c");
    assertFalse(unsupported.isActive(lazy(new GetFeaturesByIdEvent())));
    assertEquals(0, providerCalls.get());
  }

  @Test
  void connectorWithoutLazySupportIsInactive() {
    assertTrue(new PSQLXyzConnector().getResolver().isActive(lazy(new GetFeaturesByIdEvent())));
    assertFalse(new NLConnector().getResolver().isActive(lazy(new GetFeaturesByIdEvent())));
  }

  @Test
  void budgetIsTheRemainingTimeMinusTheReserveAtMostTheCap() {
    assertEquals(20_000, LazyTableResolver.materializationBudget(runtime(30_000), 25_000));
    assertEquals(25_000, LazyTableResolver.materializationBudget(runtime(35_000), 25_000));
    assertEquals(3_000, LazyTableResolver.materializationBudget(runtime(Integer.MAX_VALUE), 3_000));
    assertEquals(25_000, LazyTableResolver.materializationBudget(runtime(Integer.MAX_VALUE), 25_000));
    //Below the minimum of the materialization, which then fails without a database access
    assertEquals(2_000, LazyTableResolver.materializationBudget(runtime(12_000), 25_000));
    assertTrue(LazyTableResolver.materializationBudget(runtime(5_000), 25_000) < 0);
  }

  @Test
  void readSubstitutesTheMissingLevelAndKeepsTheOriginalParams() throws Exception {
    StubSettings settings = new StubSettings(TemplateCheck.VERIFIED);
    StubChecker checker = new StubChecker(NEVER_MATERIALIZED, true);
    GetFeaturesByIdEvent event = lazy(new GetFeaturesByIdEvent());
    Map<String, Object> original = event.getParams();
    String table = (String) original.get(Space.TABLE_NAME);

    assertNull(resolver(settings, checker).prepare(event));
    assertEquals(TEMPLATE_TABLE, event.getParams().get(Space.TABLE_NAME));
    assertEquals(true, event.getParams().get(Space.LAZY_TABLE));
    assertEquals(table, original.get(Space.TABLE_NAME), "The original params must stay unchanged");
    assertEquals(1, checker.calls.get());
    assertEquals(0, settings.markedMissing);
  }

  @Test
  void existingTablesAreCachedAndMissingOnesAreNot() throws Exception {
    StubSettings settings = new StubSettings(TemplateCheck.VERIFIED);
    StubChecker checker = new StubChecker(EXISTS, true);
    Map<String, Object> params = lazyParams();

    resolver(settings, checker).prepare(lazy(new GetFeaturesByIdEvent()).withParams(params));
    resolver(settings, checker).prepare(lazy(new GetFeaturesByIdEvent()).withParams(params));
    assertEquals(1, checker.calls.get(), "The second event must use the cache");

    StubChecker missingChecker = new StubChecker(NEVER_MATERIALIZED, true);
    Map<String, Object> otherParams = lazyParams();
    resolver(settings, missingChecker).prepare(lazy(new GetFeaturesByIdEvent()).withParams(otherParams));
    resolver(settings, missingChecker).prepare(lazy(new GetFeaturesByIdEvent()).withParams(otherParams));
    assertEquals(2, missingChecker.calls.get(), "A missing table is never cached");
  }

  @Test
  void eraseOfAMissingTargetIsAnsweredWithoutProcessing() throws Exception {
    StubSettings settings = new StubSettings(TemplateCheck.VERIFIED);
    ModifyFeaturesEvent erase = lazy(new ModifyFeaturesEvent().withEraseContent(true));
    Map<String, Object> original = erase.getParams();

    assertTrue(resolver(settings, new StubChecker(NEVER_MATERIALIZED, false)).prepare(erase) instanceof FeatureCollection);
    assertSame(original, erase.getParams(), "An erase substitutes nothing");
    assertNull(resolver(settings, new StubChecker(EXISTS, true)).prepare(lazy(new ModifyFeaturesEvent().withEraseContent(true))));
  }

  /**
   * Records the materializations instead of running them.
   */
  static class StubMaterializer implements LazyTableResolver.Materializer {
    final List<String> tables = new ArrayList<>();
    final List<List<String>> indexes = new ArrayList<>();

    @Override
    public MaterializationResult materialize(DataSourceProvider dataSourceProvider, String schema, String table, String spaceId,
        List<String> onDemandIndexes, long budgetMillis) {
      tables.add(table);
      indexes.add(onDemandIndexes);
      return MaterializationResult.CREATED;
    }
  }

  private static Map<String, Object> composite(boolean lazyExtension, boolean lazyBase) {
    Map<String, Object> base = new HashMap<>(Map.of("spaceId", "base_" + Random.randomAlpha(5), Space.TABLE_NAME,
        "b_" + Random.randomAlpha(8)));
    if (lazyBase)
      base.put(Space.LAZY_TABLE, true);
    Map<String, Object> params = new HashMap<>(Map.of(Space.TABLE_NAME, "x_" + Random.randomAlpha(8), "extends", base));
    if (lazyExtension)
      params.put(Space.LAZY_TABLE, true);
    return params;
  }

  @SuppressWarnings("unchecked")
  private static String baseTable(Map<String, Object> params) {
    return (String) ((Map<String, Object>) params.get("extends")).get(Space.TABLE_NAME);
  }

  @Test
  void firstWriteToAStandaloneSpaceMaterializesWithoutTheTemplate() throws Exception {
    StubMaterializer materializer = new StubMaterializer();
    WriteFeaturesEvent event = lazy(new WriteFeaturesEvent());
    event.getParams().put(LazySpaceTables.ON_DEMAND_INDEXES, List.of("foo", "it's"));
    String table = (String) event.getParams().get(Space.TABLE_NAME);
    LazyTableResolver resolver = resolver(new StubSettings(TemplateCheck.VERIFIED), new StubChecker(NEVER_MATERIALIZED, false))
        .withMaterializer(materializer);

    assertNull(resolver.prepare(event));
    assertEquals(List.of(table), materializer.tables);
    assertEquals(List.of(List.of("foo")), materializer.indexes, "The on-demand indexes are validated");
    assertEquals(table, event.getParams().get(Space.TABLE_NAME), "The write target is never substituted");
    FeatureCollection response = new FeatureCollection();
    assertSame(response, resolver.reportMaterialized(event, response));
    assertEquals(Boolean.TRUE, response.getMaterialized());
  }

  @Test
  void writeTargetDepthIsNeverPassedToTheTemplateCheck() throws Exception {
    //A lazy extension of a never written lazy base without the template: fails before any materialization
    StubMaterializer materializer = new StubMaterializer();
    WriteFeaturesEvent event = lazy(new WriteFeaturesEvent()).withParams(composite(true, true));
    assertThrows(TemplateUnavailableException.class, () -> resolver(new StubSettings(TemplateCheck.VERIFIED),
        new StubChecker(NEVER_MATERIALIZED, false)).withMaterializer(materializer).prepare(event));
    assertEquals(List.of(), materializer.tables);

    //With the template the base is read from it and the extension is materialized
    WriteFeaturesEvent withTemplate = lazy(new WriteFeaturesEvent()).withParams(composite(true, true));
    String extension = (String) withTemplate.getParams().get(Space.TABLE_NAME);
    assertNull(resolver(new StubSettings(TemplateCheck.VERIFIED), new StubChecker(NEVER_MATERIALIZED, true))
        .withMaterializer(materializer).prepare(withTemplate));
    assertEquals(List.of(extension), materializer.tables);
    assertEquals(extension, withTemplate.getParams().get(Space.TABLE_NAME));
    assertEquals(TEMPLATE_TABLE, baseTable(withTemplate.getParams()));
  }

  @Test
  void superWriteMaterializesTheBase() throws Exception {
    StubMaterializer materializer = new StubMaterializer();
    Map<String, Object> params = composite(true, true);
    String base = baseTable(params);
    WriteFeaturesEvent event = lazy(new WriteFeaturesEvent()).withParams(params).withContext(SpaceContext.SUPER);
    LazyTableResolver resolver = resolver(new StubSettings(TemplateCheck.VERIFIED), new StubChecker(NEVER_MATERIALIZED, true))
        .withMaterializer(materializer);

    assertNull(resolver.prepare(event));
    assertEquals(List.of(base), materializer.tables);
    assertEquals(TEMPLATE_TABLE, event.getParams().get(Space.TABLE_NAME), "The missing extension is read from the template");
    assertEquals(base, baseTable(event.getParams()));
    FeatureCollection response = new FeatureCollection();
    resolver.reportMaterialized(event, response);
    assertEquals(Boolean.TRUE, response.getMaterialized());
  }

  @Test
  void reportOnlyForASuccessfulWriteOfALazyTarget() throws Exception {
    StubSettings settings = new StubSettings(TemplateCheck.VERIFIED);
    //An existing lazy target (EXISTED or a cache hit) reports too
    ModifyFeaturesEvent write = lazy(new ModifyFeaturesEvent());
    LazyTableResolver resolver = resolver(settings, new StubChecker(EXISTS, true));
    resolver.prepare(write);
    FeatureCollection response = new FeatureCollection();
    resolver.reportMaterialized(write, response);
    assertEquals(Boolean.TRUE, response.getMaterialized());
    ErrorResponse error = new ErrorResponse().withError(XyzError.CONFLICT);
    assertSame(error, resolver.reportMaterialized(write, error));

    //A target without the flag, only its base is lazy
    WriteFeaturesEvent baseOnly = lazy(new WriteFeaturesEvent()).withParams(composite(false, true));
    LazyTableResolver baseOnlyResolver = resolver(settings, new StubChecker(NEVER_MATERIALIZED, true))
        .withMaterializer(new StubMaterializer());
    baseOnlyResolver.prepare(baseOnly);
    FeatureCollection baseOnlyResponse = new FeatureCollection();
    baseOnlyResolver.reportMaterialized(baseOnly, baseOnlyResponse);
    assertNull(baseOnlyResponse.getMaterialized());

    //A read and an erase never report
    GetFeaturesByIdEvent read = lazy(new GetFeaturesByIdEvent());
    LazyTableResolver readResolver = resolver(settings, new StubChecker(EXISTS, true));
    readResolver.prepare(read);
    FeatureCollection readResponse = new FeatureCollection();
    readResolver.reportMaterialized(read, readResponse);
    assertNull(readResponse.getMaterialized());

    ModifyFeaturesEvent erase = lazy(new ModifyFeaturesEvent().withEraseContent(true));
    LazyTableResolver eraseResolver = resolver(settings, new StubChecker(EXISTS, true));
    eraseResolver.prepare(erase);
    FeatureCollection eraseResponse = new FeatureCollection();
    eraseResolver.reportMaterialized(erase, eraseResponse);
    assertNull(eraseResponse.getMaterialized());
  }

  @Test
  void readOnlyConnectorNeverMaterializes() throws Exception {
    StubMaterializer materializer = new StubMaterializer();
    WriteFeaturesEvent event = lazy(new WriteFeaturesEvent());
    event.getConnectorParams().put("readOnly", true);
    LazyTableResolver resolver = resolver(new StubSettings(TemplateCheck.VERIFIED), new StubChecker(NEVER_MATERIALIZED, true))
        .withMaterializer(materializer);

    assertNull(resolver.prepare(event));
    assertEquals(List.of(), materializer.tables);
    FeatureCollection response = new FeatureCollection();
    resolver.reportMaterialized(event, response);
    assertNull(response.getMaterialized());
  }

  @Test
  void notIdenticalTemplateIsNeverRead() {
    StubSettings settings = new StubSettings(TemplateCheck.NOT_IDENTICAL);
    TemplateUnavailableException e = assertThrows(TemplateUnavailableException.class,
        () -> resolver(settings, new StubChecker(NEVER_MATERIALIZED, true)).prepare(lazy(new GetFeaturesByIdEvent())));
    assertEquals("Empty-space template not identical: " + TEMPLATE_TABLE + " in schema " + SCHEMA, e.getMessage());
  }

  @ParameterizedTest
  @EnumSource(value = TemplateCheck.class, names = {"VERIFIED", "MISSING_AT_RUNTIME", "FAILED_PERMANENT", "NOT_VERIFIED"})
  void otherTemplateResultsFallThroughToTheExistenceCheck(TemplateCheck check) throws Exception {
    GetFeaturesByIdEvent event = lazy(new GetFeaturesByIdEvent());
    assertNull(resolver(new StubSettings(check), new StubChecker(NEVER_MATERIALIZED, true)).prepare(event));
    assertEquals(TEMPLATE_TABLE, event.getParams().get(Space.TABLE_NAME));

    TemplateUnavailableException e = assertThrows(TemplateUnavailableException.class,
        () -> resolver(new StubSettings(check), new StubChecker(NEVER_MATERIALIZED, false)).prepare(lazy(new GetFeaturesByIdEvent())));
    assertTrue(e.getMessage().startsWith("Empty-space template missing"), e.getMessage());
  }

  @Test
  void noTemplateResultYetFallsThroughToTheExistenceCheck() throws Exception {
    GetFeaturesByIdEvent event = lazy(new GetFeaturesByIdEvent());
    assertNull(resolver(new StubSettings(null), new StubChecker(NEVER_MATERIALIZED, true)).prepare(event));
    assertEquals(TEMPLATE_TABLE, event.getParams().get(Space.TABLE_NAME));
  }

  @Test
  void writerCheckWithoutTheTemplateMarksItMissing() throws Exception {
    StubSettings settings = new StubSettings(TemplateCheck.VERIFIED);
    //Also for an event that substitutes nothing
    resolver(settings, new StubChecker(EXISTS, false)).prepare(lazy(new GetFeaturesByIdEvent()));
    assertEquals(1, settings.markedMissing);
  }

  private static ErrorResponseException prepareMappedFailure(Exception failure) {
    PSQLXyzConnector connector = new PSQLXyzConnector();
    connector.setResolver(new LazyTableResolver(() -> null, () -> true, () -> "c") {
      @Override
      com.here.xyz.responses.XyzResponse prepare(Event event) throws SQLException {
        if (failure instanceof SQLException sqlException)
          throw sqlException;
        throw (RuntimeException) failure;
      }
    });
    return assertThrows(ErrorResponseException.class, () -> connector.prepareMapped(lazy(new GetFeaturesByIdEvent())));
  }

  @Test
  void prepareMappedMapsTheLazyTableErrors() {
    ErrorResponseException timeout = prepareMappedFailure(
        new MaterializationTimeoutException("timed out", MaterializationTimeoutException.LOCK_NOT_AVAILABLE, Phase.LOCK, null));
    assertEquals(XyzError.TIMEOUT, timeout.getErrorResponse().getError());
    assertEquals("Materialization of the space table timed out, retry is safe", timeout.getErrorResponse().getErrorMessage());

    ErrorResponseException reserved = prepareMappedFailure(new ReservedTableNameException(TEMPLATE_TABLE));
    assertEquals(XyzError.EXCEPTION, reserved.getErrorResponse().getError());
    assertEquals("Internal error: reserved table name", reserved.getErrorResponse().getErrorMessage());

    for (TemplateUnavailableException e : List.of(TemplateUnavailableException.missing(SCHEMA),
        TemplateUnavailableException.notIdentical(SCHEMA))) {
      ErrorResponseException template = prepareMappedFailure(e);
      assertEquals(XyzError.EXCEPTION, template.getErrorResponse().getError());
      assertEquals("Empty-space template missing or not identical on the database", template.getErrorResponse().getErrorMessage());
      assertSame(e, template.getCause());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"57014", "08006"})
  void checkLevelsErrorsGoThroughTheSqlErrorMapping(String sqlState) {
    StubChecker checker = new StubChecker(EXISTS, true);
    checker.failure = new SQLException("failure", sqlState);
    PSQLXyzConnector connector = new PSQLXyzConnector();
    connector.setResolver(resolver(new StubSettings(TemplateCheck.VERIFIED), checker));

    ErrorResponseException e = assertThrows(ErrorResponseException.class, () -> connector.prepareMapped(lazy(new GetFeaturesByIdEvent())));
    assertEquals("57014".equals(sqlState) ? XyzError.TIMEOUT : XyzError.EXCEPTION, e.getErrorResponse().getError());
  }

  //------------ The safety net and the replica option ------------

  private static ErrorResponseException missingRelation() {
    return new ErrorResponseException(XyzError.NOT_FOUND, "Space not found in backend",
        new SQLException("ERROR: relation \"t\" does not exist", "42P01"));
  }

  /**
   * A connector whose reads and writes fail with a missing relation as many times as configured, and record the events they see.
   */
  static class FailingConnector extends PSQLXyzConnector {
    int failures;
    final List<Event<?>> dispatched = new ArrayList<>();
    final List<Map<String, Object>> dispatchedParams = new ArrayList<>();
    final List<Boolean> dispatchedPreferPrimary = new ArrayList<>();

    FailingConnector(int failures, LazyTableResolver resolver) {
      this.failures = failures;
      setResolver(resolver);
    }

    private FeatureCollection dispatch(Event<?> event) throws ErrorResponseException {
      dispatched.add(event);
      dispatchedParams.add(event.getParams());
      dispatchedPreferPrimary.add(event.getPreferPrimaryDataSource());
      if (failures-- > 0)
        throw missingRelation();
      return new FeatureCollection();
    }

    @Override
    protected FeatureCollection processGetFeaturesByIdEvent(GetFeaturesByIdEvent event) throws Exception {
      return dispatch(event);
    }

    @Override
    protected FeatureCollection processWriteFeaturesEvent(WriteFeaturesEvent event) throws Exception {
      return dispatch(event);
    }
  }

  @Test
  void safetyNetRetriesAReadOnceOnTheWriter() throws Exception {
    StubChecker checker = new StubChecker(EXISTS, true);
    FailingConnector connector = new FailingConnector(1, resolver(new StubSettings(TemplateCheck.VERIFIED), checker));
    GetFeaturesByIdEvent event = lazy(new GetFeaturesByIdEvent());
    Map<String, Object> original = event.getParams();

    assertTrue(connector.processEvent(event) instanceof FeatureCollection);
    assertEquals(List.of(false, true), connector.dispatchedPreferPrimary, "The retried read prefers the writer");
    assertEquals(2, checker.calls.get(), "The retry checks again, the cache entry is gone");
    assertSame(original, connector.dispatchedParams.get(1), "The retry starts from the original params");
  }

  @Test
  void safetyNetRetriesOnlyOnce() throws Exception {
    FailingConnector connector = new FailingConnector(2, resolver(new StubSettings(TemplateCheck.VERIFIED),
        new StubChecker(EXISTS, true)));
    ErrorResponseException e = assertThrows(ErrorResponseException.class, () -> connector.processEvent(lazy(new GetFeaturesByIdEvent())));
    assertEquals(XyzError.NOT_FOUND, e.getErrorResponse().getError());
    assertEquals(2, connector.dispatched.size());
  }

  @Test
  void otherErrorsAndSpacesWithoutLazyLevelGetNoRetry() throws Exception {
    FailingConnector connector = new FailingConnector(0, resolver(new StubSettings(TemplateCheck.VERIFIED),
        new StubChecker(EXISTS, true))) {
      @Override
      protected FeatureCollection processGetFeaturesByIdEvent(GetFeaturesByIdEvent event) throws Exception {
        dispatched.add(event);
        throw new ErrorResponseException(XyzError.EXCEPTION, "other", new SQLException("other", "XX000"));
      }
    };
    assertThrows(ErrorResponseException.class, () -> connector.processEvent(lazy(new GetFeaturesByIdEvent())));
    assertEquals(1, connector.dispatched.size());

    FailingConnector notLazy = new FailingConnector(1, resolver(new StubSettings(TemplateCheck.VERIFIED),
        new StubChecker(EXISTS, true)));
    assertThrows(ErrorResponseException.class, () -> notLazy.processEvent(new GetFeaturesByIdEvent().withSpace("s")
        .withParams(Map.of(Space.TABLE_NAME, "t")).withConnectorParams(Map.of("connectorId", "c"))));
    assertEquals(1, notLazy.dispatched.size(), "A space without a lazy level gets no safety net");
  }

  @Test
  void secondPrepareUsesTheSameMapping() throws Exception {
    //The table was dropped since it was cached, and the template is gone as well
    StubChecker checker = new StubChecker(EXISTS, true);
    StubSettings settings = new StubSettings(TemplateCheck.VERIFIED);
    FailingConnector connector = new FailingConnector(1, new LazyTableResolver(() -> new StubDataSourceProvider(settings),
        () -> true, () -> "c") {
      @Override
      XyzResponse prepare(Event event) throws SQLException, ErrorResponseException {
        if (checker.calls.get() == 1) {
          checker.state = NEVER_MATERIALIZED;
          checker.templatePresent = false;
        }
        return super.prepare(event);
      }
    }.withWriterChecker(checker));

    ErrorResponseException e = assertThrows(ErrorResponseException.class, () -> connector.processEvent(lazy(new GetFeaturesByIdEvent())));
    assertEquals(XyzError.EXCEPTION, e.getErrorResponse().getError());
    assertEquals("Empty-space template missing or not identical on the database", e.getErrorResponse().getErrorMessage());
    assertTrue(e.getCause() instanceof TemplateUnavailableException);
    assertEquals(1, connector.dispatched.size());
  }

  @Test
  void retriedWriteReportsMaterialized() throws Exception {
    StubChecker checker = new StubChecker(EXISTS, true);
    StubMaterializer materializer = new StubMaterializer();
    StubSettings settings = new StubSettings(TemplateCheck.VERIFIED);
    FailingConnector connector = new FailingConnector(1, new LazyTableResolver(() -> new StubDataSourceProvider(settings),
        () -> true, () -> "c") {
      @Override
      XyzResponse prepare(Event event) throws SQLException, ErrorResponseException {
        //The cached table was dropped, the second check finds it missing
        if (checker.calls.get() == 1)
          checker.state = NEVER_MATERIALIZED;
        return super.prepare(event);
      }
    }.withWriterChecker(checker).withMaterializer(materializer));
    WriteFeaturesEvent event = lazy(new WriteFeaturesEvent());

    XyzResponse response = connector.processEvent(event);
    assertEquals(Boolean.TRUE, ((FeatureCollection) response).getMaterialized());
    assertEquals(1, materializer.tables.size(), "The retry materializes the table again");
    assertEquals(List.of(false, false), connector.dispatchedPreferPrimary, "A write always uses the writer");
  }

  @Test
  void replicaCheckWithoutTheTemplateIsRepeatedOnTheWriter() throws Exception {
    StubSettings settings = new StubSettings(TemplateCheck.VERIFIED);
    StubChecker replica = new StubChecker(NEVER_MATERIALIZED, false);
    StubChecker writer = new StubChecker(NEVER_MATERIALIZED, true);
    LazyTableResolver resolver = resolver(settings, writer).withReaderChecker(replica);
    GetFeaturesByIdEvent event = lazy(new GetFeaturesByIdEvent());
    event.getConnectorParams().put("lazyCheckOnReplica", true);

    assertNull(resolver.prepare(event));
    assertEquals(1, replica.calls.get());
    assertEquals(1, writer.calls.get());
    assertEquals(TEMPLATE_TABLE, event.getParams().get(Space.TABLE_NAME));
    assertEquals(0, settings.markedMissing, "A replica result alone never marks the template missing");
  }

  @Test
  void onlyReplicaReadsCheckOnTheReplica() throws Exception {
    StubChecker replica = new StubChecker(EXISTS, true);
    StubChecker writer = new StubChecker(EXISTS, true);
    StubSettings settings = new StubSettings(TemplateCheck.VERIFIED);
    for (Event<?> event : List.<Event<?>>of(lazy(new GetFeaturesByIdEvent()), lazy(new LoadFeaturesEvent()),
        lazy(new WriteFeaturesEvent()), lazy(new GetFeaturesByIdEvent()).withPreferPrimaryDataSource(true))) {
      event.getConnectorParams().put("lazyCheckOnReplica", true);
      resolver(settings, writer).withReaderChecker(replica).withMaterializer(new StubMaterializer()).prepare(event);
    }
    assertEquals(1, replica.calls.get(), "Only the plain read checks on the replica");
    assertEquals(3, writer.calls.get());

    //Without the param every check runs on the writer
    resolver(settings, writer).withReaderChecker(replica).prepare(lazy(new GetFeaturesByIdEvent()));
    assertEquals(1, replica.calls.get());
  }

  private static FunctionRuntime runtime(int remainingMillis) {
    return new FunctionRuntime() {
      @Override
      public int getRemainingTime() {
        return remainingMillis;
      }

      @Override
      public String getApplicationName() {
        return "test";
      }

      @Override
      public String getEnvironmentVariable(String variableName) {
        return null;
      }

      @Override
      public boolean isRunningLocally() {
        return true;
      }

      @Override
      public String getStreamId() {
        return "test";
      }
    };
  }
}
