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
import com.here.xyz.responses.XyzError;
import com.here.xyz.util.Random;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.pg.LazySpaceTables.Level;
import com.here.xyz.util.db.pg.LazySpaceTables.LevelCheck;
import com.here.xyz.util.db.pg.LazySpaceTables.LevelState;
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

  @Test
  void writesPassUnchanged() throws Exception {
    StubChecker checker = new StubChecker(NEVER_MATERIALIZED, true);
    WriteFeaturesEvent event = lazy(new WriteFeaturesEvent());
    Map<String, Object> original = event.getParams();
    assertNull(resolver(new StubSettings(TemplateCheck.VERIFIED), checker).prepare(event));
    assertSame(original, event.getParams());
    assertEquals(0, checker.calls.get());
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
