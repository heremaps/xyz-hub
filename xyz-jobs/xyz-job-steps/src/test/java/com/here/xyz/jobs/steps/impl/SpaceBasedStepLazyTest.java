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

import static com.here.xyz.models.hub.Space.LAZY_TABLE;
import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.execution.StepException;
import com.here.xyz.jobs.steps.impl.transport.CopySpace;
import com.here.xyz.jobs.steps.impl.transport.CountSpace;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.models.hub.Space.Extension;
import com.here.xyz.util.db.pg.LazySpaceTables.MaterializationResult;
import com.here.xyz.util.db.pg.LazySpaceTables.Profile;
import com.here.xyz.util.db.pg.MaterializationTimeoutException;
import com.here.xyz.util.db.pg.MaterializationTimeoutException.Phase;
import com.here.xyz.util.db.pg.ReservedTableNameException;
import com.here.xyz.util.db.pg.TemplateUnavailableException;
import com.here.xyz.util.runtime.FunctionRuntime;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.ErrorResponseException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient.Version;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.net.ssl.SSLSession;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The lazy table helpers of {@link SpaceBasedStep} without any service: a stub Hub client, no database.
 */
public class SpaceBasedStepLazyTest {
  private static final String CONNECTOR = "lazy-connector";
  private static final String LOGGER = SpaceBasedStep.class.getName();
  private static Level previousLevel;
  private CapturingAppender appender;

  @BeforeAll
  static void setUpConfig() {
    if (Config.instance == null)
      new Config();
    previousLevel = LogManager.getLogger(LOGGER).getLevel();
    Configurator.setLevel(LOGGER, Level.INFO);
  }

  @AfterAll
  static void restoreLogLevel() {
    Configurator.setLevel(LOGGER, previousLevel);
  }

  @BeforeEach
  void startCapturing() {
    appender = new CapturingAppender();
    appender.start();
    ((Logger) LogManager.getLogger(SpaceBasedStep.class)).addAppender(appender);
  }

  @AfterEach
  void stopCapturing() {
    ((Logger) LogManager.getLogger(SpaceBasedStep.class)).removeAppender(appender);
    appender.stop();
  }

  @Test
  void toStepExceptionRetriesOnlyTimeoutsLockWaitsAndConnectionErrors() {
    TestStep step = new TestStep(new StubHubWebClient());

    assertTrue(step.toStepException(new SQLException("canceled", "57014")).isRetryable());
    assertTrue(step.toStepException(new SQLException("lock", "55P03")).isRetryable());
    assertTrue(step.toStepException(new SQLException("connection lost", "08006")).isRetryable());
    assertTrue(step.toStepException(new MaterializationTimeoutException("timeout", "55P03", Phase.LOCK, null)).isRetryable());
    assertTrue(step.toStepException(new MaterializationTimeoutException("budget", "57014", Phase.BUDGET, null)).isRetryable());

    assertFalse(step.toStepException(new SQLException("no writer", "XX000")).isRetryable());
    assertFalse(step.toStepException(new ReservedTableNameException("xyz_empty_space_x")).isRetryable());
    assertFalse(step.toStepException(new SQLException("missing", "42P01")).isRetryable());
    assertFalse(step.toStepException(new SQLException("no state")).isRetryable());

    StepException templateMissing = step.toStepException(TemplateUnavailableException.missing("public"));
    assertFalse(templateMissing.isRetryable());
    assertInstanceOf(TemplateUnavailableException.class, templateMissing.getCause());
    assertTrue(templateMissing.getCause().getMessage().startsWith("Empty-space template missing"));
  }

  @Test
  void materializeTablesOfNonLazySpaceMakesNoCall() throws Exception {
    StubHubWebClient hub = new StubHubWebClient();
    TestStep step = new TestStep(hub);
    Space eager = space("eager", "eager_table", false).withExtension(new Extension().withSpaceId("base"));

    assertEquals(MaterializationResult.NOT_LAZY, step.materializeTables(eager, Profile.FULL));
    assertEquals(MaterializationResult.NOT_LAZY, step.materializeTablesOrFail(eager, Profile.BARE));
    assertEquals(0, hub.calls());
  }

  @Test
  void prepareLazyTablesMakesNoCallForNonLazyTargetsAndReaders() throws Exception {
    StubHubWebClient hub = new StubHubWebClient();
    TestImportStep importStep = new TestImportStep(hub);
    importStep.withSpaceId("eager");
    snapshots(importStep).put("eager", space("eager", "eager_table", false));
    importStep.runPrepareLazyTables();

    //The default of the steps that only read does nothing, also for a lazy space
    TestStep countStep = new TestStep(hub);
    countStep.withSpaceId("lazy");
    snapshots(countStep).put("lazy", space("lazy", "lazy_table", true));
    countStep.runPrepareLazyTables();

    assertEquals(0, hub.calls());
  }

  @Test
  void hubErrorsSurfaceAsWebClientException() {
    StubHubWebClient hub = new StubHubWebClient();
    hub.loadSpaceStatus = 500;
    hub.loadConnectorStatus = 500;
    Space lazyExtension = space("extension", "extension_table", true).withExtension(new Extension().withSpaceId("base"));
    Space lazy = space("standalone", "standalone_table", true);

    for (TestStep step : List.of(new TestStep(hub), new StepExceptionReturningTestStep(hub))) {
      ErrorResponseException spaceError = assertThrows(ErrorResponseException.class,
          () -> step.materializeTables(lazyExtension, Profile.FULL));
      assertEquals(500, spaceError.getStatusCode());
      ErrorResponseException connectorError = assertThrows(ErrorResponseException.class,
          () -> step.materializeTables(lazy, Profile.FULL));
      assertEquals(500, connectorError.getStatusCode());
      assertThrows(ErrorResponseException.class, () -> step.tableState(lazyExtension));
      assertThrows(ErrorResponseException.class, () -> step.getReadableRootTableName(lazyExtension));
      assertThrows(ErrorResponseException.class, () -> step.resolveReadableSpaceParams(lazyExtension));
    }
  }

  @Test
  void orFailVariantsMapHubErrorsToRetryableStepExceptions() {
    StubHubWebClient hub = new StubHubWebClient();
    hub.loadSpaceStatus = 500;
    hub.loadConnectorStatus = 500;
    TestStep step = new TestStep(hub);
    Space lazyExtension = space("extension", "extension_table", true).withExtension(new Extension().withSpaceId("base"));
    Space lazy = space("standalone", "standalone_table", true);

    List<StepException> exceptions = List.of(
        assertThrows(StepException.class, () -> step.materializeTablesOrFail(lazy, Profile.FULL)),
        assertThrows(StepException.class, () -> step.materializeTablesOrFail(lazyExtension, Profile.BARE, List.of())),
        assertThrows(StepException.class, () -> step.tableStateOrFail(lazyExtension)),
        assertThrows(StepException.class, () -> step.getReadableRootTableNameOrFail(lazyExtension)),
        assertThrows(StepException.class, () -> step.getReadableRootTableNameOrFail(lazyExtension, lazyExtension)),
        assertThrows(StepException.class, () -> step.resolveReadableSpaceParamsOrFail(lazyExtension)),
        assertThrows(StepException.class, () -> step.resolveReadableSpaceParamsOrFail(lazyExtension, lazyExtension)));
    for (StepException e : exceptions) {
      assertTrue(e.isRetryable());
      assertEquals("HTTP-500", e.getCode());
    }
  }

  @Test
  void writerOfConvertsAnUnreachableHubIntoWebClientException() throws Exception {
    StubHubWebClient hub = new StubHubWebClient();
    TestStep step = new TestStep(hub);
    String hubEndpoint = Config.instance.HUB_ENDPOINT;
    Config.instance.HUB_ENDPOINT = "http://localhost:" + unusedPort() + "/hub";
    try {
      WebClientException e = assertThrows(WebClientException.class,
          () -> step.materializeTables(space("standalone", "standalone_table", true), Profile.FULL));
      assertFalse(e instanceof ErrorResponseException);
    }
    finally {
      Config.instance.HUB_ENDPOINT = hubEndpoint;
    }
    //materializeTables never clears the flag
    assertEquals(0, hub.clearCalls.size());
  }

  @Test
  void dbReaderKeepsItsBaseBehavior() throws Exception {
    for (Class<?> stepClass : List.of(CountSpace.class, CopySpace.class, TaskedImportFilesToSpace.class, ExportSpaceToFiles.class,
        CreateIndex.class, DropIndexes.class, AnalyzeSpaceTable.class))
      for (Class<?> c = stepClass; c != SpaceBasedStep.class; c = c.getSuperclass())
        for (var method : c.getDeclaredMethods())
          assertFalse(method.getName().equals("dbReader"), c.getSimpleName() + " overrides dbReader()");
    assertEquals(SpaceBasedStep.class, SpaceBasedStep.class.getDeclaredMethod("dbReader").getDeclaringClass());
  }

  @Test
  void stepWithUnknownPropertyDeserializes() throws Exception {
    Map<String, Object> stepMap = new CountSpace().withSpaceId("space").toMap();
    stepMap.put("someFutureProperty", Map.of("x", 1));

    Step<?> step = XyzSerializable.fromMap(stepMap, Step.class);

    assertInstanceOf(CountSpace.class, step);
    assertEquals("space", ((CountSpace) step).getSpaceId());
  }

  @Test
  void serializedStepCarriesNoTransientLazyState() throws Exception {
    StubHubWebClient hub = new StubHubWebClient();
    TestStep step = new TestStep(hub);
    step.withSpaceId("lazy");
    step.clearLazyTableFlag(space("lazy", "lazy_table", true));
    transientSet(step, "existingLazyTables").add(CONNECTOR + "/public.lazy_table");
    assertEquals(1, transientSet(step, "lazyFlagClearRequested").size());

    String json = step.serialize();
    assertFalse(json.contains("existingLazyTables"));
    assertFalse(json.contains("lazyFlagClearRequested"));
    assertFalse(json.contains("lazy_table"));
    CountSpace restored = XyzSerializable.deserialize(json, CountSpace.class);
    assertTrue(transientSet(restored, "existingLazyTables").isEmpty());
    assertTrue(transientSet(restored, "lazyFlagClearRequested").isEmpty());

    String internalJson = XyzSerializable.serialize(step, XyzSerializable.Internal.class);
    assertFalse(internalJson.contains("existingLazyTables"));
    assertFalse(internalJson.contains("lazyFlagClearRequested"));
  }

  @Test
  void copySpaceDoesNotPersistItsReadableSourceParams() throws Exception {
    CopySpace step = new CopySpace().withSpaceId("source").withTargetSpaceId("target");
    Field field = CopySpace.class.getDeclaredField("readableSourceParams");
    field.setAccessible(true);
    field.set(step, Map.of(TABLE_NAME, "xyz_empty_space_v1"));

    for (String json : List.of(step.serialize(), XyzSerializable.serialize(step, XyzSerializable.Internal.class))) {
      assertFalse(json.contains("readableSourceParams"));
      assertFalse(json.contains("xyz_empty_space_v1"));
      assertNull(field.get(XyzSerializable.deserialize(json, CopySpace.class)));
    }
  }

  @Test
  void skippedNeverMaterializedSurvivesTheStoredStep() throws Exception {
    List<SpaceBasedStep<?>> steps = List.of(new CreateIndex().withSpaceId("lazy"), new AnalyzeSpaceTable().withSpaceId("lazy"));
    for (SpaceBasedStep<?> step : steps) {
      Field field = step.getClass().getDeclaredField("skippedNeverMaterialized");
      field.setAccessible(true);
      field.set(step, true);
      assertEquals("SUCCEEDED", String.valueOf(step.getExecutionState()));

      SpaceBasedStep<?> stored = XyzSerializable.deserialize(XyzSerializable.serialize(step, XyzSerializable.Internal.class),
          step.getClass());
      assertTrue((Boolean) field.get(stored), step.getClass().getSimpleName());
      assertEquals("SUCCEEDED", String.valueOf(stored.getExecutionState()));
    }
  }

  @Test
  void clearLazyTableFlagNeverThrows() {
    List<WebClientException> failures = List.of(
        new ErrorResponseException(new StubResponse(404)),
        new ErrorResponseException(new StubResponse(409)),
        new ErrorResponseException(new StubResponse(500)),
        new WebClientException("Error sending the request or receiving the response", new java.net.ConnectException("refused")));

    StubHubWebClient hub = new StubHubWebClient();
    TestStep step = new TestStep(hub);
    step.clearLazyTableFlag(space("cleared", "cleared_table", true));
    assertEquals(List.of(List.of("cleared", CONNECTOR, "cleared_table")), hub.clearCalls);
    assertEquals(0, appender.messages(Level.WARN, "lazyTable not cleared").size());
    assertEquals(1, appender.messages(Level.INFO, "lazyTable clear requested space=cleared connector=" + CONNECTOR).size());

    for (int i = 0; i < failures.size(); i++) {
      hub.clearFailure = failures.get(i);
      step.clearLazyTableFlag(space("failing-" + i, "failing_table", true));
      assertEquals(i + 1, appender.messages(Level.WARN, "lazyTable not cleared").size());
    }
    hub.clearFailure = new IllegalStateException("unexpected");
    step.clearLazyTableFlag(space("runtime", "runtime_table", true));
    assertEquals(failures.size() + 1, appender.messages(Level.WARN, "lazyTable not cleared").size());
    assertEquals(failures.size() + 2, hub.clearCalls.size());
  }

  @Test
  void clearLazyTableFlagCallsTheHubOncePerSpaceAndOnlyForFlaggedSnapshots() {
    StubHubWebClient hub = new StubHubWebClient();
    TestStep step = new TestStep(hub);

    step.clearLazyTableFlag(null);
    step.clearLazyTableFlag(space("eager", "eager_table", false));
    assertEquals(0, hub.calls());

    Space lazy = space("lazy", "lazy_table", true);
    step.clearLazyTableFlag(lazy);
    step.clearLazyTableFlag(lazy);
    assertEquals(1, hub.clearCalls.size());

    //A failed request is not repeated within the invocation either
    hub.clearFailure = new ErrorResponseException(new StubResponse(500));
    Space failing = space("failing", "failing_table", true);
    step.clearLazyTableFlag(failing);
    step.clearLazyTableFlag(failing);
    assertEquals(2, hub.clearCalls.size());

    //A legacy space has no table name
    hub.clearFailure = null;
    step.clearLazyTableFlag(space("legacy", null, true));
    assertEquals(new ArrayList<>(Arrays.asList("legacy", CONNECTOR, null)), hub.clearCalls.get(2));

    //The flag stays in the step's snapshot
    assertTrue(step.isLazy(lazy));
  }

  @Test
  void materializationBudgetIsCappedAndKeepsAReserve() {
    assertEquals(120_000, SpaceBasedStep.materializationBudgetMillis(null));
    assertEquals(120_000, SpaceBasedStep.materializationBudgetMillis(runtime(Integer.MAX_VALUE)));
    assertEquals(50_000, SpaceBasedStep.materializationBudgetMillis(runtime(60_000)));
    assertTrue(SpaceBasedStep.materializationBudgetMillis(runtime(5_000)) < 0);
  }

  @Test
  void isLazyReadsOnlyABooleanFlag() {
    TestStep step = new TestStep(new StubHubWebClient());
    assertTrue(step.isLazy(space("lazy", "t", true)));
    assertFalse(step.isLazy(space("eager", "t", false)));
    assertFalse(step.isLazy(new Space().withId("noStorage")));
    assertFalse(step.isLazy(null));
    Space stringFlag = space("string", "t", false);
    stringFlag.getStorage().getParams().put(LAZY_TABLE, "true");
    assertFalse(step.isLazy(stringFlag));
  }

  private static Space space(String id, String tableName, boolean lazy) {
    Map<String, Object> params = new HashMap<>();
    if (tableName != null)
      params.put(TABLE_NAME, tableName);
    if (lazy)
      params.put(LAZY_TABLE, true);
    return new Space().withId(id).withStorage(new ConnectorRef().withId(CONNECTOR).withParams(params));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Space> snapshots(SpaceBasedStep<?> step) throws Exception {
    Field field = SpaceBasedStep.class.getDeclaredField("cachedSpaces");
    field.setAccessible(true);
    return (Map<String, Space>) field.get(step);
  }

  @SuppressWarnings("unchecked")
  private static Set<String> transientSet(SpaceBasedStep<?> step, String name) throws Exception {
    Field field = SpaceBasedStep.class.getDeclaredField(name);
    field.setAccessible(true);
    return (Set<String>) field.get(step);
  }

  private static int unusedPort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
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

  @JsonTypeName("CountSpace")
  private static class TestStep extends CountSpace {
    private final HubWebClient webClient;

    private TestStep(HubWebClient webClient) {
      this.webClient = webClient;
    }

    @Override
    protected HubWebClient hubWebClient() {
      return webClient;
    }

    @Override
    public List<Load> getNeededResources() {
      return List.of();
    }

    void runPrepareLazyTables() throws Exception {
      prepareLazyTables();
    }
  }

  @JsonTypeName("TaskedImportFilesToSpace")
  private static class TestImportStep extends TaskedImportFilesToSpace {
    private final HubWebClient webClient;

    private TestImportStep(HubWebClient webClient) {
      this.webClient = webClient;
    }

    @Override
    protected HubWebClient hubWebClient() {
      return webClient;
    }

    void runPrepareLazyTables() throws Exception {
      prepareLazyTables();
    }
  }

  /**
   * A step whose error mapping returns a StepException instead of throwing it. The plain helpers never use it.
   */
  @JsonTypeName("CountSpace")
  private static class StepExceptionReturningTestStep extends TestStep {
    private StepExceptionReturningTestStep(HubWebClient webClient) {
      super(webClient);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T handleErrorResponse(ErrorResponseException e) {
      return (T) new StepException("mapped", e);
    }
  }

  private static class StubHubWebClient extends HubWebClient {
    private final List<List<String>> clearCalls = new ArrayList<>();
    private int loadSpaceStatus;
    private int loadConnectorStatus;
    private int loadCalls;
    private Exception clearFailure;

    private StubHubWebClient() {
      super("http://localhost");
    }

    int calls() {
      return loadCalls + clearCalls.size();
    }

    @Override
    public Space loadSpace(String spaceId, boolean skipLocalCache) throws WebClientException {
      loadCalls++;
      if (loadSpaceStatus > 0)
        throw new ErrorResponseException(new StubResponse(loadSpaceStatus));
      return space(spaceId, spaceId + "_table", true);
    }

    @Override
    public Connector loadConnector(String connectorId) throws WebClientException {
      loadCalls++;
      if (loadConnectorStatus > 0)
        throw new ErrorResponseException(new StubResponse(loadConnectorStatus));
      Connector connector = new Connector();
      connector.id = connectorId;
      connector.params = new HashMap<>();
      return connector;
    }

    @Override
    public void clearLazyTable(String spaceId, String connectorId, String tableName) throws WebClientException {
      clearCalls.add(new ArrayList<>(Arrays.asList(spaceId, connectorId, tableName)));
      if (clearFailure instanceof WebClientException webClientException)
        throw webClientException;
      if (clearFailure instanceof RuntimeException runtimeException)
        throw runtimeException;
    }
  }

  private record StubResponse(int statusCode) implements HttpResponse<byte[]> {
    @Override
    public HttpRequest request() {
      return HttpRequest.newBuilder(URI.create("http://localhost")).build();
    }

    @Override
    public Optional<HttpResponse<byte[]>> previousResponse() {
      return Optional.empty();
    }

    @Override
    public HttpHeaders headers() {
      return HttpHeaders.of(Map.of(), (name, value) -> true);
    }

    @Override
    public byte[] body() {
      return ("{\"type\":\"ErrorResponse\",\"error\":\"Exception\",\"errorMessage\":\"status " + statusCode + "\"}").getBytes();
    }

    @Override
    public Optional<SSLSession> sslSession() {
      return Optional.empty();
    }

    @Override
    public URI uri() {
      return URI.create("http://localhost");
    }

    @Override
    public Version version() {
      return Version.HTTP_1_1;
    }
  }

  private static class CapturingAppender extends AbstractAppender {
    private final List<LogEvent> events = new CopyOnWriteArrayList<>();

    private CapturingAppender() {
      super("SpaceBasedStepLazyTest", null, null, true, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
      events.add(event.toImmutable());
    }

    List<String> messages(Level level, String prefix) {
      return events.stream()
          .filter(event -> event.getLevel() == level)
          .map(event -> event.getMessage().getFormattedMessage())
          .filter(message -> message.startsWith(prefix))
          .toList();
    }
  }
}
