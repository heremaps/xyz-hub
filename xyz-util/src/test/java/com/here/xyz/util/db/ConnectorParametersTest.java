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

package com.here.xyz.util.db;

import static com.here.xyz.util.Random.randomAlpha;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
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
import org.junit.jupiter.api.parallel.Isolated;

//The tests count log lines, which they miss while other classes run concurrently
@Isolated
public class ConnectorParametersTest {
  private static final String LOGGER = ConnectorParameters.class.getName();
  private static Level previousLevel;
  private CapturingAppender appender;

  @BeforeAll
  public static void raiseLogLevel() {
    previousLevel = LogManager.getLogger(LOGGER).getLevel();
    Configurator.setLevel(LOGGER, Level.WARN);
  }

  @AfterAll
  public static void restoreLogLevel() {
    Configurator.setLevel(LOGGER, previousLevel);
  }

  @BeforeEach
  public void startCapturing() {
    appender = new CapturingAppender();
    appender.start();
    ((Logger) LogManager.getLogger(LOGGER)).addAppender(appender);
  }

  @AfterEach
  public void stopCapturing() {
    ((Logger) LogManager.getLogger(LOGGER)).removeAppender(appender);
    appender.stop();
  }

  private static ConnectorParameters parse(String name, Object value) {
    Map<String, Object> params = new HashMap<>();
    params.put(name, value);
    return ConnectorParameters.fromMap(params);
  }

  //Values that are neither a boolean nor a "true" / "false" string
  private static List<Object> invalidFlagValues() {
    return Arrays.asList("yes", 1, "25s", Map.of("on", true), List.of(true));
  }

  @Test
  public void lazyTableCreationIsLenient() {
    assertFalse(ConnectorParameters.fromMap(Map.of()).isLazyTableCreation());
    for (Object value : invalidFlagValues())
      assertFalse(parse("lazyTableCreation", value).isLazyTableCreation(), "Value " + value);
    assertFalse(parse("lazyTableCreation", null).isLazyTableCreation());
    assertFalse(parse("lazyTableCreation", false).isLazyTableCreation());
    assertFalse(parse("lazyTableCreation", "false").isLazyTableCreation());
    assertTrue(parse("lazyTableCreation", true).isLazyTableCreation());
    assertTrue(parse("lazyTableCreation", "TRUE").isLazyTableCreation());
  }

  @Test
  public void lazyCheckOnReplicaIsLenient() {
    assertFalse(ConnectorParameters.fromMap(Map.of()).isLazyCheckOnReplica());
    for (Object value : invalidFlagValues())
      assertFalse(parse("lazyCheckOnReplica", value).isLazyCheckOnReplica(), "Value " + value);
    assertFalse(parse("lazyCheckOnReplica", null).isLazyCheckOnReplica());
    assertFalse(parse("lazyCheckOnReplica", "False").isLazyCheckOnReplica());
    assertTrue(parse("lazyCheckOnReplica", true).isLazyCheckOnReplica());
    assertTrue(parse("lazyCheckOnReplica", "TRUE").isLazyCheckOnReplica());
  }

  @Test
  public void lazyMaterializationMaxBudgetIsLenientAndClamped() {
    assertEquals(25_000, ConnectorParameters.fromMap(Map.of()).getLazyMaterializationMaxBudget());
    for (Object value : Arrays.asList("yes", "25s", null, Map.of("ms", 5000), List.of(5000), true, -1))
      assertEquals(25_000, parse("lazyMaterializationMaxBudget", value).getLazyMaterializationMaxBudget(), "Value " + value);
    assertEquals(3_000, parse("lazyMaterializationMaxBudget", 1).getLazyMaterializationMaxBudget());
    assertEquals(3_000, parse("lazyMaterializationMaxBudget", 1000).getLazyMaterializationMaxBudget());
    assertEquals(12_000, parse("lazyMaterializationMaxBudget", 12000).getLazyMaterializationMaxBudget());
    assertEquals(12_000, parse("lazyMaterializationMaxBudget", "12000").getLazyMaterializationMaxBudget());
    assertEquals(12_000, parse("lazyMaterializationMaxBudget", 12000.9).getLazyMaterializationMaxBudget());
    assertEquals(25_000, parse("lazyMaterializationMaxBudget", 99999).getLazyMaterializationMaxBudget());
  }

  @Test
  public void scriptInstallTimeoutSecondsAbsent() {
    assertEquals(0, ConnectorParameters.fromMap(Map.of("onDemandIdxLimit", 7)).getScriptInstallTimeoutSeconds());
    assertEquals(List.of(), appender.messages("scriptInstallTimeoutSeconds="));
  }

  @Test
  public void scriptInstallTimeoutSecondsNull() {
    //The mapper drops null map values (NON_NULL inclusion) before the setter runs, so null is the same as absent
    assertEquals(0, parse("scriptInstallTimeoutSeconds", null).getScriptInstallTimeoutSeconds());
    assertEquals(List.of(), appender.messages("scriptInstallTimeoutSeconds="));
  }

  @Test
  public void scriptInstallTimeoutSecondsInvalid() {
    List<Object> list = List.of(20, randomAlpha(5));
    for (Object value : Arrays.asList("20s", list)) {
      assertEquals(0, parse("scriptInstallTimeoutSeconds", value).getScriptInstallTimeoutSeconds(), "Value " + value);
      assertEquals(1, appender.messages("scriptInstallTimeoutSeconds=" + value + ",").size(), "WARN lines for " + value);
    }
  }

  @Test
  public void scriptInstallTimeoutSecondsClamped() {
    assertEquals(5, parse("scriptInstallTimeoutSeconds", 1).getScriptInstallTimeoutSeconds());
    assertEquals(5, parse("scriptInstallTimeoutSeconds", "1").getScriptInstallTimeoutSeconds());
    assertEquals(20, parse("scriptInstallTimeoutSeconds", 20).getScriptInstallTimeoutSeconds());
    assertEquals(20, parse("scriptInstallTimeoutSeconds", "20").getScriptInstallTimeoutSeconds());
    assertEquals(25, parse("scriptInstallTimeoutSeconds", 99).getScriptInstallTimeoutSeconds());
    assertEquals(12, parse("scriptInstallTimeoutSeconds", 12.9).getScriptInstallTimeoutSeconds());
    assertEquals(List.of(), appender.messages("scriptInstallTimeoutSeconds="));
  }

  @Test
  public void invalidValueIsLoggedOncePerContainer() {
    //The set of logged values is static, so the value must be unique to this test run
    String invalid = "yes-" + randomAlpha(5);
    //Two different maps, so the second one is not served from the cache of parsed parameters
    ConnectorParameters first = ConnectorParameters.fromMap(Map.of("lazyTableCreation", invalid));
    ConnectorParameters second = ConnectorParameters.fromMap(Map.of("lazyTableCreation", invalid, "onDemandIdxLimit", 12));

    assertFalse(first.isLazyTableCreation());
    assertFalse(second.isLazyTableCreation());
    assertEquals(12, second.getOnDemandIdxLimit());
    assertEquals(1, appender.messages("lazyTableCreation=" + invalid + ",").size());
  }

  @Test
  public void existingParamsKeepTheirParsing() {
    ConnectorParameters params = ConnectorParameters.fromMap(Map.of("propertySearch", true, "autoIndexing", true,
        "enableHashedSpaceId", true, "onDemandIdxLimit", 12, "dbMaxPoolSize", 3, "readOnly", true, "lazyTableCreation", true));

    assertTrue(params.isPropertySearch());
    assertTrue(params.isAutoIndexing());
    assertTrue(params.isEnableHashedSpaceId());
    assertFalse(params.isMvtSupport());
    assertEquals(12, params.getOnDemandIdxLimit());
    assertEquals(3, params.getDbMaxPoolSize());
    assertTrue(params.isReadOnly());
    assertTrue(params.isLazyTableCreation());
  }

  private static class CapturingAppender extends AbstractAppender {
    private final List<LogEvent> events = new CopyOnWriteArrayList<>();

    CapturingAppender() {
      super(ConnectorParametersTest.class.getSimpleName() + "-" + randomAlpha(5), null, null, true, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
      events.add(event.toImmutable());
    }

    /**
     * @return The messages of the captured WARN events, which contain the provided text
     */
    List<String> messages(String containing) {
      return events.stream()
          .filter(event -> event.getLevel() == Level.WARN)
          .map(event -> event.getMessage().getFormattedMessage())
          .filter(message -> message.contains(containing))
          .toList();
    }
  }
}
