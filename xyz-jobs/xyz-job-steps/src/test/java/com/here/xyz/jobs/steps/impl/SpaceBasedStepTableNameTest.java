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

import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.impl.transport.CountSpace;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.models.hub.Space.Extension;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SpaceBasedStepTableNameTest {

  @BeforeAll
  static void setUpConfig() {
    if (Config.instance == null)
      new Config();
  }

  @Test
  void includesTheRootAndFirstLevelPhysicalTableNames() throws Exception {
    Space base = space("base", "base_table");
    Space composite = space("composite", "composite_table")
        .withExtension(new Extension().withSpaceId(base.getId()));
    TestStep step = new TestStep(Map.of(base.getId(), base));

    Map<String, Object> params = step.resolveParams(composite);
    Map<String, Object> extensionParams = extensionParams(params);

    assertEquals("composite_table", params.get(TABLE_NAME));
    assertEquals("base", extensionParams.get("spaceId"));
    assertEquals("base_table", extensionParams.get(TABLE_NAME));
  }

  @Test
  void includesPhysicalTableNamesForTwoExtensionLevels() throws Exception {
    Space base = space("base", "base_table");
    Space intermediate = space("intermediate", "intermediate_table")
        .withExtension(new Extension().withSpaceId(base.getId()));
    Space composite = space("composite", "composite_table")
        .withExtension(new Extension().withSpaceId(intermediate.getId()));
    TestStep step = new TestStep(Map.of(base.getId(), base, intermediate.getId(), intermediate));

    Map<String, Object> params = step.resolveParams(composite);
    Map<String, Object> intermediateParams = extensionParams(params);
    Map<String, Object> baseParams = extensionParams(intermediateParams);

    assertEquals("composite_table", params.get(TABLE_NAME));
    assertEquals("intermediate_table", intermediateParams.get(TABLE_NAME));
    assertEquals("base_table", baseParams.get(TABLE_NAME));
  }

  @Test
  void leavesLegacyTableResolutionToTheConnector() throws Exception {
    Space legacySpace = space("legacy", null);

    Map<String, Object> params = new TestStep(Map.of()).resolveParams(legacySpace);

    assertFalse(params.containsKey(TABLE_NAME));
  }

  @Test
  void refreshesTheSpaceOncePerStep() throws Exception {
    Space currentIncarnation = space("recreated", "current_table");
    RecordingHubWebClient webClient = new RecordingHubWebClient(currentIncarnation);
    CachingTestStep step = new CachingTestStep(webClient);

    assertSame(currentIncarnation, step.load("recreated"));
    assertSame(currentIncarnation, step.load("recreated"));
    assertEquals(1, webClient.loadCount);
    assertTrue(webClient.lastSkipLocalCache);
  }

  @Test
  void preservesTheResolvedPhysicalTableAcrossStepSerialization() throws Exception {
    Space originalIncarnation = space("recreated", "original_table");
    CachingTestStep originalStep = new CachingTestStep(new RecordingHubWebClient(originalIncarnation));
    originalStep.withSpaceId("recreated");
    originalStep.load("recreated");

    CountSpace restoredStep = XyzSerializable.deserialize(originalStep.serialize(), CountSpace.class);
    Map<String, Object> restoredParams = restoredStep.resolveSpaceParams(restoredStep.space("recreated"));

    assertEquals("original_table", restoredParams.get(TABLE_NAME));
  }

  private static Space space(String id, String tableName) {
    Map<String, Object> params = tableName == null ? null : new HashMap<>(Map.of(TABLE_NAME, tableName));
    return new Space().withId(id).withStorage(new ConnectorRef().withId("psql").withParams(params));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> extensionParams(Map<String, Object> params) {
    return (Map<String, Object>) params.get("extends");
  }

  private static class TestStep extends CountSpace {
    private final Map<String, Space> spaces;

    private TestStep(Map<String, Space> spaces) {
      this.spaces = spaces;
    }

    private Map<String, Object> resolveParams(Space space) throws Exception {
      return resolveSpaceParams(space);
    }

    @Override
    protected Space space(String spaceId) {
      return spaces.get(spaceId);
    }
  }

  @JsonTypeName("CountSpace")
  private static class CachingTestStep extends CountSpace {
    private final HubWebClient webClient;

    private CachingTestStep(HubWebClient webClient) {
      this.webClient = webClient;
    }

    private Space load(String spaceId) throws WebClientException {
      return space(spaceId);
    }

    @Override
    protected HubWebClient hubWebClient() {
      return webClient;
    }
  }

  private static class RecordingHubWebClient extends HubWebClient {
    private final Space space;
    private int loadCount;
    private boolean lastSkipLocalCache;

    private RecordingHubWebClient(Space space) {
      super("http://localhost");
      this.space = space;
    }

    @Override
    public Space loadSpace(String spaceId, boolean skipLocalCache) {
      loadCount++;
      lastSkipLocalCache = skipLocalCache;
      return space;
    }
  }
}