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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.XyzSerializable;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Space.Extension;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class AnalyzeSpaceTableStepTest extends StepTest {

  @Test
  public void analyzeEagerSpace() throws Exception {
    deactivate(SPACE_ID);

    sendLambdaStepRequestBlock(new AnalyzeSpaceTable().withSpaceId(SPACE_ID).withJobId(JOB_ID), true);

    assertTrue(loadSpace(SPACE_ID).isActive());
  }

  @Test
  public void neverWrittenLazySpaceIsSkipped() throws Exception {
    Space lazy = createLazyTestSpace(SPACE_ID + "-lazy", LAZY_CONNECTOR);
    deactivate(lazy.getId());

    sendLambdaStepRequestBlock(new AnalyzeSpaceTable().withSpaceId(lazy.getId()).withJobId(JOB_ID), true);

    assertTrue(loadSpace(lazy.getId()).isActive());
    assertEquals(0, relationCount(DB_URL, tableName(lazy)));
    assertTrue(isFlagged(lazy.getId()));
  }

  @Test
  public void neverWrittenLazySpaceCompletesThroughTheStateCheck() throws Exception {
    Space lazy = createLazyTestSpace(SPACE_ID + "-lazy", LAZY_CONNECTOR);
    deactivate(lazy.getId());

    //The completion path of production: execute() starts no query, the next state check loads the stored step
    AnalyzeSpaceTable step = new AnalyzeSpaceTable().withSpaceId(lazy.getId()).withJobId(JOB_ID);
    step.execute(false);
    assertEquals("SUCCEEDED", String.valueOf(step.getExecutionState()));
    AnalyzeSpaceTable stored = XyzSerializable.deserialize(XyzSerializable.serialize(step, XyzSerializable.Internal.class),
        AnalyzeSpaceTable.class);
    assertEquals("SUCCEEDED", String.valueOf(stored.getExecutionState()));

    assertEquals(0, relationCount(DB_URL, tableName(lazy)));
    assertTrue(isFlagged(lazy.getId()));
    //Only the success handling of the state check re-activates the space
    assertFalse(loadSpace(lazy.getId()).isActive());
  }

  @Test
  public void materializedExtensionOfNeverWrittenLazyBaseIsAnalyzed() throws Exception {
    Space base = createLazyTestSpace(SPACE_ID + "-base", LAZY_CONNECTOR);
    Space extension = createTestSpace(new Space().withId(SPACE_ID + "-ext").withExtension(new Extension().withSpaceId(base.getId())));
    //Materializes only the extension
    putFeatureToSpace(extension.getId(), simpleFeature("f1"));
    assertTrue(tableExists(DB_URL, tableName(extension)));
    deactivate(extension.getId());

    sendLambdaStepRequestBlock(new AnalyzeSpaceTable().withSpaceId(extension.getId()).withJobId(JOB_ID), true);

    assertTrue(loadSpace(extension.getId()).isActive());
    assertEquals(0, relationCount(DB_URL, tableName(base)));
  }

  private void deactivate(String spaceId) {
    patchSpace(spaceId, Map.of("active", false));
    assertFalse(loadSpace(spaceId).isActive());
  }
}
