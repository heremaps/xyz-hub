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

package com.here.xyz.jobs.steps.impl.transport;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.UpdateStrategy.OnExists;
import com.here.xyz.events.UpdateStrategy.OnMergeConflict;
import com.here.xyz.events.UpdateStrategy.OnNotExists;
import com.here.xyz.events.UpdateStrategy.OnVersionConflict;
import com.here.xyz.jobs.steps.impl.transport.ExpressSpaceWriter.WriterMode;
import org.junit.jupiter.api.Test;

/**
 * Pins which write implementation the reused FeatureWriter scenarios are routed to.
 *
 * <h2>Why this test exists</h2>
 * <p>{@link ExpressSpaceWriter} falls back to the PLV8 FeatureWriter for everything the express writer does not
 * support. That is the production behavior, but it also means the express suites would stay green even if the
 * express writer stopped being selected altogether. This test therefore pins the selection itself, and
 * {@link ExpressSpaceWriterIT} additionally asserts at runtime that a supported scenario really is written by the
 * express writer.</p>
 *
 * <p>Runs without a database, because the selection only depends on the update strategy and the space context.</p>
 */
public class ExpressRoutingGuardTest {

  /**
   * The scenarios of the reused suites express "use the default" as {@code null}, while {@code FeatureWriter.js}
   * applies its defaults itself. If this normalization ever got lost, nearly every scenario would silently fall
   * back and the express writer would not be covered at all.
   */
  @Test
  void routesDefaultUpdateStrategyToExpressWriterIncludingNullValues() {
    assertEquals(WriterMode.EXPRESS, writerModeFor(null, null, DEFAULT),
        "An unset update strategy is the default one, so the express writer applies.");
    assertEquals(WriterMode.EXPRESS, writerModeFor(OnExists.REPLACE, OnNotExists.CREATE, DEFAULT),
        "The explicitly spelled out default update strategy has to be routed to the express writer.");
    assertEquals(WriterMode.EXPRESS, writerModeFor(null, OnNotExists.CREATE, DEFAULT),
        "Scenario 1 of the reused suites only sets onNotExists, it still is the default strategy.");
    assertEquals(WriterMode.EXPRESS, writerModeFor(OnExists.REPLACE, null, DEFAULT),
        "Scenario 5 of the reused suites only sets onExists, it still is the default strategy.");
  }

  /**
   * Contexts {@code DEFAULT} and {@code EXTENSION} are the ones a tasked import can use.
   */
  @Test
  void routesSupportedSpaceContextsToExpressWriter() {
    assertEquals(WriterMode.EXPRESS, writerModeFor(null, null, DEFAULT));
    assertEquals(WriterMode.EXPRESS, writerModeFor(null, null, EXTENSION));
  }

  /**
   * Every dimension the express writer does not implement has to end up at the FeatureWriter.
   */
  @Test
  void routesUnsupportedScenariosToFeatureWriter() {
    assertEquals(WriterMode.FEATURE_WRITER, writerModeFor(OnExists.RETAIN, null, DEFAULT),
        "The express writer has no OnExists handling.");
    assertEquals(WriterMode.FEATURE_WRITER, writerModeFor(OnExists.ERROR, null, DEFAULT),
        "The express writer has no OnExists handling.");
    assertEquals(WriterMode.FEATURE_WRITER, writerModeFor(OnExists.DELETE, null, DEFAULT),
        "A deletion through the update strategy is not supported, only a deleted flag on the feature is.");
    assertEquals(WriterMode.FEATURE_WRITER, writerModeFor(null, OnNotExists.RETAIN, DEFAULT),
        "The express writer has no OnNotExists handling.");
    assertEquals(WriterMode.FEATURE_WRITER, writerModeFor(null, OnNotExists.ERROR, DEFAULT),
        "The express writer has no OnNotExists handling.");

    for (OnVersionConflict onVersionConflict : OnVersionConflict.values())
      assertEquals(WriterMode.FEATURE_WRITER,
          ExpressSpaceWriter.writerModeFor(null, null, onVersionConflict, null, false, DEFAULT),
          "The express writer detects version conflicts only per batch, never per feature: " + onVersionConflict);

    for (OnMergeConflict onMergeConflict : OnMergeConflict.values())
      assertEquals(WriterMode.FEATURE_WRITER,
          ExpressSpaceWriter.writerModeFor(null, null, null, onMergeConflict, false, DEFAULT),
          "The express writer can not merge: " + onMergeConflict);

    assertEquals(WriterMode.FEATURE_WRITER,
        ExpressSpaceWriter.writerModeFor(null, null, null, null, true, DEFAULT),
        "The express writer can not patch a partial input onto the HEAD feature.");

    assertEquals(WriterMode.FEATURE_WRITER, writerModeFor(null, null, SUPER),
        "Writing with context SUPER is rejected by TaskedImportFilesToSpace.validate altogether.");
  }

  private static WriterMode writerModeFor(OnExists onExists, OnNotExists onNotExists, SpaceContext spaceContext) {
    return ExpressSpaceWriter.writerModeFor(onExists, onNotExists, null, null, false, spaceContext);
  }
}
