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

import static com.here.xyz.events.UpdateStrategy.DEFAULT_UPDATE_STRATEGY;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.X;
import static com.here.xyz.events.UpdateStrategy.OnExists.RETAIN;
import static com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace.EntityPerLine.Feature;
import static com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace.EntityPerLine.FeatureCollection;
import static com.here.xyz.util.db.ConnectorParameters.TableLayout.NEW_LAYOUT;
import static com.here.xyz.util.db.ConnectorParameters.TableLayout.OLD_LAYOUT;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.execution.StepException;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TaskedImportWriterModeTest {
  private Config previousConfig;

  @BeforeEach
  void setUp() {
    previousConfig = Config.instance;
    Config.instance = new Config();
  }

  @AfterEach
  void tearDown() {
    Config.instance = previousConfig;
  }

  @Test
  void recognizesDefaultUpdateStrategyByValue() {
    UpdateStrategy equivalentDefault = new UpdateStrategy(
        DEFAULT_UPDATE_STRATEGY.onExists(),
        DEFAULT_UPDATE_STRATEGY.onNotExists(),
        null,
        null
    );

    assertTrue(TaskedImportFilesToSpace.isDefaultUpdateStrategy(equivalentDefault));
  }

  @Test
  void rejectsNonDefaultUpdateStrategy() {
    UpdateStrategy customStrategy = new UpdateStrategy(
        RETAIN,
        DEFAULT_UPDATE_STRATEGY.onNotExists(),
        null,
        null
    );

    assertFalse(TaskedImportFilesToSpace.isDefaultUpdateStrategy(customStrategy));
    assertFalse(TaskedImportFilesToSpace.isDefaultUpdateStrategy(null));
  }

  @Test
  void enablesExpressImportOnlyForSupportedStagedImports() {
    assertTrue(TaskedImportFilesToSpace.supportsExpressImport(
        true, Feature, DEFAULT_UPDATE_STRATEGY, OLD_LAYOUT, DEFAULT));
    assertTrue(TaskedImportFilesToSpace.supportsExpressImport(
        true, Feature, DEFAULT_UPDATE_STRATEGY, null, EXTENSION));
    assertTrue(TaskedImportFilesToSpace.supportsExpressImport(
        true, Feature, DEFAULT_UPDATE_STRATEGY, OLD_LAYOUT, null));

    assertFalse(TaskedImportFilesToSpace.supportsExpressImport(
        false, Feature, DEFAULT_UPDATE_STRATEGY, OLD_LAYOUT, DEFAULT));
    assertFalse(TaskedImportFilesToSpace.supportsExpressImport(
        true, FeatureCollection, DEFAULT_UPDATE_STRATEGY, OLD_LAYOUT, DEFAULT));
    assertFalse(TaskedImportFilesToSpace.supportsExpressImport(
        true, Feature, DEFAULT_UPDATE_STRATEGY, NEW_LAYOUT, DEFAULT));
    assertFalse(TaskedImportFilesToSpace.supportsExpressImport(
        true, Feature, DEFAULT_UPDATE_STRATEGY, OLD_LAYOUT, X));
    assertFalse(TaskedImportFilesToSpace.supportsExpressImport(
        true, Feature, DEFAULT_UPDATE_STRATEGY, OLD_LAYOUT, SUPER));
  }

  @Test
  void rejectsSuperContextDuringValidationRegardlessOfWriterMode() {
    for (Boolean expressImport : new Boolean[] {null, false, true}) {
      TaskedImportFilesToSpace step = new TaskedImportFilesToSpace().withContext(SUPER);
      step.setExpressImport(expressImport);

      ValidationException exception = assertThrows(ValidationException.class, step::validate);
      assertEquals("Importing data with context SUPER is not supported.", exception.getMessage());
    }
  }

  @Test
  void rejectsSuperContextEvenWithPersistedWriterMode() {
    for (Boolean expressImport : new Boolean[] {null, false, true}) {
      TaskedImportFilesToSpace step = new TaskedImportFilesToSpace().withContext(SUPER);
      step.setExpressImport(expressImport);

      assertThrows(StepException.class, step::useExpressImport);
    }
  }
}
