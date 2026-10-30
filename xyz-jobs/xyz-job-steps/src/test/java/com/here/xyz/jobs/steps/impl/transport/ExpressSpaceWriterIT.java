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

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

import com.here.xyz.events.UpdateStrategy.OnExists;
import com.here.xyz.events.UpdateStrategy.OnNotExists;
import com.here.xyz.events.UpdateStrategy.OnVersionConflict;
import static com.here.xyz.jobs.steps.impl.transport.ExpressSpaceWriter.WriterMode.EXPRESS;
import static com.here.xyz.jobs.steps.impl.transport.ExpressSpaceWriter.WriterMode.FEATURE_WRITER;
import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.I;
import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.U;
import com.here.xyz.test.featurewriter.TestSuite;
import static com.here.xyz.test.featurewriter.TestSuite.TableOperation.INSERT;
import static com.here.xyz.test.featurewriter.TestSuite.TableOperation.UPDATE;

/**
 * Drives the express import writer through the scenario runner and the assertions of {@link TestSuite}, and
 * verifies the writer selection including the fallback to the FeatureWriter.
 *
 * <p>The scenarios below are taken from
 * {@code com.here.xyz.test.featurewriter.sql.noncomposite.nohistory.SQLNonCompositeNoHistoryTestSuiteIT}. They
 * are spelled out here to make the wiring and the routing explicit; the reuse of the complete scenario tables
 * happens in the suites which extend those classes.</p>
 *
 * <p>Note that the pre-existing feature of the scenarios below is written by the PLV8 FeatureWriter, because
 * {@code TestSuite.writeFeatureForPreparation} always uses a {@code SQLSpaceWriter}. So this does not only check
 * the express writer in isolation, it checks that it correctly continues on state produced by the
 * FeatureWriter.</p>
 */
public class ExpressSpaceWriterIT extends ExpressTestSuite {

  /**
   * Scenario "1": the feature does not exist yet and gets created.
   */
  @Test
  void createsNotExistingFeature() throws Exception {
    runTest(new TestArgs("express-create", false, false, false, true, null, null, UserIntent.WRITE,
        OnNotExists.CREATE, null, null, null, null,
        new TestAssertions(INSERT, I)));
    assertEquals(EXPRESS, usedWriterMode(), "OnNotExists.CREATE is the default, so the express writer applies.");
  }

  /**
   * Scenario "5": the feature exists and gets replaced.
   */
  @Test
  void replacesExistingFeature() throws Exception {
    runTest(new TestArgs("express-replace", false, true, UserIntent.WRITE,
        null, OnExists.REPLACE, null, null, null,
        new TestAssertions(UPDATE, U)));
    assertEquals(EXPRESS, usedWriterMode(), "OnExists.REPLACE is the default, so the express writer applies.");
  }

  /**
   * Scenario "6". {@code OnExists.RETAIN} is not supported by the express writer, so this asserts the fallback:
   * the write is performed by the FeatureWriter and still fulfills the original expectations of the scenario.
   */
  @Test
  void fallsBackToFeatureWriterForUnsupportedUpdateStrategy() throws Exception {
    runTest(new TestArgs("express-retain", false, true, UserIntent.WRITE,
        null, OnExists.RETAIN, null, null, null,
        new TestAssertions()));
    assertEquals(FEATURE_WRITER, usedWriterMode(), "OnExists.RETAIN is not supported by the express writer.");
  }

  /**
   * Scenario "1 (with conflict detection)". The express writer performs no version conflict detection per
   * feature, so any non-null {@code OnVersionConflict} has to fall back as well.
   */
  @Test
  void fallsBackToFeatureWriterForConflictDetection() throws Exception {
    runTest(new TestArgs("express-conflict-detection", false, false, false, true, null, null, UserIntent.WRITE,
        OnNotExists.CREATE, null, OnVersionConflict.ERROR, null, null,
        new TestAssertions(INSERT, I)));
    assertEquals(FEATURE_WRITER, usedWriterMode(), "Conflict detection is not supported by the express writer.");
  }
}
