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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import com.here.xyz.jobs.steps.impl.transport.ExpressSpaceWriter.WriterMode;

/**
 * Runs the scenarios of
 * {@code com.here.xyz.test.featurewriter.sql.composite.nohistory.SQLComposite_SUPER_NoHistoryTestSuiteIT}
 * against the express import writer.
 *
 * <p>Writing with context {@code SUPER} is not supported by the express writer, so every scenario of this suite
 * is expected to fall back to the FeatureWriter. Keeping the suite is worthwhile as a regression check: it fails
 * if the express writer ever starts claiming context {@code SUPER}, which
 * {@code TaskedImportFilesToSpace.validate} rejects outright.</p>
 */
public class ExpressComposite_SUPER_NoHistoryIT extends ExpressTestSuite {

  //NOTE: Mirrors the modifyArgs of the referenced suite
  @Override
  protected TestArgs modifyArgs(TestArgs args) {
    return args.withComposite(true).withContext(SUPER).withFeatureExistsInSuper(args.featureExists());
  }

  /*
   * NOTE: SQLComposite_SUPER_NoHistoryTestSuiteIT does not declare own scenarios, it inherits them from the
   * non-composite suite and only changes the arguments through modifyArgs. Therefore the scenarios are referenced
   * at their declaring class.
   */
  @ParameterizedTest
  @MethodSource("com.here.xyz.test.featurewriter.sql.noncomposite.nohistory."
      + "SQLNonCompositeNoHistoryTestSuiteIT#testScenarios")
  void start(TestArgs args) throws Exception {
    runTest(args);
    Assertions.assertEquals(WriterMode.FEATURE_WRITER, usedWriterMode(),
        "Context SUPER has to be written by the FeatureWriter.");
  }
}
