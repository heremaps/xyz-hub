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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Runs the scenarios of
 * {@code com.here.xyz.test.featurewriter.sql.composite.nohistory.SQLComposite_EXTENSION_NoHistoryTestSuiteIT}
 * against the express import writer.
 *
 * <p>For context {@code EXTENSION} the express writer never looks at the super space, it writes into the
 * extension space alone.</p>
 */
public class ExpressComposite_EXTENSION_NoHistoryIT extends ExpressTestSuite {

  //NOTE: Mirrors the modifyArgs of the referenced suite
  @Override
  protected TestArgs modifyArgs(TestArgs args) {
    return args.withComposite(true).withContext(EXTENSION).withFeatureExistsInExtension(args.featureExists());
  }

  /*
   * NOTE: SQLComposite_EXTENSION_NoHistoryTestSuiteIT does not declare own scenarios, it inherits them from the
   * non-composite suite and only changes the arguments through modifyArgs. Therefore the scenarios are referenced
   * at their declaring class.
   */
  @ParameterizedTest
  @MethodSource("com.here.xyz.test.featurewriter.sql.noncomposite.nohistory."
      + "SQLNonCompositeNoHistoryTestSuiteIT#testScenarios")
  void start(TestArgs args) throws Exception {
    runTest(args);
  }
}
