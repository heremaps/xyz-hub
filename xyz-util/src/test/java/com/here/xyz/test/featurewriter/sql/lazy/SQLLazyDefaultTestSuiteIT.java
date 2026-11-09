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

package com.here.xyz.test.featurewriter.sql.lazy;

import static org.junit.Assert.assertFalse;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.test.featurewriter.SpaceWriter;
import com.here.xyz.test.featurewriter.sql.SQLSpaceWriter;
import com.here.xyz.test.featurewriter.sql.composite.nohistory.SQLComposite_DEFAULT_NoHistoryTestSuiteIT;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The scenarios of {@link SQLComposite_DEFAULT_NoHistoryTestSuiteIT} whose base holds no feature, on lazy spaces: the base is never
 * written and is read from the empty-space template, the extension is created by its first write. The assertions are the ones of
 * the eager suite.
 */
public class SQLLazyDefaultTestSuiteIT extends SQLComposite_DEFAULT_NoHistoryTestSuiteIT {

  public static Stream<TestArgs> testScenarios() {
    return SQLComposite_DEFAULT_NoHistoryTestSuiteIT.testScenarios().filter(args -> !args.featureExistsInSuper());
  }

  @Override
  protected SpaceWriter spaceWriter() {
    return new SQLSpaceWriter(composite, getClass().getSimpleName()).withLazyTables(true);
  }

  @Override
  protected SpaceWriter preparationWriter() {
    return spaceWriter();
  }

  //Runs before the cleanup of the superclass
  @AfterEach
  public void baseStaysNeverWritten() throws Exception {
    assertFalse("The base must still be read from the template",
        new SQLSpaceWriter(composite, getClass().getSimpleName()).tableExists(SpaceContext.SUPER));
  }

  @ParameterizedTest
  @MethodSource("testScenarios")
  void start(TestArgs args) throws Exception {
    runTest(args);
  }
}
