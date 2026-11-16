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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.test.featurewriter.SpaceWriter;
import com.here.xyz.test.featurewriter.sql.SQLSpaceWriter;
import com.here.xyz.test.featurewriter.sql.composite.nohistory.SQLComposite_SUPER_NoHistoryTestSuiteIT;
import com.here.xyz.test.featurewriter.sql.noncomposite.nohistory.SQLNonCompositeNoHistoryTestSuiteIT;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The scenarios of {@link SQLComposite_SUPER_NoHistoryTestSuiteIT} whose base holds no feature, on lazy spaces: the base is created
 * by the write of the test, and the extension is never written and is read from the empty-space template. The assertions are the
 * ones of the eager suite.
 */
@Tag("lazy-space-tables")
public class SQLLazySuperTestSuiteIT extends SQLComposite_SUPER_NoHistoryTestSuiteIT {

  //The SUPER suite sets featureExistsInSuper from featureExists
  public static Stream<TestArgs> testScenarios() throws JsonProcessingException {
    return SQLNonCompositeNoHistoryTestSuiteIT.testScenarios().filter(args -> !args.featureExists());
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
  public void extensionStaysNeverWritten() throws Exception {
    assertFalse("The extension must still be read from the template",
        new SQLSpaceWriter(composite, getClass().getSimpleName()).tableExists(SpaceContext.EXTENSION));
  }

  @ParameterizedTest
  @MethodSource("testScenarios")
  void start(TestArgs args) throws Exception {
    runTest(args);
  }
}
