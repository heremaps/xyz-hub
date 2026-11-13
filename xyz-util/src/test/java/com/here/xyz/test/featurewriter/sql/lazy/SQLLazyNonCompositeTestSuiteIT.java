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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.test.featurewriter.SpaceWriter;
import com.here.xyz.test.featurewriter.sql.SQLSpaceWriter;
import com.here.xyz.test.featurewriter.sql.noncomposite.nohistory.SQLNonCompositeNoHistoryTestSuiteIT;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * All scenarios of {@link SQLNonCompositeNoHistoryTestSuiteIT} on a lazy space: its table is created by the first write,
 * and reads before that use the empty-space template. The assertions are the ones of the eager suite.
 */
@Tag("lazy-space-tables")
public class SQLLazyNonCompositeTestSuiteIT extends SQLNonCompositeNoHistoryTestSuiteIT {

  public static Stream<TestArgs> testScenarios() throws JsonProcessingException {
    return SQLNonCompositeNoHistoryTestSuiteIT.testScenarios();
  }

  @Override
  protected SpaceWriter spaceWriter() {
    return new SQLSpaceWriter(composite, getClass().getSimpleName()).withLazyTables(true);
  }

  @Override
  protected SpaceWriter preparationWriter() {
    return spaceWriter();
  }

  @ParameterizedTest
  @MethodSource("testScenarios")
  void start(TestArgs args) throws Exception {
    runTest(args);
  }
}
