/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.test.featurewriter.rest.noncomposite.history;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.test.featurewriter.rest.RestTestSuite;
import com.here.xyz.test.featurewriter.sql.noncomposite.history.SQLNonCompositeWithHistoryTestSuiteIT;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class HubNonCompositeWithHistoryTestSuiteIT extends RestTestSuite {

  public static Stream<TestArgs> testScenarios() throws JsonProcessingException {
    Set<String> ignoredTests = Set.of(
        "12", //FIXME: Issue in Hub: No version conflict is thrown in that case
        "18", //FIXME: Issue in Hub: Concurrently written attribute is sometimes not merged into the result correctly (flickering) [will be fixed by new FeatureWriter impl]
        "19", //FIXME: Issue in Hub: MergeConflictResolution is not supported / The service does not distinguish between version- & merge-conflicts
        "20", //FIXME: Issue in Hub: MergeConflictResolution is not supported / The service does not distinguish between version- & merge-conflicts
        "21" //FIXME: Issue in Hub: MergeConflictResolution is not supported / The service does not distinguish between version- & merge-conflicts
    );

    return SQLNonCompositeWithHistoryTestSuiteIT.testScenarios()
        .filter(args -> !ignoredTests.contains(args.testName()));
  }

  //TODO: Check missing version conflict errors
  //TODO: Check creation timestamp issues (actually in case of history the test should not treat a table.INSERT as creation, but a feature.I should be instead => probably FeatureWriter has to be fixed as well)

  @ParameterizedTest
  @MethodSource("testScenarios")
  void start(TestArgs args) throws Exception {
    runTest(args);
  }

  @Override
  protected TestArgs modifyArgs(TestArgs args) {
    return args.withHistory(true);
  }
}
