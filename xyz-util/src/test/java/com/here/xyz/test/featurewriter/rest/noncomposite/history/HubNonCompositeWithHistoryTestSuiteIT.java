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
import java.util.Collection;
import java.util.Set;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class HubNonCompositeWithHistoryTestSuiteIT extends RestTestSuite {

  public HubNonCompositeWithHistoryTestSuiteIT(TestArgs args) {
    super(args);
  }

  @Parameters(name = "{0}")
  public static Collection<Object[]> parameterSets() throws JsonProcessingException {
    return testScenarios().stream().map(args -> new Object[]{args}).toList();
  }

  public static Collection<TestArgs> testScenarios() throws JsonProcessingException {
    Set<String> ignoredTests = Set.of(
        "4", //FIXME: Issue in Hub: Author is not being written for deletion markers
        "8", //FIXME: Issue in Hub: Author is not being written for deletion markers
        "12", //FIXME: Issue in Hub: No version conflict is thrown in that case
        "14", //FIXME: Issue in Hub: Author is not being written for deletion markers
        "18", //FIXME: Issue in Hub: Concurrently written attribute is sometimes not merged into the result correctly (flickering)
        "19", //FIXME: Issue in Hub: MergeConflictResolution is not supported / The service does not distinguish between version- & merge-conflicts
        "20", //FIXME: Issue in Hub: MergeConflictResolution is not supported / The service does not distinguish between version- & merge-conflicts
        "21" //FIXME: Issue in Hub: MergeConflictResolution is not supported / The service does not distinguish between version- & merge-conflicts
    );

    return SQLNonCompositeWithHistoryTestSuiteIT.testScenarios()
        .stream().filter(args -> !ignoredTests.contains(args.testName())).toList();
  }

  //TODO: Check missing version conflict errors
  //TODO: Check creation timestamp issues (actually in case of history the test should not treat a table.INSERT as creation, but a feature.I should be instead => probably FeatureWriter has to be fixed as well)

}
