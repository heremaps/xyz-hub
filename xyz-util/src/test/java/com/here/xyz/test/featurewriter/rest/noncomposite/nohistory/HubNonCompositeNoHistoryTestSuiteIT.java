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

package com.here.xyz.test.featurewriter.rest.noncomposite.nohistory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.test.featurewriter.rest.RestTestSuite;
import com.here.xyz.test.featurewriter.sql.noncomposite.nohistory.SQLNonCompositeNoHistoryTestSuiteIT;
import java.util.List;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class HubNonCompositeNoHistoryTestSuiteIT extends RestTestSuite {

  public HubNonCompositeNoHistoryTestSuiteIT(TestArgs args) {
    super(args);
  }

  @Parameters(name = "{0}")
  public static List<Object[]> parameterSets() throws JsonProcessingException {
    return testScenarios().stream().map(args -> new Object[]{args}).toList();
  }

  public static List<TestArgs> testScenarios() throws JsonProcessingException {
    Set<String> ignoredTests = Set.of(
      "12", //FIXME: Issue in Hub: No version conflict is thrown in that case
      "15" //FIXME: Issue in Hub: No illegal argument error is thrown in that case
    );

    return SQLNonCompositeNoHistoryTestSuiteIT.testScenarios()
        .stream().filter(args -> !ignoredTests.contains(args.testName())).toList();
  }

  //TODO: Check missing version conflict errors

  @Test
  public void start() throws Exception {
    runTest();
  }
}
