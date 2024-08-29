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

package com.here.xyz.test.featurewriter.rest.composite.nohistory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.test.featurewriter.rest.RestTestSuite;
import com.here.xyz.test.featurewriter.sql.composite.nohistory.SQLComposite_DEFAULT_NoHistoryTestSuiteIT;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class HubComposite_DEFAULT_NoHistoryTestSuiteIT extends RestTestSuite {

  public HubComposite_DEFAULT_NoHistoryTestSuiteIT(TestArgs args) {
    super(args);
  }

  @Parameters(name = "{0}")
  public static Collection<Object[]> parameterSets() throws JsonProcessingException {
    return testScenarios().stream().map(args -> new Object[]{args}).toList();
  }

  public static List<TestArgs> testScenarios() throws JsonProcessingException {
    Set<String> ignoredTests = Set.of(
        "2.1", //FIXME: The feature's update timestamp has to be higher than the timestamp when the test started.
        "2.2", //FIXME: The feature's update timestamp has to be higher than the timestamp when the test started.
        "4.1", //FIXME: The feature's update timestamp has to be higher than the timestamp when the test started.
        "5.4", //FIXME: The feature's update timestamp has to be higher than the timestamp when the test started.
        "5.1", //FIXME: Issue in Hub: No version conflict is thrown in that case
        "5.5" //FIXME: Issue in Hub: No illegal argument error is thrown in that case [will be fixed by new FeatureWriter impl]
    );

    //TODO: Check missing version conflict errors
    //TODO: Maybe also use the SQL writer for the preparation of the Hub tests

    return SQLComposite_DEFAULT_NoHistoryTestSuiteIT.testScenarios()
        .stream().filter(args -> !ignoredTests.contains(args.testName())).toList();
  }

  @Test
  public void start() throws Exception {
    runTest();
  }
}
