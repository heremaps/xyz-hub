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

package com.here.xyz.test.featurewriter.rest.composite.history;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.test.featurewriter.rest.composite.nohistory.HubComposite_EXTENSION_NoHistoryTestSuiteIT;
import com.here.xyz.test.featurewriter.sql.composite.history.SQLComposite_EXTENSION_WithHistoryTestSuiteIT;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class HubComposite_EXTENSION_WithHistoryTestSuiteIT extends HubComposite_EXTENSION_NoHistoryTestSuiteIT {

  public static Stream<TestArgs> testScenarios() throws JsonProcessingException {
    Set<String> ignoredTests = Set.of(
        "12", //FIXME: Issue in Hub: No version conflict is thrown in that case, because Hub does not distinguish an existence conflict and a version conflict in its error message [will be fixed by new FeatureWriter impl]
        "18", //FIXME: Issue in Hub: Concurrently written attribute is sometimes not merged into the result correctly (flickering) [will be fixed by new FeatureWriter impl]
        "19", //FIXME: Issue in Hub: MergeConflictResolution is not supported / The service does not distinguish between version- & merge-conflicts [flickering: works sometimes in hub, will be reliably fixed by new FeatureWriter impl]
        "20", //FIXME: Issue in Hub: MergeConflictResolution is not supported / The service does not distinguish between version- & merge-conflicts [flickering: sometimes wrong table operation is done, sometimes wrong error is thrown, will be reliably fixed by new FeatureWriter impl]
        "21" //FIXME: Issue in Hub: MergeConflictResolution is not supported / The service does not distinguish between version- & merge-conflicts [flickering: sometimes the feature is simply written (most likely a bug) and sometimes an unexpetced MERGE_CONFLICT_ERROR is thrown, will be reliably fixed by new FeatureWriter impl]
    );

    return SQLComposite_EXTENSION_WithHistoryTestSuiteIT.testScenarios()
        .filter(args -> !ignoredTests.contains(args.testName()));
  }

  @ParameterizedTest
  @MethodSource("testScenarios")
  void start(TestArgs args) throws Exception {
    runTest(args);
  }

  @Override
  protected TestArgs modifyArgs(TestArgs args) {
    return args.withComposite(true).withContext(EXTENSION).withFeatureExistsInExtension(args.featureExists()).withHistory(true);
  }
}
