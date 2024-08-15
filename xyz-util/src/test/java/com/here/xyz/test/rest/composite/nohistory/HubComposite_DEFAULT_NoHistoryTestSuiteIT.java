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

package com.here.xyz.test.rest.composite.nohistory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.test.GenericSpaceBased;
import com.here.xyz.test.GenericSpaceBased.OnNotExists;
import com.here.xyz.test.GenericSpaceBased.Operation;
import com.here.xyz.test.featurewriter.noncomposite.nohistory.SQLNonCompositeNoHistoryTestSuiteIT;
import java.util.Collection;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class HubComposite_DEFAULT_NoHistoryTestSuiteIT extends SQLNonCompositeNoHistoryTestSuiteIT {

  public HubComposite_DEFAULT_NoHistoryTestSuiteIT(TestArgs args) {
    super(args);
  }

  @Parameters(name = "{0}")
  public static Collection<Object[]> parameterSets() throws JsonProcessingException {
    return testScenarios().stream().map(args -> new Object[]{args}).toList();
  }

  public static List<TestArgs> testScenarios() throws JsonProcessingException {
    return List.of(
        /** Feature not exists */
        new TestArgs(
            "0",
            true,
            false,
            false,
            null,
            null,
            null,
            null,

            UserIntent.WRITE,
            OnNotExists.CREATE,
            null,
            null,
            null,
            SpaceContext.DEFAULT,

            //Expected content of newly created Feature
            new Expectations(
                TableOperation.INSERT,
                Operation.I,
                simpleFeature(),
                1,
                Long.MAX_VALUE,
                GenericSpaceBased.DEFAULT_AUTHOR,
                null
            )
        )
//                /* No existing Feature expected. No TableOperation!  */
//                new TestArgs("2", true, false, false, null, null, null, SpaceContext.EXTENSION, UserIntent.WRITE, OnNotExists.ERROR, null, null, null, null, new Expectations(SQLError.FEATURE_NOT_EXISTS) ),
//                /* No existing Feature expected. No TableOperation!  */
//                new TestArgs("3", true, false, false, null, null, null, null, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null, null, null ),
    );
  }

  @Test
  public void start() throws Exception {
    featureWriterExecutor();
  }
}
