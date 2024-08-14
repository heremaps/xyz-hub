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

package com.here.xyz.test.featurewriter.composite.history;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;

import com.here.xyz.test.featurewriter.noncomposite.history.SQLNonCompositeWithHistoryTestSuiteIT;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SQLComposite_SUPER_WithHistoryTestSuiteIT extends SQLNonCompositeWithHistoryTestSuiteIT {

  public SQLComposite_SUPER_WithHistoryTestSuiteIT(TestArgs args) {
    super(args.withComposite(true).withContext(SUPER));
  }

  //TODO: Align Hub and featureWriter
  //It's not permitted to perform modifications through context SUPER.
//    @Test
//    public void start() throws Exception {
//        featureWriterExecutor();
//    }
}
