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

package com.here.xyz.jobs.steps.impl;

import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.START_EXECUTION;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.Index.GEO;
import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;

public class CreateIndexStepTest extends StepTest {

    @Test
    public void testCreateIndex() throws Exception {
        deleteAllExistingIndexes(SPACE_ID);
        Assertions.assertEquals(0, listExistingIndexes(SPACE_ID).size());

        LambdaBasedStep step = new CreateIndex().withSpaceId(SPACE_ID).withIndex(GEO);

        sendLambdaStepRequestBlock(step, true);

        List<String> indexes = listExistingIndexes(SPACE_ID);
        Assertions.assertEquals(1, indexes.size());
        Assertions.assertEquals("idx_" + SPACE_ID + "_" + GEO.toString().toLowerCase(), indexes.get(0));
    }
}
