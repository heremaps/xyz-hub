/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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
import com.here.xyz.util.db.pg.IndexHelper.Index;
import com.here.xyz.util.db.pg.IndexHelper.OnDemandIndex;
import com.here.xyz.util.db.pg.IndexHelper.SystemIndex;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CreateIndexStepTest extends StepTest {

    @Test
    public void testCreateSystemIndex() throws Exception {
        deleteAllExistingIndices(SPACE_ID);
        Assertions.assertTrue(getAllExistingIndices(SPACE_ID).isEmpty());

        SystemIndex systemIndex = SystemIndex.GEO;
        LambdaBasedStep step = new CreateIndex().withSpaceId(SPACE_ID).withIndex(SystemIndex.GEO);

        sendLambdaStepRequestBlock(step, true);

        List<Index> indexes = getSystemIndices(SPACE_ID);
        Assertions.assertEquals(1, indexes.size());
        Assertions.assertEquals(systemIndex.getIndexName(SPACE_ID), indexes.get(0).getIndexName(SPACE_ID));
    }

    @Test
    public void testCreateOnDemandIndex() throws Exception {
        deleteAllExistingIndices(SPACE_ID);
        Assertions.assertTrue(getAllExistingIndices(SPACE_ID).isEmpty());

        OnDemandIndex onDemandIndex = new OnDemandIndex().withPropertyPath("foo");

        LambdaBasedStep step = new CreateIndex().withSpaceId(SPACE_ID).withIndex(onDemandIndex);

        sendLambdaStepRequestBlock(step, true);

        List<Index> indexes = getOnDemandIndices(SPACE_ID);
        Assertions.assertEquals(1, indexes.size());
        Assertions.assertEquals(onDemandIndex.getIndexName(SPACE_ID), indexes.get(0).getIndexName());
    }
}
