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

import com.here.xyz.models.hub.Branch;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.util.db.pg.IndexHelper.Index;
import com.here.xyz.util.db.pg.IndexHelper.OnDemandIndex;
import com.here.xyz.util.db.pg.IndexHelper.SystemIndex;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.util.List;

public class CreateIndexStepTest extends StepTest {

    @Test
    public void testCreateSystemIndex() throws Exception {
        executeCreateIndexStep(SPACE_ID, SystemIndex.GEO);
    }

    @Test
    public void testCreateOnDemandIndex() throws Exception {
        executeCreateIndexStep(SPACE_ID, new OnDemandIndex().withPropertyPath("foo"));

    }

    @Test
    public void testCreateIndexOnBranch() throws Exception {
        putFeatureToSpace(SPACE_ID, simpleFeature("f1"));
        createBranch(SPACE_ID, new Branch().withId(BRANCH_ID));

        //Create system index on branch
        executeCreateIndexStep(SPACE_ID, BRANCH_ID, SystemIndex.GEO);

        //Create on-demand index on branch
        executeCreateIndexStep(SPACE_ID, BRANCH_ID, new OnDemandIndex().withPropertyPath("foo"));
    }

    private void executeCreateIndexStep(String spaceId, Index index) throws Exception {
        executeCreateIndexStep(spaceId, null, index);
    }

    private void executeCreateIndexStep(String spaceId, String branchId, Index index) throws Exception {
        String tableName = spaceId;
        CreateIndex step = new CreateIndex().withSpaceId(spaceId).withIndex(index);

        if (branchId != null) {
            tableName = getBranchTableName(spaceId, branchId);
            step.setVersionRef(new Ref(branchId));
        }

        deleteAllExistingIndices(tableName);
        Assertions.assertTrue(getAllExistingIndices(tableName).isEmpty());

        sendLambdaStepRequestBlock(step, true);
        List<Index> indexes = getIndices(tableName, step.getIndex() instanceof SystemIndex, step.getIndex() instanceof OnDemandIndex);
        Assertions.assertEquals(1, indexes.size());
        Assertions.assertEquals(step.getIndex().getIndexName(tableName), indexes.get(0).getIndexName(tableName));
    }
}
