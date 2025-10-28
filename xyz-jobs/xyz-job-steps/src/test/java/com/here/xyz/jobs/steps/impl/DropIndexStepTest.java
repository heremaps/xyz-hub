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
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.db.pg.IndexHelper.Index;
import com.here.xyz.util.db.pg.IndexHelper.OnDemandIndex;
import com.here.xyz.util.db.pg.IndexHelper.SystemIndex;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DropIndexStepTest extends StepTest {

  @Test
  public void testDropIndexesStepWithoutWhitelist() throws Exception {
    createTestSpace(true);
    Assertions.assertFalse(getAllExistingIndices(SPACE_ID).isEmpty());

    LambdaBasedStep step = new DropIndexes().withSpaceId(SPACE_ID);
    sendLambdaStepRequestBlock(step, true);

    //no indexes should remain
    Assertions.assertEquals(0, getAllExistingIndices(SPACE_ID).size());
  }

  @Test
  public void testDropIndexesStepWithWhitelist() throws Exception {
    createTestSpace(true);
    Assertions.assertFalse(getSystemIndices(SPACE_ID).isEmpty());
    //three on-demand indices should be created with the space creation
    Assertions.assertEquals(3, getOnDemandIndices(SPACE_ID).size());

    LambdaBasedStep step = new DropIndexes()
      .withSpaceId(SPACE_ID)
      .withSpaceDeactivation(false)
      .withIndexWhiteList(
                List.of(
                    new OnDemandIndex().withPropertyPath("foo1"),
                    new OnDemandIndex().withPropertyPath("foo3")
                )
      );

    sendLambdaStepRequestBlock(step, true);
    Assertions.assertEquals(2,getOnDemandIndices(SPACE_ID).size(), "whitelisted indexes should remain");
  }

  @Test
  public void testDropIndexesStepWithEmptyWhitelist() throws Exception {
    createTestSpace(true);
    Assertions.assertFalse(getSystemIndices(SPACE_ID).isEmpty());

    LambdaBasedStep step = new DropIndexes()
            .withSpaceId(SPACE_ID)
            .withSpaceDeactivation(false)
            .withIndexWhiteList(List.of());
    sendLambdaStepRequestBlock(step, true);

    Assertions.assertEquals(0, getOnDemandIndices(SPACE_ID).size(), "no indexes should remain");
    Assertions.assertEquals(0, getSystemIndices(SPACE_ID).size(), "no indexes should remain");
  }

  @Test
  public void testDropIndexesStepWithWhitelistedSystemIndexes() throws Exception {
    createTestSpace(true);
    Assertions.assertFalse(getSystemIndices(SPACE_ID).isEmpty());

    LambdaBasedStep step = new DropIndexes()
            .withSpaceId(SPACE_ID)
            .withSpaceDeactivation(false)
            .withIndexWhiteList(List.of(SystemIndex.VERSION_ID, SystemIndex.OPERATION));
    sendLambdaStepRequestBlock(step, true);

    Assertions.assertEquals(0, getOnDemandIndices(SPACE_ID).size(), "no on-demand indexes should remain");
    Assertions.assertEquals(2, getSystemIndices(SPACE_ID).size(), "only 2 system indexes should remain");
  }

  @Test
  public void testDropIndexesStepWithWhitelistOnSpaceWithoutOnDemandIndices() throws Exception {
    //recreate space without on-demandindices
    createTestSpace(false);

    List<Index> whiteListIndexes = new ArrayList<>(List.of(SystemIndex.values()));
    whiteListIndexes.add(new OnDemandIndex().withPropertyPath("foo1"));
    LambdaBasedStep step = new DropIndexes()
            .withSpaceId(SPACE_ID)
            .withSpaceDeactivation(false)
            .withIndexWhiteList(whiteListIndexes);
    sendLambdaStepRequestBlock(step, true);

    Assertions.assertTrue(getOnDemandIndices(SPACE_ID).isEmpty(), "only system indices should remain");
    Assertions.assertFalse(getSystemIndices(SPACE_ID).isEmpty(),"system indices should remain");
  }

  private void createTestSpace(boolean withOnDemandIndices) {
    Space space = new Space().withId(SPACE_ID);
    if (withOnDemandIndices) {
      space.setSearchableProperties(
        Map.of(
                "foo1", true,
                "foo2.nested", true,
                "foo3", true
        )
      );
    }
    createSpace(space, false);
  }
}
