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
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper.OnDemandIndex;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DropIndexStepTest extends StepTest {

  @BeforeEach
  public void setup() throws SQLException {
    createSpace(new Space().withId(SPACE_ID).withSearchableProperties(Map.of(
                    "foo1", true,
                    "foo2.nested", true,
                    "foo3", true
            )
    ), false);
  }

  @Test
  public void testDropIndexesStepWithoutWhitelist() throws Exception {
    Assertions.assertTrue(listExistingIndexes(SPACE_ID).size() > 0);

    LambdaBasedStep step = new DropIndexes().withSpaceId(SPACE_ID);
    sendLambdaStepRequestBlock(step, true);

    //no indexes should remain
    Assertions.assertEquals(0, listExistingIndexes(SPACE_ID).size());
  }

  @Test
  public void testDropIndexesStepWithWhitelist() throws Exception {
    Assertions.assertTrue(listExistingIndexes(SPACE_ID).size() > 0);

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

    //system indexes + whitelisted indexes should remain
    Assertions.assertEquals(7 + 2, listExistingIndexes(SPACE_ID).size());
  }

  @Test
  public void testDropIndexesStepWithEmptyWhitelist() throws Exception {
    Assertions.assertTrue(listExistingIndexes(SPACE_ID).size() > 0);

    LambdaBasedStep step = new DropIndexes()
            .withSpaceId(SPACE_ID)
            .withSpaceDeactivation(false)
            .withIndexWhiteList(List.of());
    sendLambdaStepRequestBlock(step, true);

    //only system indexes should remain
    Assertions.assertEquals(7, listExistingIndexes(SPACE_ID).size());
  }

  @Test
  public void testDropIndexesStepWithWhitelistOnSpaceWithoutOnDemandIndices() throws Exception {
    //recreate space without on-demandindices
    createSpace(new Space().withId(SPACE_ID), true);

    LambdaBasedStep step = new DropIndexes()
            .withSpaceId(SPACE_ID)
            .withSpaceDeactivation(false)
            .withIndexWhiteList(List.of(new OnDemandIndex().withPropertyPath("foo1")));
    sendLambdaStepRequestBlock(step, true);

    //only system indexes should remain
    Assertions.assertEquals(7, listExistingIndexes(SPACE_ID).size());
  }
}
