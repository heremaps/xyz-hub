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
import com.here.xyz.jobs.steps.impl.transport.CopySpace;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.StatisticsResponse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

public class CopySpaceStepsTest extends StepTest {

  private String SrcSpc = "testCopy-Source-07", 
                 TrgSpc = "testCopy-Target-07"; 

  @BeforeEach
  public void setup() throws SQLException {
      cleanup();
      createSpace(new Space().withId(SrcSpc).withVersionsToKeep(100),false);
      createSpace(new Space().withId(TrgSpc).withVersionsToKeep(100),false);
      //write features source
      putRandomFeatureCollectionToSpace(SrcSpc, 2);
      putRandomFeatureCollectionToSpace(SrcSpc, 2);
      //write features target - non-empty-space
      putRandomFeatureCollectionToSpace(TrgSpc, 2);

  }

  @AfterEach
  public void cleanup() throws SQLException {
    deleteSpace(SrcSpc);
    deleteSpace(TrgSpc);
  }

  
  @Test
  public void testCopySpaceToSpaceStep() throws Exception {
    
    StatisticsResponse statsBefore = getStatistics(TrgSpc);

    assertEquals(2L, (Object) statsBefore.getCount().getValue());

    LambdaBasedStep step = new CopySpace()
                               .withSpaceId(SrcSpc).withSourceVersionRef(new Ref("HEAD"))
                               .withTargetSpaceId( TrgSpc );
          
    sendLambdaStepRequestBlock(step);
//    sendLambdaStepRequest(step, LambdaBasedStep.LambdaStepRequest.RequestType.START_EXECUTION, true);
//    sleep(2000);

    StatisticsResponse statsAfter = getStatistics(TrgSpc);
    assertEquals(6L, (Object) statsAfter.getCount().getValue());
  }

}
