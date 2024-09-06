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
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.responses.StatisticsResponse;
import org.junit.jupiter.api.Test;

import static com.here.xyz.jobs.datasets.space.UpdateStrategy.DEFAULT_UPDATE_STRATEGY;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.START_EXECUTION;
import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;

public class ImportStepsTest extends JobStepsTest {


  /**
    Test Format`s
      * CSV_JSON_WKB
      * CSV_GEOSJON
      * GEOJSON

    Test EntityPerLine`s
      * Feature
      * FeatureCollection
   *
    Test UpdateStrategy`s
      * UpdateStrategy.OnExists
      * UpdateStrategy.OnNotExists
      * UpdateStrategy.OnMergeConflict
      * UpdateStrategy.OnVersionConflict

    Test ThreadCount-Settings
      * Check ThreadCount Calculation

    Test START_EXECUTION
      * Check Result
      * Check Temp-Table and its content
      * Check Temp-Trigger-Table
      * Check Trigger-Creation
      * Check set of ReadOnlyMode

    Test SUCCESS_CALLBACK
      * Check Statistics
      * Check cleanup of Temp-Table
      * Check cleanup Temp-Trigger-Table
      * Check release of ReadOnlyMode

    Test FAILURE_CALLBACK
      * Retryable: Check persistence of temp-tables
      * NonRetryable: Check clean-up of tables

    Test STATE_CHECK
      * Check Statistics
      * Check Result (running queries)

    Test ExecutionModes
      * ASYCN
      * SYNC

    Test Import in Empty Layer (all Formats / all execution modes )
      * Check Trigger-Type
      * Check result (Operation, author, etc)
      * Check if expected Features are present

    Test Import in NonEmpty Layer (all Formats / all execution modes )
      * Check Trigger-Type
      * Check result (Operation, author, etc)
      * Check if expected Features are present
      * Check if existing id got updated

    Test Cancel

    Test Resume
   */

  @Test
  public void testImportFilesToSpaceStep() throws Exception {
    StatisticsResponse statsBefore = getStatistics(SPACE_ID);
    assertEquals(0L, (Object) statsBefore.getCount().getValue());

    uploadInputFile(JOB_ID);
    LambdaBasedStep step = new ImportFilesToSpace()
            .withUpdateStrategy(DEFAULT_UPDATE_STRATEGY)
            .withSpaceId(SPACE_ID);

    sendLambdaStepRequest(step, START_EXECUTION);
    sleep(2000);

    StatisticsResponse statsAfter = getStatistics(SPACE_ID);
    assertEquals(2L, (Object) statsAfter.getCount().getValue());
  }

}
