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

import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.START_EXECUTION;
import static com.here.xyz.jobs.steps.inputs.Input.inputS3Prefix;
import static com.here.xyz.util.Random.randomAlpha;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.Index.GEO;
import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.services.lambda.runtime.Context;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.TestSteps;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.service.aws.SimulatedContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JobStepsTest extends TestSteps {
  private static final String LAMBDA_ARN = "arn:aws:lambda:us-east-1:000000000000:function:job-step";
  private static final String SPACE_ID = "test-space-" + randomAlpha(5);
  private static final String JOB_ID = "test-job-" + randomAlpha(5);

  @BeforeEach
  public void setup() {
    cleanup();
    createSpace(SPACE_ID);
  }
  @AfterEach
  public void cleanup() {
    deleteSpace(SPACE_ID);
    cleanS3Files(JOB_ID);
  }

  @Test
  public void testDropIndexesStep() throws Exception {
    assertTrue(listExistingIndexes(SPACE_ID).size() > 0);

    LambdaBasedStep step = new DropIndexes().withSpaceId(SPACE_ID);
//    simulateLambdaStepRequest(step, START_EXECUTION);
//    simulateLambdaStepRequest(step, SUCCESS_CALLBACK);

    sendLambdaStepRequest(step, START_EXECUTION);
    sleep(2000);

    assertEquals(0, listExistingIndexes(SPACE_ID).size());
  }

  @Test
  public void testCreateIndex() throws Exception {
    deleteAllExistingIndexes(SPACE_ID);
    assertEquals(0, listExistingIndexes(SPACE_ID).size());

    LambdaBasedStep step = new CreateIndex().withSpaceId(SPACE_ID).withIndex(GEO);

//    simulateLambdaStepRequest(step, START_EXECUTION);
//    simulateLambdaStepRequest(step, SUCCESS_CALLBACK);

    sendLambdaStepRequest(step, START_EXECUTION);
    sleep(2000);

    List<String> indexes = listExistingIndexes(SPACE_ID);
    assertEquals(1, indexes.size());
    assertEquals("idx_" + SPACE_ID + "_" + GEO.toString().toLowerCase(), indexes.get(0));
  }

  @Test
  public void testImportFilesToSpaceStep() throws Exception {
    StatisticsResponse statsBefore = getStatistics(SPACE_ID);
    assertEquals(0L, (Object) statsBefore.getCount().getValue());

    uploadInputFile(JOB_ID);
    LambdaBasedStep step = new ImportFilesToSpace().withSpaceId(SPACE_ID);

//    simulateLambdaStepRequest(step, START_EXECUTION);
//    simulateLambdaStepRequest(step, SUCCESS_CALLBACK);

    sendLambdaStepRequest(step, START_EXECUTION);
    sleep(2000);

    StatisticsResponse statsAfter = getStatistics(SPACE_ID);
    assertEquals(2L, (Object) statsAfter.getCount().getValue());
  }

  private void simulateLambdaStepRequest(LambdaBasedStep step, LambdaBasedStep.LambdaStepRequest.RequestType requestType) throws IOException {
    OutputStream os = new ByteArrayOutputStream();
    Context ctx = new SimulatedContext("localLambda", null);

    Map<String, Object> stepMap = step.toMap();
    stepMap.put("taskToken.$", "test123");
    stepMap.put("jobId", JOB_ID);
    LambdaBasedStep enrichedStep = XyzSerializable.fromMap(stepMap, LambdaBasedStep.class);

    LambdaBasedStep.LambdaStepRequest request = new LambdaBasedStep.LambdaStepRequest().withStep(enrichedStep).withType(requestType);
    new LambdaBasedStep.LambdaBasedStepExecutor().handleRequest(new ByteArrayInputStream(request.toByteArray()), os, ctx);
  }

  private void sendLambdaStepRequest(LambdaBasedStep step, LambdaBasedStep.LambdaStepRequest.RequestType requestType) {
    Map<String, Object> stepMap = step.toMap();
    stepMap.put("taskToken.$", "test123");
    stepMap.put("jobId", JOB_ID);
    LambdaBasedStep enrichedStep = XyzSerializable.fromMap(stepMap, LambdaBasedStep.class);
    LambdaBasedStep.LambdaStepRequest request = new LambdaBasedStep.LambdaStepRequest().withStep(enrichedStep).withType(requestType);

    invokeLambda(LAMBDA_ARN, request.toByteArray());
  }

  private void uploadInputFile(String jobId) throws IOException {
    String data = "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[8,50]},\"properties\":{\"test\":1}}";
    uploadFileToS3(inputS3Prefix(jobId) + "/" + UUID.randomUUID(), S3ContentType.APPLICATION_JSON, data.getBytes(), false);
    String data2 = "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[8,50]},\"properties\":{\"test\":2}}";
    uploadFileToS3(inputS3Prefix(jobId) + "/" + UUID.randomUUID(), S3ContentType.APPLICATION_JSON, data2.getBytes(), false);
  }

}
