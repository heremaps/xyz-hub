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

import static com.here.xyz.jobs.steps.inputs.Input.inputS3Prefix;
import static com.here.xyz.util.Random.randomAlpha;

import com.amazonaws.services.lambda.runtime.Context;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.TestSteps;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.Format;
import com.here.xyz.util.service.aws.SimulatedContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class JobStepTest extends TestSteps {
  protected static final String LAMBDA_ARN = "arn:aws:lambda:us-east-1:000000000000:function:job-step";
  protected static final String SPACE_ID = "test-space-" + randomAlpha(5);
  protected static final String JOB_ID = "test-job-" + randomAlpha(5);

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

  protected void simulateLambdaStepRequest(LambdaBasedStep step, LambdaBasedStep.LambdaStepRequest.RequestType requestType) throws IOException {
    OutputStream os = new ByteArrayOutputStream();
    Context ctx = new SimulatedContext("localLambda", null);

    Map<String, Object> stepMap = step.toMap();
    stepMap.put("taskToken.$", "test123");
    stepMap.put("jobId", JOB_ID);
    LambdaBasedStep enrichedStep = XyzSerializable.fromMap(stepMap, LambdaBasedStep.class);

    LambdaBasedStep.LambdaStepRequest request = new LambdaBasedStep.LambdaStepRequest().withStep(enrichedStep).withType(requestType);
    new LambdaBasedStep.LambdaBasedStepExecutor().handleRequest(new ByteArrayInputStream(request.toByteArray()), os, ctx);
  }

  protected void sendLambdaStepRequest(LambdaBasedStep step, LambdaBasedStep.LambdaStepRequest.RequestType requestType) {
    Map<String, Object> stepMap = step.toMap();
    stepMap.put("taskToken.$", "test123");
    stepMap.put("jobId", JOB_ID);
    LambdaBasedStep enrichedStep = XyzSerializable.fromMap(stepMap, LambdaBasedStep.class);
    LambdaBasedStep.LambdaStepRequest request = new LambdaBasedStep.LambdaStepRequest().withStep(enrichedStep).withType(requestType);

    invokeLambda(LAMBDA_ARN, request.toByteArray());
  }

  //TODO: find a central place to avoid double implementation from JobPlayground
  protected void uploadFiles(String jobId, int uploadFileCount, int featureCountPerFile, Format format)
          throws IOException {
    //Generate N Files with M features
    for (int i = 0; i < uploadFileCount; i++)
      uploadInputFile(jobId, generateContent(format, featureCountPerFile));
  }

  private void uploadInputFile(String jobId , byte[] bytes) throws IOException {
    uploadFileToS3(inputS3Prefix(jobId) + "/" + UUID.randomUUID(), S3ContentType.APPLICATION_JSON, bytes, false);
  }

  private byte[] generateContent(ImportFilesToSpace.Format format, int featureCnt) {
    String output = "";

    for (int i = 1; i <= featureCnt; i++) {
      output += generateContentLine(format, i);
    }
    return output.getBytes();
  }

  private String generateContentLine(ImportFilesToSpace.Format format, int i){
    Random rd = new Random();
    String lineSeparator = "\n";

    if(format.equals(Format.CSV_JSON_WKB))
      return "\"{'\"properties'\": {'\"test'\": "+i+"}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A400000000000000000"+lineSeparator;
    else if(format.equals(Format.CSV_GEOJSON))
      return "\"{'\"type'\":'\"Feature'\",'\"geometry'\":{'\"type'\":'\"Point'\",'\"coordinates'\":["+(rd.nextInt(179))+"."+(rd.nextInt(100))+","+(rd.nextInt(79))+"."+(rd.nextInt(100))+"]},'\"properties'\":{'\"test'\":"+i+"}}\""+lineSeparator;
    else
      return "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":["+(rd.nextInt(179))+"."+(rd.nextInt(100))+","+(rd.nextInt(79))+"."+(rd.nextInt(100))+"]},\"properties\":{\"te\\\"st\":"+i+"}}"+lineSeparator;
  }
}
