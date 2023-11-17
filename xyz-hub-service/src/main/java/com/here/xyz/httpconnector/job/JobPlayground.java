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

package com.here.xyz.httpconnector.job;

import static com.here.xyz.httpconnector.job.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.START_EXECUTION;
import static com.here.xyz.httpconnector.util.web.HubWebClient.createSpace;
import static com.here.xyz.httpconnector.util.web.HubWebClient.deleteSpace;

import com.amazonaws.services.lambda.runtime.Context;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.runtime.SimulatedContext;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.Config;
import com.here.xyz.httpconnector.job.steps.execution.LambdaBasedStep;
import com.here.xyz.httpconnector.job.steps.execution.LambdaBasedStep.LambdaBasedStepExecutor;
import com.here.xyz.httpconnector.job.steps.execution.LambdaBasedStep.LambdaStepRequest;
import com.here.xyz.httpconnector.job.steps.impl.AnalyzeSpaceTable;
import com.here.xyz.httpconnector.job.steps.impl.CreateIndex;
import com.here.xyz.httpconnector.job.steps.impl.DropIndexes;
import com.here.xyz.httpconnector.job.steps.impl.ImportFilesToSpace;
import com.here.xyz.httpconnector.job.steps.impl.MarkForMaintenance;
import com.here.xyz.httpconnector.util.jobs.datasets.DatasetDescription;
import com.here.xyz.httpconnector.util.jobs.datasets.FileOutputSettings;
import com.here.xyz.httpconnector.util.jobs.datasets.Files;
import com.here.xyz.httpconnector.util.jobs.datasets.files.GeoJson;
import com.here.xyz.httpconnector.util.web.HubWebClient.ErrorResponseException;
import com.here.xyz.httpconnector.util.web.HubWebClient.HubWebClientException;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper.Index;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JobPlayground {
  private static final Logger logger = LogManager.getLogger();

  static {
    VertxOptions vertxOptions = new VertxOptions()
        .setWorkerPoolSize(128)
        .setPreferNativeTransport(true)
        .setBlockedThreadCheckInterval(TimeUnit.MINUTES.toMillis(15))
        .setPreferNativeTransport(true)
        .setBlockedThreadCheckInterval(TimeUnit.MINUTES.toMillis(1))
        .setMaxEventLoopExecuteTime(TimeUnit.MINUTES.toMillis(1))
        .setMaxWorkerExecuteTime(TimeUnit.MINUTES.toMillis(1))
        .setWarningExceptionTime(TimeUnit.MINUTES.toMillis(1));

    CService.vertx = Vertx.vertx(vertxOptions);
    CService.configuration = new Config();
    CService.configuration.ECPS_PHRASE = "local";
    CService.configuration.HUB_ENDPOINT = "http://localhost:8080/hub";
    CService.configuration.JOBS_S3_BUCKET = "test-bucket";
    CService.configuration.JOBS_REGION = "eu-west-1";
    CService.configuration.LOCALSTACK_ENDPOINT = "http://localhost:4566";
  }

  private static Job mockJob = new Job()
      .withDescription("Sample playground job for mocked steps")
      .withOwner("me");

  private static Space sampleSpace;

  public static void main(String[] args) throws IOException, HubWebClientException {
    sampleSpace = createSampleSpace("TEST");

    //Upload files with each having one feature without id
    for (int i = 0; i < 2; i++)
      uploadInputFile("\"{'\"properties'\": {'\"foo'\": '\"bar'\",'\"foo_nested'\": {'\"nested_bar'\":true}}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A400000000000000000".getBytes());

    runDropIndexStep(sampleSpace.getId());
    runImportFilesToSpaceStep(sampleSpace.getId());

    for (Index index : Index.values())
      runCreateIndexStep(sampleSpace.getId(), index);

    runAnalyzeSpaceTableStep(sampleSpace.getId());
    runMarkForMaintenanceStep(sampleSpace.getId());
  }

  private static Space createSampleSpace(String spaceId) throws HubWebClientException {
    final String title = "Playground";
    try {
      return createSpace(spaceId, title);
    }
    catch (ErrorResponseException e) {
      if (e.getErrorResponse().statusCode() == 409) {
        deleteSpace(spaceId);
        return createSpace(spaceId, title);
      }
      else {
        HttpResponse<byte[]> response = e.getErrorResponse();
        logger.error("HTTP error {}, response text:\n{}", response.statusCode(), new String(response.body()));
        throw e;
      }
    }
  }

  private static void uploadInputFile(byte[] data) throws IOException {
    uploadInputFile(data, false);
  }

  private static void uploadInputFile(byte[] data, boolean compressed) throws IOException {
    //TODO: Compression

    URL uploadUrl = mockJob.createUploadUrl().getUrl();

    HttpURLConnection connection = (HttpURLConnection) uploadUrl.openConnection();
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setRequestMethod("PUT");
    OutputStream out = connection.getOutputStream();

    out.write(data);
    out.close();

    if (connection.getResponseCode() < 200 || connection.getResponseCode() > 299)
      throw new RuntimeException("Error uploading file, got status code " + connection.getResponseCode());
  }

  private static void startImportJob() {
    new Job()
        .withOwner("TestUser")
        .withDescription("Some test Import Job")
        .withSource(new DatasetDescription.Space().withId("someSpace"))
        .withTarget(new Files().withOutputSettings(new FileOutputSettings().withFormat(new GeoJson())))
        .submit();
  }

  public static void runDropIndexStep(String spaceId) throws IOException {
    runLambdaStep(new DropIndexes().withSpaceId(spaceId));
  }

  public static void runImportFilesToSpaceStep(String spaceId) throws IOException {
    runLambdaStep(new ImportFilesToSpace().withSpaceId(spaceId));
  }

  public static void runCreateIndexStep(String spaceId, Index index) throws IOException {
    runLambdaStep(new CreateIndex().withSpaceId(spaceId).withIndex(index));
  }

  public static void runAnalyzeSpaceTableStep(String spaceId) throws IOException {
    runLambdaStep(new AnalyzeSpaceTable().withSpaceId(spaceId));
  }

  public static void runMarkForMaintenanceStep(String spaceId) throws IOException {
    runLambdaStep(new MarkForMaintenance().withSpaceId(spaceId));
  }

  private static void runLambdaStep(LambdaBasedStep step) throws IOException {
    InputStream is = null;
    OutputStream os = null;
    Context ctx = new SimulatedContext("localLambda", null);

    Map<String, Object> stepMap = step.toMap();
    stepMap.put("taskToken", "test123");
    stepMap.put("jobId", mockJob.getId());
    LambdaBasedStep enrichedStep = XyzSerializable.fromMap(stepMap, LambdaBasedStep.class);

    LambdaStepRequest request = new LambdaStepRequest().withStep(enrichedStep).withType(START_EXECUTION);

    new LambdaBasedStepExecutor().handleRequest(new ByteArrayInputStream(request.toByteArray()), os, ctx);
  }
}
