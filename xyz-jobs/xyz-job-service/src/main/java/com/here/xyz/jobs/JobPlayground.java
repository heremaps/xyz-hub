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

package com.here.xyz.jobs;

import static com.here.xyz.jobs.datasets.files.FileFormat.EntityPerLine.Feature;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.START_EXECUTION;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.SUCCESS_CALLBACK;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.config.JobConfigClient;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.datasets.files.FileInputSettings;
import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.service.Config;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaBasedStepExecutor;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType;
import com.here.xyz.jobs.steps.impl.AnalyzeSpaceTable;
import com.here.xyz.jobs.steps.impl.CreateIndex;
import com.here.xyz.jobs.steps.impl.DropIndexes;
import com.here.xyz.jobs.steps.impl.MarkForMaintenance;
import com.here.xyz.jobs.steps.impl.imp.ImportFilesToSpace;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.ARN;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper.Index;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.aws.SimulatedContext;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.ErrorResponseException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

public class JobPlayground {
  private static final Logger logger = LogManager.getLogger();
  private static HubWebClient hubWebClient;
  private static LambdaClient lambdaClient;
  private static Space sampleSpace;
  private static boolean simulateExecution = true;
  private static boolean executeWholeJob = true;
  private static ImportFilesToSpace.Format format = ImportFilesToSpace.Format.GEOJSON;
  private static int uploadFileCount = 2;

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

    Core.vertx = Vertx.vertx(vertxOptions);
    new Config();
    Config.instance.ECPS_PHRASE = "local";
    Config.instance.HUB_ENDPOINT = "http://localhost:8080/hub";
    Config.instance.JOBS_S3_BUCKET = "test-bucket";
    Config.instance.AWS_REGION = "us-east-1";
    Config.instance.JOBS_DYNAMODB_TABLE_ARN = "arn:aws:dynamodb:localhost:000000008000:table/xyz-jobs-local";
    Config.instance.STEP_LAMBDA_ARN = new ARN("arn:aws:lambda:us-east-1:000000000000:function:job-step");
    hubWebClient = HubWebClient.getInstance(Config.instance.HUB_ENDPOINT);
    try {
      Config.instance.LOCALSTACK_ENDPOINT = new URI("http://localhost:4566");
      lambdaClient = LambdaClient.builder()
          .region(Region.US_EAST_1)
          .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("localstack", "localstack")))
          .endpointOverride(Config.instance.LOCALSTACK_ENDPOINT)
          .build();
      Config.instance.JOB_API_ENDPOINT = new URL(simulateExecution && !executeWholeJob ? "http://localhost:7070"
          : "http://host.docker.internal:7070");
    }
    catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  static {
    try {
      sampleSpace = createSampleSpace("TEST");
    }
    catch (WebClientException e) {
      throw new RuntimeException(e);
    }
    JobConfigClient.getInstance().init();
  }

  private static Job mockJob = new Job().create()
      .withDescription("Sample import job")
      .withOwner("me")
      .withSource(new Files<>().withInputSettings(new FileInputSettings().withFormat(new GeoJson().withEntityPerLine(Feature))))
      .withTarget(new DatasetDescription.Space<>().withId(sampleSpace.getId()));

  public static void main(String[] args) throws IOException, InterruptedException {
    if (executeWholeJob)
      startMockJob();
    else
      startLambdaExecutions();
  }

  private static void startLambdaExecutions() throws IOException, InterruptedException {
    uploadFiles();

    runDropIndexStep(sampleSpace.getId());

    runImportFilesToSpaceStep(sampleSpace.getId(), format);

    for (Index index : Index.values())
      runCreateIndexStep(sampleSpace.getId(), index);

    runAnalyzeSpaceTableStep(sampleSpace.getId());

    runMarkForMaintenanceStep(sampleSpace.getId());
  }

  private static void uploadFiles() throws IOException {
    //Generate N Files with M features
    for (int i = 0; i < uploadFileCount; i++)
      uploadInputFile(generateContent(format, 10));
  }

  private static byte[] generateContent(ImportFilesToSpace.Format format, int featureCnt) {
    Random rd = new Random();
    String output = "";
    for (int i = 0; i < featureCnt; i++) {
      if(format.equals(ImportFilesToSpace.Format.CSV_JSONWKB))
        output += "\"{'\"properties'\": {'\"test'\": "+i+"}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A400000000000000000\n";
      else if(format.equals(ImportFilesToSpace.Format.CSV_GEOJSON))
        output += "\"{'\"type'\":'\"Feature'\",'\"geometry'\":{'\"type'\":'\"Point'\",'\"coordinates'\":[8,50."+(rd.nextInt(100))+"]},'\"properties'\":{'\"test'\":"+i+"}}\"\n";
      else
        output += "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[8,50."+(rd.nextInt(100))+"]},\"properties\":{\"test\":"+i+"}}\n";
    }
    return output.getBytes();
  }

  private static void startMockJob() {
    mockJob.submit()
        .compose(submitted -> {
          if (submitted)
            return Future.succeededFuture(true);
          try {
            uploadFiles();
          }
          catch (IOException e) {
            return Future.failedFuture(e);
          }
          return mockJob.submit(); //Submit the job, now that the input files have been uploaded
        })
        .compose(submitted -> {
          if (!submitted)
            return Future.failedFuture("Job is still not ready, even after uploading the input files");
          return Future.succeededFuture();
        })
        .onFailure(t -> logger.error("Error submitting job:", t));
  }

  private static Space createSampleSpace(String spaceId) throws WebClientException {
    final String title = "Playground";
    try {
      return hubWebClient.createSpace(spaceId, title);
    }
    catch (ErrorResponseException e) {
      if (e.getErrorResponse().statusCode() == 409) {
        hubWebClient.deleteSpace(spaceId);
        return hubWebClient.createSpace(spaceId, title);
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

  public static void runDropIndexStep(String spaceId) throws IOException {
    runStep(new DropIndexes().withSpaceId(spaceId));
  }

  public static void runImportFilesToSpaceStep(String spaceId, ImportFilesToSpace.Format format) throws IOException {
    runStep(new ImportFilesToSpace().withSpaceId(spaceId).withFormat(format));
  }

  public static void runCreateIndexStep(String spaceId, Index index) throws IOException {
    runStep(new CreateIndex().withSpaceId(spaceId).withIndex(index));
  }

  public static void runAnalyzeSpaceTableStep(String spaceId) throws IOException {
    runStep(new AnalyzeSpaceTable().withSpaceId(spaceId));
  }

  public static void runMarkForMaintenanceStep(String spaceId) throws IOException {
    runStep(new MarkForMaintenance().withSpaceId(spaceId));
  }

  private static void runStep(LambdaBasedStep step) throws IOException {
    if (simulateExecution) {
      simulateLambdaStepRequest(step, START_EXECUTION);
      //Wait some time before simulating success callback
      sleep(4000);
      simulateLambdaStepRequest(step, SUCCESS_CALLBACK);
    }
    else {
      sendLambdaStepRequest(step, START_EXECUTION);
      //Wait some time to give some time for the execution of the success callback
      sleep(4000);
    }
  }

  private static void simulateLambdaStepRequest(LambdaBasedStep step, RequestType requestType) throws IOException {
    OutputStream os = new ByteArrayOutputStream();
    Context ctx = new SimulatedContext("localLambda", null);

    final LambdaStepRequest request = prepareStepRequestPayload(step, requestType);
    new LambdaBasedStepExecutor().handleRequest(new ByteArrayInputStream(request.toByteArray()), os, ctx);

    logger.info("Response from simulated Lambda:\n{}", os.toString());
  }

  private static void sendLambdaStepRequest(LambdaBasedStep step, RequestType requestType) {
    InvokeResponse response = lambdaClient.invoke(InvokeRequest.builder()
        .functionName(Config.instance.STEP_LAMBDA_ARN.toString())
        .payload(SdkBytes.fromByteArray(prepareStepRequestPayload(step, requestType).toByteArray()))
        .build());
    try {
      logger.info("Response from lambda function: Status-code ({}), Payload:\n{}", response.statusCode(),
          XyzSerializable.serialize(XyzSerializable.deserialize(response.payload().asByteArray(), Map.class), true));
    }
    catch (JsonProcessingException e) {
      logger.error("Response from Lambda:\n{}\nError deserializing response from lambda:", response.payload().asUtf8String(), e);
    }
    logger.info("Logs from lambda function:\n{}", response.logResult());
  }

  private static LambdaStepRequest prepareStepRequestPayload(LambdaBasedStep step, RequestType requestType) {
    Map<String, Object> stepMap = step.toMap();
    stepMap.put("taskToken.$", "test123");
    stepMap.put("jobId", mockJob.getId());
    LambdaBasedStep enrichedStep = XyzSerializable.fromMap(stepMap, LambdaBasedStep.class);

    LambdaStepRequest request = new LambdaStepRequest().withStep(enrichedStep).withType(requestType);
    return request;
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
