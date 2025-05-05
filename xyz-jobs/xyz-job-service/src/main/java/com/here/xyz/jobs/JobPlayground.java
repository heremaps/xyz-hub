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

package com.here.xyz.jobs;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.here.xyz.jobs.JobPlayground.Usecase.COPY;
import static com.here.xyz.jobs.JobPlayground.Usecase.EXPORT;
import static com.here.xyz.jobs.JobPlayground.Usecase.IMPORT;
import static com.here.xyz.jobs.datasets.files.FileFormat.EntityPerLine.Feature;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.START_EXECUTION;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.SUCCESS_CALLBACK;
import static java.net.http.HttpClient.Redirect.NORMAL;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.jobs.config.JobConfigClient;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.FileOutputSettings;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.datasets.files.FileInputSettings;
import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.service.Config;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaBasedStepExecutor;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType;
import com.here.xyz.jobs.steps.impl.AnalyzeSpaceTable;
import com.here.xyz.jobs.steps.impl.CreateIndex;
import com.here.xyz.jobs.steps.impl.DropIndexes;
import com.here.xyz.jobs.steps.impl.MarkForMaintenance;
import com.here.xyz.jobs.steps.impl.transport.CopySpace;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.ARN;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper.SystemIndex;
import com.here.xyz.util.runtime.LambdaFunctionRuntime;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.aws.SimulatedContext;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.ErrorResponseException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

//TODO: Delete or extend JopTestBase and cleanUp.
public class JobPlayground {
  private static final Logger logger = LogManager.getLogger();
  private static HubWebClient hubWebClient;
  private static LambdaClient lambdaClient;
  private static Space sampleSpace;
  private static Space targetSpace;
  private static boolean simulateExecution = true;
  private static boolean executeWholeJob = false;
  private static ImportFilesToSpace.Format importFormat = ImportFilesToSpace.Format.GEOJSON;
  private static int uploadFileCount = 2;
  private static String jobServiceBaseUrl = "http://localhost:7070";

  private static Usecase playgroundUsecase = EXPORT;

  protected enum Usecase {
    IMPORT,
    EXPORT,
    COPY;
  }

  static {
    XyzSerializable.registerSubtypes(StepGraph.class);
    XyzSerializable.registerSubtypes(Input.class);
    XyzSerializable.registerSubtypes(Output.class);

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

  private static void init() throws WebClientException, JsonProcessingException {
    sampleSpace = createPlaygroundSpace("TEST");

    JobConfigClient.getInstance().init();

    if(playgroundUsecase.equals(IMPORT)) {
      mockJob = new Job().create()
              .withDescription("Sample import job")
              .withOwner("me")
              .withSource(new Files<>().withInputSettings(new FileInputSettings().withFormat(new GeoJson().withEntityPerLine(Feature))))
              .withTarget(new DatasetDescription.Space<>().withId(sampleSpace.getId()));
    }else if(playgroundUsecase.equals(COPY)) {
      addTestFeaturesToSpace(sampleSpace.getId(), 10);

      targetSpace = createPlaygroundSpace("TEST-TARGET");
      mockJob = new Job().create()
              .withDescription("Sample copy job")
              .withOwner("me")
              .withSource(new DatasetDescription.Space<>().withId(sampleSpace.getId()))
              .withTarget(new DatasetDescription.Space<>().withId(targetSpace.getId()));

    }else if(playgroundUsecase.equals(EXPORT)) {
      mockJob = new Job().create()
              .withDescription("Sample export job")
              .withOwner("me")
              .withSource(new DatasetDescription.Space<>().withId("REPLACE_WITH_EXISITNG"))
              .withTarget(new Files<>().withOutputSettings(new FileOutputSettings().withFormat(new GeoJson())));
    }
  }

  private static Job mockJob;

  public static void main(String[] args) throws IOException, InterruptedException, WebClientException {

    String realJobSourceSpaceId = "TEST";
    //For Copy needed
    String realJobTargetSpaceId = "TEST-TARGET";

    if (args.length > 0) {
      realJobSourceSpaceId = args[0];
      if (args.length > 1)
        jobServiceBaseUrl = args[1];
      if (args.length > 2)
      realJobTargetSpaceId = args[2];
    }
    else
      init();

//    startRealJob(realJobSourceSpaceId, realJobTargetSpaceId);

    init();

    if (executeWholeJob)
      startMockJob();
    else
      startLambdaExecutions();
  }

  private static void startLambdaExecutions() throws IOException, WebClientException {
    if (playgroundUsecase == IMPORT) {
      uploadFiles();

      runDropIndexStep(sampleSpace.getId());

      runImportFilesToSpaceStep(sampleSpace.getId(), importFormat);

      for (SystemIndex index : SystemIndex.values())
        runCreateIndexStep(sampleSpace.getId(), index);

      runAnalyzeSpaceTableStep(sampleSpace.getId());

      runMarkForMaintenanceStep(sampleSpace.getId());
    }
    else if (playgroundUsecase == COPY)
      runCopySpaceStep(sampleSpace.getId(), targetSpace.getId());
    else if (playgroundUsecase == EXPORT)
      runExportSpaceToFilesStep(sampleSpace.getId());
  }

  private static void uploadFiles() throws IOException {
    //Generate N Files with M features
    for (int i = 0; i < uploadFileCount; i++)
      uploadInputFile(generateContent(importFormat, 10));
  }

  private static void uploadFilesToRealJob(String jobId) throws IOException, InterruptedException {
    //Generate N Files with M features
    for (int i = 0; i < uploadFileCount; i++) {
      HttpResponse<byte[]> inputResponse = post("/jobs/" + jobId + "/inputs", Map.of("type", "UploadUrl"));
      String uploadUrl = (String) XyzSerializable.deserialize(inputResponse.body(), Map.class).get("url");
      uploadInputFile(generateContent(importFormat, 10), new URL(uploadUrl));
    }

  }

  private static void pollRealJobStatus(String jobId) throws InterruptedException {
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    //Poll job status every 5 seconds
    executor.scheduleAtFixedRate(() -> {
      try {
        HttpResponse<byte[]> statusResponse = get("/jobs/" + jobId + "/status");
        RuntimeStatus status = XyzSerializable.deserialize(statusResponse.body(), RuntimeStatus.class);
        logger.info("Job state for {}: {} ({}/{} steps succeeded)", jobId, status.getState(), status.getSucceededSteps(),
            status.getOverallStepCount());
        if (status.getState().isFinal())
          executor.shutdownNow();
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 0, 5, TimeUnit.SECONDS);

    int timeoutSeconds = 120;
    if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
      executor.shutdownNow();
      logger.info("Stopped polling status for job {} after timeout {} seconds", jobId, timeoutSeconds);
    }
  }

  private static byte[] generateContent(ImportFilesToSpace.Format format, int featureCnt) {
    String output = "";

    for (int i = 1; i <= featureCnt; i++) {
      output += generateContentLine(format, i);
    }
    return output.getBytes();
  }

  private static void generateContentToFile(ImportFilesToSpace.Format format, int featureCnt, boolean beZipped) throws IOException {
    String outputFile = "/tmp/output.file" + (beZipped ? ".gz" : "");

    BufferedWriter writer = null;
    try {
      if(!beZipped)
        writer = new BufferedWriter(new FileWriter(outputFile, true));
      else{
        GZIPOutputStream zip = new GZIPOutputStream(
                new FileOutputStream(outputFile));

        writer = new BufferedWriter(
                new OutputStreamWriter(zip, "UTF-8"));
      }
      for (int i = 1; i <= featureCnt; i++) {
        writer.write(generateContentLine(format, i));
      }
    }finally {
      if (writer != null)
        writer.close();
    }
  }

  private static String generateContentLine(ImportFilesToSpace.Format format, int i){
    Random rd = new Random();
    String lineSeparator = "\n";

    if(format.equals(ImportFilesToSpace.Format.CSV_JSON_WKB))
      return "\"{'\"properties'\": {'\"test'\": "+i+"}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A400000000000000000"+lineSeparator;
    else if(format.equals(ImportFilesToSpace.Format.CSV_GEOJSON))
      return "\"{'\"type'\":'\"Feature'\",'\"geometry'\":{'\"type'\":'\"Point'\",'\"coordinates'\":["+(rd.nextInt(179))+"."+(rd.nextInt(100))+","+(rd.nextInt(79))+"."+(rd.nextInt(100))+"]},'\"properties'\":{'\"test'\":"+i+"}}\""+lineSeparator;
    else
      return "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":["+(rd.nextInt(179))+"."+(rd.nextInt(100))+","+(rd.nextInt(79))+"."+(rd.nextInt(100))+"]},\"properties\":{\"te\\\"st\":"+i+"}}"+lineSeparator;
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

  private static void startRealJob(String sourceSpaceId, String targetSpaceId) throws IOException, InterruptedException {
    Job job = null;

    if(playgroundUsecase.equals(IMPORT)) {
      job = new Job().create()
              .withDescription("Sample import job")
              .withOwner("me")
              .withSource(new Files<>().withInputSettings(new FileInputSettings().withFormat(new GeoJson().withEntityPerLine(Feature))))
              .withTarget(new DatasetDescription.Space<>().withId(sourceSpaceId));
    }else if(playgroundUsecase.equals(COPY)) {
      job = new Job().create()
              .withDescription("Sample copy job")
              .withOwner("me")
              .withSource(new DatasetDescription.Space<>().withId(sourceSpaceId))
              .withTarget(new DatasetDescription.Space<>().withId(targetSpaceId));
    }

    System.out.println("Creating job ...");
    HttpResponse<byte[]> jobResponse = post("/jobs", job);

    System.out.println("Got response:");
    System.out.println(new String(jobResponse.body()));

    Job createdJob = XyzSerializable.deserialize(jobResponse.body(), Job.class);
    String jobId = createdJob.getId();

    if(playgroundUsecase.equals(IMPORT)) {
      //Uploading files
      uploadFilesToRealJob(jobId);
      //Start the job execution
      patch("/jobs/" + jobId + "/status", Map.of("desiredAction", "START"));
    }

    pollRealJobStatus(jobId);
  }

  private static HttpResponse<byte[]> get(String path) throws IOException, InterruptedException {
    return request("GET", path, null);
  }

  private static HttpResponse<byte[]> post(String path, Object requestPayload) throws IOException, InterruptedException {
    return request("POST", path, requestPayload);
  }

  private static HttpResponse<byte[]> patch(String path, Object requestPayload) throws IOException, InterruptedException {
    return request("PATCH", path, requestPayload);
  }

  private static HttpResponse<byte[]> request(String method, String path, Object requestPayload) throws IOException, InterruptedException {
    BodyPublisher bodyPublisher = requestPayload == null ? BodyPublishers.noBody()
            : BodyPublishers.ofByteArray(XyzSerializable.serialize(requestPayload).getBytes());

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(jobServiceBaseUrl + path))
        .header(CONTENT_TYPE, JSON_UTF_8.toString())
        .method(method, bodyPublisher)
        .version(Version.HTTP_1_1)
        .build();

    HttpClient client = HttpClient.newBuilder().followRedirects(NORMAL).build();
    HttpResponse<byte[]> response = client.send(request, BodyHandlers.ofByteArray());
    if (response.statusCode() >= 400)
      throw new RuntimeException("Received error response with status code: " + response.statusCode() + " response:\n"
          + new String(response.body()));
    return response;
  }

  private static Space createPlaygroundSpace(String spaceId) throws WebClientException {
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

  protected static void addTestFeaturesToSpace(String spaceId, int count) throws WebClientException, JsonProcessingException {
    FeatureCollection fc = new FeatureCollection();
    for (int i = 0; i < count; i++)
      fc.getFeatures().add(new Feature().withProperties(new Properties().with("test", i))
              .withGeometry(new Point().withCoordinates(new PointCoordinates(i, i % 90))));
    hubWebClient.putFeaturesWithoutResponse(spaceId, fc);
  }

  private static void uploadInputFile(byte[] data) throws IOException {
    uploadInputFile(data, false);
  }

  private static void uploadInputFile(byte[] data, boolean compressed) throws IOException {
    //TODO: Compression

    URL uploadUrl = mockJob.createUploadUrl(compressed).getUrl();

    uploadInputFile(data, uploadUrl);
  }

  private static void uploadInputFile(byte[] data, URL uploadUrl) throws IOException {
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
    runStep(new ImportFilesToSpace().withSpaceId(spaceId).withFormat(format).withUpdateStrategy(UpdateStrategy.DEFAULT_UPDATE_STRATEGY));
  }

  public static void runCreateIndexStep(String spaceId, SystemIndex index) throws IOException {
    runStep(new CreateIndex().withSpaceId(spaceId).withIndex(index));
  }

  public static void runAnalyzeSpaceTableStep(String spaceId) throws IOException {
    runStep(new AnalyzeSpaceTable().withSpaceId(spaceId));
  }

  public static void runMarkForMaintenanceStep(String spaceId) throws IOException {
    runStep(new MarkForMaintenance().withSpaceId(spaceId));
  }

  public static void runCopySpaceStep(String sourceSpaceId, String targetSpaceId) throws IOException, WebClientException {
    //NOTE: The source version must always be a resolved one when using the copy step directly
    long headVersion = hubWebClient.loadSpaceStatistics(sourceSpaceId).getMaxVersion().getValue();
    runStep(new CopySpace().withSpaceId(sourceSpaceId).withTargetSpaceId(targetSpaceId).withSourceVersionRef(new Ref(headVersion)));
  }

  public static void runExportSpaceToFilesStep(String sourceSpaceId) throws IOException {
    runStep(new ExportSpaceToFiles().withSpaceId(sourceSpaceId));
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
    new LambdaFunctionRuntime(ctx, step.getGlobalStepId());

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


  //tmp hack
  static {
    XyzSerializable.registerSubtypes(StepGraph.class);
    XyzSerializable.registerSubtypes(Input.class);
    XyzSerializable.registerSubtypes(Output.class);
  }
}
