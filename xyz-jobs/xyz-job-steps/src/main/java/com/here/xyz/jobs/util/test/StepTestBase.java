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

package com.here.xyz.jobs.util.test;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.here.xyz.jobs.steps.Step.InputSet.DEFAULT_INPUT_SET_NAME;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.START_EXECUTION;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.SUCCESS_CALLBACK;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.getTemporaryJobTableName;
import static com.here.xyz.jobs.steps.inputs.Input.inputS3Prefix;
import static com.here.xyz.util.Random.randomAlpha;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildSpaceTableDropIndexQueries;
import static java.net.http.HttpClient.Redirect.NORMAL;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.util.IOUtils;
import com.google.common.io.ByteStreams;
import com.google.common.net.MediaType;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.impl.transport.CountSpace;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.jobs.steps.impl.transport.TransportTools;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.jobs.util.S3Client;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.datasource.PooledDataSources;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.service.aws.SimulatedContext;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;

public class StepTestBase {

  public static final Config config = new Config();
  protected static final String LAMBDA_ARN = "arn:aws:lambda:us-east-1:000000000000:function:job-step";
  private static final Logger logger = LogManager.getLogger();
  private static final S3Client s3Client;
  private static final String PG_HOST = "localhost";
  private static final String PG_DB = "postgres";
  private static final String PG_USER = "postgres";
  private static final String PG_PW = "password";
  private static final String SCHEMA = "public";
  private static LambdaClient lambdaClient;
  private static PooledDataSources testDatasource;

  static {
    try {

      Config.instance.JOBS_S3_BUCKET = "test-bucket";
      Config.instance.AWS_REGION = "us-east-1";
      Config.instance.ECPS_PHRASE = "local";
      Config.instance.HUB_ENDPOINT = "http://localhost:8080/hub";
      Config.instance.LOCALSTACK_ENDPOINT = new URI("http://localhost:4566");
      Config.instance.JOB_API_ENDPOINT = new URL("http://localhost:7070");
      HubWebClient.STATISTICS_CACHE_TTL_SECONDS = 0;
      s3Client = S3Client.getInstance();
      lambdaClient = LambdaClient.builder()
          .region(Region.of(Config.instance.AWS_REGION))
          .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("localstack", "localstack")))
          .endpointOverride(Config.instance.LOCALSTACK_ENDPOINT)
          .build();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected String SPACE_ID = getClass().getSimpleName() + "_" + randomAlpha(5);
  protected String JOB_ID = generateJobId();

  private HubWebClient hubWebClient() {return HubWebClient.getInstance(config.HUB_ENDPOINT);}

  public String generateJobId() {
    return getClass().getSimpleName() + "_" + randomAlpha(5);
  }

  protected Space createSpace(String spaceId) {
    return createSpace(new Space().withId(spaceId), false);
  }

  protected Space createSpace(Space space, boolean force) {
    String title = "test space for jobs";
    try {
      space.setTitle(title);
      return hubWebClient().createSpace(space);
    }
    catch (XyzWebClient.ErrorResponseException e) {
      if (e.getErrorResponse().statusCode() == 409) {
        deleteSpace(space.getId());
        return createSpace(space, false);
      }
      else {
        System.out.println("Hub Error: " + e.getMessage());
      }
    }
    catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
    return null;
  }

  protected void patchSpace(String spaceId, Map<String, Object> spaceUpdates) {
    try {
      hubWebClient().patchSpace(spaceId, spaceUpdates);
    }
    catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
  }


  protected void createTag(String spaceId, Tag tag) {
    try {
      hubWebClient().postTag(spaceId, tag);
    }
    catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
  }

  protected void deleteTag(String spaceId, String tagId) {
    try {
      hubWebClient().deleteTag(spaceId, tagId);
    }
    catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
  }

  protected void deleteSpace(String spaceId) {
    try {
      hubWebClient().deleteSpace(spaceId);
    }
    catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
  }

  protected StatisticsResponse getStatistics(String spaceId) {
    try {
      return hubWebClient().loadSpaceStatistics(spaceId, SpaceContext.DEFAULT, true, true);
    }
    catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
    return null;
  }

  protected FeatureCollection getFeaturesFromSmallSpace(String spaceId, String propertyFilter, boolean force2D) {
    try {
      return hubWebClient().getFeaturesFromSmallSpace(spaceId, SpaceContext.DEFAULT, propertyFilter, force2D);
    }
    catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
    return null;
  }

  protected FeatureCollection getFeaturesFromSmallSpace(String spaceId, ContextAwareEvent.SpaceContext context, String propertyFilter,
      boolean force2D) {
    try {
      return hubWebClient().getFeaturesFromSmallSpace(spaceId, context, propertyFilter, force2D);
    }
    catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
    return null;
  }

  protected FeatureCollection customReadFeaturesQuery(String spaceId, String customPath) {
    try {
      return hubWebClient().customReadFeaturesQuery(spaceId, customPath);
    }
    catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
    return null;
  }

  protected void putFeatureCollectionToSpace(String spaceId, FeatureCollection fc) {
    try {
      hubWebClient().putFeaturesWithoutResponse(spaceId, fc);
    }
    catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
  }

  protected void deleteFeaturesInSpace(String spaceId, List<String> ids) {
    try {
      hubWebClient().deleteFeatures(spaceId, ids);
    }
    catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
  }

  protected void putRandomFeatureCollectionToSpace(String spaceId, int featureCount) {
    try {
      hubWebClient().putFeaturesWithoutResponse(spaceId, ContentCreator.generateRandomFeatureCollection(featureCount));
    }
    catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
  }

  protected void putRandomFeatureCollectionToSpace(String spaceId, int featureCount, float xmin, float ymin, float xmax, float ymax) {
    try {
      hubWebClient().putFeaturesWithoutResponse(spaceId,
          ContentCreator.generateRandomFeatureCollection(featureCount, xmin, ymin, xmax, ymax));
    }
    catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
  }

  protected void deleteAllExistingIndexes(String spaceId) throws SQLException {
    List<String> existingIndexes = listExistingIndexes(spaceId);
    List<SQLQuery> dropQueries = buildSpaceTableDropIndexQueries(SCHEMA, existingIndexes);
    SQLQuery.join(dropQueries, ";").write(getDataSourceProvider());
  }

  protected List<String> listExistingIndexes(String spaceId) throws SQLException {
    return new SQLQuery("SELECT * FROM xyz_index_list_all_available(#{schema}, #{table});")
        .withNamedParameter("schema", SCHEMA)
        .withNamedParameter("table", spaceId)
        .run(getDataSourceProvider(), rs -> {
          List<String> result = new ArrayList<>();
          while (rs.next())
            result.add(rs.getString(1));
          return result;
        });
  }

  private DataSourceProvider getDataSourceProvider() {
    if(testDatasource == null)
      testDatasource = new PooledDataSources(
            new DatabaseSettings("testSteps")
                    .withApplicationName(StepTestBase.class.getSimpleName())
                    .withHost(PG_HOST)
                    .withDb(PG_DB)
                    .withUser(PG_USER)
                    .withPassword(PG_PW)
                    .withDbMaxPoolSize(2));
    return testDatasource;
  }

  protected void deleteAllJobTables(List<String> stepIds) throws SQLException {
    List<SQLQuery> dropQueries = new ArrayList<>();
    for (String stepId : stepIds) {
      dropQueries.add(new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table};")
          .withVariable("schema", SCHEMA)
          .withVariable("table", TransportTools.getTemporaryJobTableName(stepId))
      );
      dropQueries.add(
          new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table};")
              .withVariable("schema", SCHEMA)
              .withVariable("table", TransportTools.getTemporaryTriggerTableName(stepId))
      );
    }
    SQLQuery.join(dropQueries, ";").write(getDataSourceProvider());
  }

  protected void cleanS3Files(String s3Prefix) {
    s3Client.deleteFolder(s3Prefix);
  }

  protected void sendLambdaStepRequestBlock(LambdaBasedStep step, boolean simulate)
          throws IOException, InterruptedException {
    try {
      step.prepare(null, null);
      if (!step.validate())
        throw new IllegalStateException(
            "The step " + step.getGlobalStepId() + " of type " + step.getClass().getSimpleName() + " is not ready for execution yet");
    }
    catch (ValidationException e) {
      throw new RuntimeException("Validation exception: " + e.getMessage(), e);
    }

    sendLambdaStepRequest(step, START_EXECUTION, simulate);

    DataSourceProvider dsp = getDataSourceProvider();

    if(   step instanceof ExportSpaceToFiles
       || step instanceof CountSpace ){
      waitTillTaskItemsAreFinalized(step);
    }else{
      waitTillAllQueriesAreFinalized(step);
    }

    if (simulate)
      sendLambdaStepRequest(step, SUCCESS_CALLBACK, true);
  }

  protected void waitTillTaskItemsAreFinalized(Step step)  throws InterruptedException{
    //WORKAROUND!! To solve UPDATE_CALLBACK problem.
    //This is only working if Lambda is installed. ExportSteps are using
    //Lambda calls from db to invoke new db thread calls.
    try{
      Integer i = -1;
      while (i != 0) {
        Thread.sleep(1000);
        SQLQuery query = retrieveNumberOfNotFinalizedTasks("public", step);
        i = query.run(getDataSourceProvider(), rs -> rs.next() ? rs.getInt(1) : null);
        logger.info("{} Threads are not finished!", i);
      }
    } catch (XyzWebClient.WebClientException | SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected void waitTillAllQueriesAreFinalized(Step step) throws InterruptedException{
    while (true) {
      Thread.sleep(500);
      try {
        boolean running = SQLQuery.isRunning(getDataSourceProvider(), false, "jobId", step.getJobId());
        if (!running)
          break;
      }
      catch (SQLException e) {
        break;
      }
    }
  }

  protected SQLQuery retrieveNumberOfNotFinalizedTasks(String schema, Step step) throws XyzWebClient.WebClientException {
    return new SQLQuery("SELECT count(1) from ${schema}.${table} WHERE finalized = false;")
            .withVariable("schema", schema)
            .withVariable("table", getTemporaryJobTableName(step.getId()));
  }

  protected void sendLambdaStepRequest(LambdaBasedStep step, LambdaBasedStep.LambdaStepRequest.RequestType requestType, boolean simulate)
      throws IOException {
    Map<String, Object> stepMap = step.toMap();
    stepMap.put("taskToken.$", "test123");
    stepMap.put("jobId", JOB_ID);
    LambdaBasedStep enrichedStep = XyzSerializable.fromMap(stepMap, LambdaBasedStep.class);
    LambdaBasedStep.LambdaStepRequest request = new LambdaBasedStep.LambdaStepRequest().withStep(enrichedStep).withType(requestType);

    logger.info("sendLambdaStepRequest with job-id: {}", JOB_ID);

    if (!simulate)
      invokeLambda(LAMBDA_ARN, request.toByteArray());
    else {
      OutputStream os = new ByteArrayOutputStream();
      Context ctx = new SimulatedContext("localLambda", null);
      new LambdaBasedStep.LambdaBasedStepExecutor().handleRequest(new ByteArrayInputStream(request.toByteArray()), os, ctx);
    }
  }

  private void invokeLambda(String lambdaArn, byte[] payload) {
    lambdaClient.invoke(InvokeRequest.builder()
        .functionName(lambdaArn)
        .payload(SdkBytes.fromByteArray(payload))
        .build());
  }

  //TODO: find a central place to avoid double implementation from JobPlayground
  public void uploadFiles(String jobId, int uploadFileCount, int featureCountPerFile, ImportFilesToSpace.Format format)
      throws IOException {
    //Generate N Files with M features
    for (int i = 0; i < uploadFileCount; i++)
      uploadInputFile(jobId, ContentCreator.generateImportFileContent(format, featureCountPerFile), S3ContentType.APPLICATION_JSON);
  }

  public void uploadInputFile(String jobId, byte[] bytes, S3ContentType contentType) throws IOException {
    uploadFileToS3(inputS3Prefix(jobId, DEFAULT_INPUT_SET_NAME) + "/" + UUID.randomUUID(), contentType, bytes, false);
  }

  protected void uploadFileToS3(String s3Key, S3ContentType contentType, byte[] data, boolean gzip) throws IOException {
    s3Client.putObject(s3Key, contentType.value, data, gzip);
  }

  public void uploadOutputFile(String jobId, String stepId, String outputSetName, byte[] bytes, S3ContentType contentType) throws IOException {
    uploadFileToS3(Output.stepOutputS3Prefix(jobId, stepId, outputSetName) + "/" + UUID.randomUUID(), contentType, bytes, false);
  }

  protected List<Feature> downloadFileAndSerializeFeatures(DownloadUrl output) throws IOException {
    logger.info("Check file: {}", output.getS3Key());
    List<Feature> features = new ArrayList<>();

    InputStream dataStream = S3Client.getInstance().streamObjectContent(output.getS3Key());

    if (output.isCompressed())
      dataStream = new GZIPInputStream(dataStream);

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(dataStream))) {
      String line;

      while ((line = reader.readLine()) != null) {
        features.add(XyzSerializable.deserialize(line, Feature.class));
      }
    }
    return features;
  }

  protected List<String> listFilesNamesFromArchive(URL url, boolean isCompressed, MediaType mediaType)
          throws URISyntaxException, IOException, InterruptedException {
    List<String> fileNames = new ArrayList<>();

    logger.info("Opening URL for archive processing: {}", url);

    HttpRequest request = createHttpRequest(url, mediaType);

    HttpResponse<InputStream> response = sendHttpRequest(request);

    try (InputStream compressedStream = isCompressed ? new GZIPInputStream(response.body()) : response.body();
         InputStream bufferedStream = new BufferedInputStream(compressedStream);
         ZipInputStream zipStream = new ZipInputStream(bufferedStream)) {

      ZipEntry entry;
      while ((entry = zipStream.getNextEntry()) != null) {
        if (!entry.isDirectory()) {
          fileNames.add(entry.getName());
        }
        zipStream.closeEntry();
      }
    } catch (IOException e) {
      throw new IOException("Error occurred while reading archive", e);
    }

    return fileNames;
  }

  protected List<String> downloadArchiveContentAsText(URL url, boolean isCompressed, MediaType mediaType)
          throws URISyntaxException, IOException, InterruptedException {
    List<String> archiveLines = new ArrayList<>();

    logger.info("Opening URL for archive processing: {}", url);

    HttpRequest request = createHttpRequest(url, mediaType);

    HttpResponse<InputStream> response = sendHttpRequest(request);

    try (InputStream compressedStream = isCompressed ? new GZIPInputStream(response.body()) : response.body();
         InputStream bufferedStream = new BufferedInputStream(compressedStream);
         ZipInputStream zipStream = new ZipInputStream(bufferedStream)) {

      ZipEntry entry;
      while ((entry = zipStream.getNextEntry()) != null) {
        if (!entry.isDirectory()) {
          System.out.println("File data content: ");
          byte[] c = IOUtils.toByteArray(zipStream);
          zipStream.closeEntry();
          String fileContent = new String(c);
          archiveLines.addAll(Arrays.stream(fileContent.split("\n")).toList());
        }
        zipStream.closeEntry();
      }
    } catch (IOException e) {
      throw new IOException("Error occurred while reading archive", e);
    }

    return archiveLines;
  }

  protected List<String> downloadFileAsText(URL url, boolean isCompressed, MediaType mediaType)
      throws IOException, URISyntaxException, InterruptedException {
    List<String> fileInLines = new ArrayList<>();

    logger.info("Check file: {}", url);

    HttpRequest request = createHttpRequest(url, mediaType);

    HttpResponse<InputStream> response = sendHttpRequest(request);

    try (InputStreamReader inputStreamReader = new InputStreamReader(isCompressed ? new GZIPInputStream(response.body()) : response.body());
         BufferedReader reader = new BufferedReader(inputStreamReader)) {
      String line;

      while ((line = reader.readLine()) != null) {
        fileInLines.add(line);
      }
    }
    return fileInLines;
  }


  private HttpRequest createHttpRequest(URL url, MediaType mediaType) throws URISyntaxException {
    return HttpRequest.newBuilder()
            .uri(url.toURI())
            .header(CONTENT_TYPE, mediaType.toString())
            .method("GET", HttpRequest.BodyPublishers.noBody())
            .version(HttpClient.Version.HTTP_1_1)
            .build();
  }

  private HttpResponse<InputStream> sendHttpRequest(HttpRequest request) throws IOException, InterruptedException {
    HttpClient client = HttpClient.newBuilder().followRedirects(NORMAL).build();

    HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());

    if (response.statusCode() >= 400) {
      logger.error("Received error response from server: {}", response.statusCode());
      throw new RuntimeException("Received HTTP error response with status code: " + response.statusCode());
    }

    return response;
  }

  protected FeatureCollection readTestFeatureCollection(String filePath) throws IOException {
    return XyzSerializable.deserialize(new String(ByteStreams.toByteArray(this.getClass().getResourceAsStream(filePath))).trim(),
        FeatureCollection.class);
  }

  public enum S3ContentType {
    APPLICATION_JSON("application/json"),
    TEXT_CSV("text/csv");

    private final String value;

    S3ContentType(String value) {
      this.value = value;
    }
  }

  protected long loadHeadVersion(String spaceId) throws WebClientException {
    return hubWebClient().loadSpaceStatistics(spaceId).getMaxVersion().getValue();
  }
}
