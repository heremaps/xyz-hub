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

package com.here.xyz.jobs.util.test;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.START_EXECUTION;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.SUCCESS_CALLBACK;
import static com.here.xyz.jobs.steps.inputs.Input.inputS3Prefix;
import static com.here.xyz.util.Random.randomAlpha;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildSpaceTableDropIndexQueries;
import static java.net.http.HttpClient.Redirect.NORMAL;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.common.io.ByteStreams;
import com.google.common.net.MediaType;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.jobs.steps.impl.transport.TransportTools;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
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
import com.here.xyz.util.service.aws.SimulatedContext;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;

public class StepTestBase {
  private static final Logger logger = LogManager.getLogger();
  protected String SPACE_ID =  getClass().getSimpleName() + "_" + randomAlpha(5);
  protected String JOB_ID =  generateJobId();

  protected static final String LAMBDA_ARN = "arn:aws:lambda:us-east-1:000000000000:function:job-step";
  private static final HubWebClient hubWebClient;
  private static final S3Client s3Client;
  private static LambdaClient lambdaClient;
  private static final String PG_HOST = "localhost";
  private static final String PG_DB = "postgres";
  private static final String PG_USER = "postgres";
  private static final String PG_PW = "password";
  private static final String SCHEMA = "public";
  public static final Config config = new Config();

  static {
    try {

      Config.instance.JOBS_S3_BUCKET = "test-bucket";
      Config.instance.AWS_REGION = "us-east-1";
      Config.instance.ECPS_PHRASE = "local";
      Config.instance.HUB_ENDPOINT = "http://localhost:8080/hub";
      Config.instance.LOCALSTACK_ENDPOINT = new URI("http://localhost:4566");
      Config.instance.JOB_API_ENDPOINT = new URL("http://localhost:7070");
      hubWebClient = HubWebClient.getInstance("http://localhost:8080/hub");
      s3Client = S3Client.getInstance();
      lambdaClient = LambdaClient.builder()
              .region(Region.of(Config.instance.AWS_REGION))
              .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("localstack", "localstack")))
              .endpointOverride(Config.instance.LOCALSTACK_ENDPOINT)
              .build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String generateJobId(){
    return getClass().getSimpleName() + "_" + randomAlpha(5);
  }

  public enum S3ContentType {
    APPLICATION_JSON("application/json"),
    TEXT_CSV("text/csv");

    private final String value;
    S3ContentType(String value) { this.value = value; }
  }

  private DataSourceProvider getDataSourceProvider() {
    return new PooledDataSources(
            new DatabaseSettings("testSteps")
            .withApplicationName(StepTestBase.class.getSimpleName())
            .withHost(PG_HOST)
            .withDb(PG_DB)
            .withUser(PG_USER)
            .withPassword(PG_PW)
            .withDbMaxPoolSize(2));
  }

  protected Space createSpace(String spaceId){
    return createSpace(new Space().withId(spaceId), false);
  }

  protected Space createSpace(Space space, boolean force) {
    String title = "test space for jobs";
    try {
      space.setTitle(title);
      return hubWebClient.createSpace(space);
    }
    catch (XyzWebClient.ErrorResponseException e) {
      if (e.getErrorResponse().statusCode() == 409) {
        deleteSpace(space.getId());
        return createSpace(space, false);
      }
      else {
        System.out.println("Hub Error: " + e.getMessage());
      }
    } catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
    return null;
  }

  protected void patchSpace(String spaceId, Map<String,Object> spaceUpdates) {
    try {
      hubWebClient.patchSpace(spaceId, spaceUpdates);
    } catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
  }

  protected void createTag(String spaceId, Tag tag) {
    try {
      hubWebClient.postTag(spaceId, tag);
    } catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
  }

  protected void deleteSpace(String spaceId) {
    try {
      hubWebClient.deleteSpace(spaceId);
    } catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
  }

  protected StatisticsResponse getStatistics(String spaceId) {
    try {
      return hubWebClient.loadSpaceStatistics(spaceId, SpaceContext.EXTENSION, true);
    } catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
    return null;
  }

  protected FeatureCollection getFeaturesFromSmallSpace(String spaceId ,String propertyFilter, boolean force2D) {
    try {
      return hubWebClient.getFeaturesFromSmallSpace(spaceId, SpaceContext.EXTENSION, propertyFilter, force2D);
    } catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
    return null;
  }

  protected FeatureCollection getFeaturesFromSmallSpace(String spaceId, ContextAwareEvent.SpaceContext context, String propertyFilter, boolean force2D) {
    try {
      return hubWebClient.getFeaturesFromSmallSpace(spaceId, context, propertyFilter, force2D);
    } catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
    return null;
  }

  protected FeatureCollection customReadFeaturesQuery(String spaceId, String customPath) {
    try {
      return hubWebClient.customReadFeaturesQuery(spaceId, customPath);
    } catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
    return null;
  }

  protected void putFeatureCollectionToSpace(String spaceId, FeatureCollection fc) {
    try {
      hubWebClient.putFeaturesWithoutResponse(spaceId, fc);
    } catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
  }

  protected void deleteFeaturesInSpace(String spaceId, List<String> ids) {
    try {
      hubWebClient.deleteFeatures(spaceId, ids);
    } catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
  }

  protected void putRandomFeatureCollectionToSpace(String spaceId, int featureCount) {
    try {
      hubWebClient.putFeaturesWithoutResponse(spaceId, ContentCreator.generateRandomFeatureCollection(featureCount));
    } catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
  }

  protected void putRandomFeatureCollectionToSpace(String spaceId, int featureCount ,float xmin, float ymin, float xmax, float ymax)
  {
    try {
      hubWebClient.putFeaturesWithoutResponse(spaceId, ContentCreator.generateRandomFeatureCollection(featureCount,xmin,ymin,xmax,ymax));
    } catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
  }

  protected List<String> listExistingIndexes(String spaceId) throws SQLException {
    return new SQLQuery("SELECT * FROM xyz_index_list_all_available(#{schema}, #{table});")
            .withNamedParameter("schema", SCHEMA)
            .withNamedParameter("table", spaceId)
            .run(getDataSourceProvider(), rs -> {
              List<String> result = new ArrayList<>();
              while(rs.next())
                result.add(rs.getString(1));
              return result;
            });
  }

  protected void deleteAllExistingIndexes(String spaceId) throws SQLException {
    List<String> existingIndexes = listExistingIndexes(spaceId);
    List<SQLQuery> dropQueries = buildSpaceTableDropIndexQueries(SCHEMA, existingIndexes);
    SQLQuery.join(dropQueries, ";").write(getDataSourceProvider());
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

  protected void uploadFileToS3(String s3Key, S3ContentType contentType, byte[] data, boolean gzip) throws IOException {
    s3Client.putObject(s3Key, contentType.value, data, gzip);
  }

  protected void cleanS3Files(String s3Prefix) {
    s3Client.deleteFolder(s3Prefix);
  }

  private void invokeLambda(String lambdaArn, byte[] payload) {
    lambdaClient.invoke(InvokeRequest.builder()
            .functionName(lambdaArn)
            .payload(SdkBytes.fromByteArray(payload))
            .build());
  }

  protected void sendLambdaStepRequestBlock(LambdaBasedStep step, boolean simulate) throws IOException, InterruptedException {
    sendLambdaStepRequest(step, START_EXECUTION, simulate);
    DataSourceProvider dsp = getDataSourceProvider();

    while (true) {
      Thread.sleep(500);
      try {
        boolean running = SQLQuery.isRunning(dsp, false, "jobId", step.getJobId());
        if(!running)
          break;
      }catch (SQLException e) {
        break;
      }
    }

    if(simulate)
      sendLambdaStepRequest(step, SUCCESS_CALLBACK, true);
  }

  protected void sendLambdaStepRequest(LambdaBasedStep step, LambdaBasedStep.LambdaStepRequest.RequestType requestType, boolean simulate) throws IOException {
    Map<String, Object> stepMap = step.toMap();
    stepMap.put("taskToken.$", "test123");
    stepMap.put("jobId", JOB_ID);
    LambdaBasedStep enrichedStep = XyzSerializable.fromMap(stepMap, LambdaBasedStep.class);
    LambdaBasedStep.LambdaStepRequest request = new LambdaBasedStep.LambdaStepRequest().withStep(enrichedStep).withType(requestType);

    logger.info("sendLambdaStepRequest with job-id: {}", JOB_ID);

    if(!simulate)
      invokeLambda(LAMBDA_ARN, request.toByteArray());
    else{
      OutputStream os = new ByteArrayOutputStream();
      Context ctx = new SimulatedContext("localLambda", null);
      new LambdaBasedStep.LambdaBasedStepExecutor().handleRequest(new ByteArrayInputStream(request.toByteArray()), os, ctx);
    }
  }

  //TODO: find a central place to avoid double implementation from JobPlayground
  public void uploadFiles(String jobId, int uploadFileCount, int featureCountPerFile, ImportFilesToSpace.Format format)
          throws IOException {
    //Generate N Files with M features
    for (int i = 0; i < uploadFileCount; i++)
      uploadInputFile(jobId, ContentCreator.generateImportFileContent(format, featureCountPerFile), S3ContentType.APPLICATION_JSON);
  }

  public void uploadInputFile(String jobId , byte[] bytes, S3ContentType contentType) throws IOException {
    uploadFileToS3(inputS3Prefix(jobId) + "/" + UUID.randomUUID(), contentType, bytes, false);
  }

  protected List<Feature> downloadFileAndSerializeFeatures(DownloadUrl output) throws IOException {
    logger.info("Check file: {}",output.getS3Key());
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

  protected List<String> downloadFileAsText(URL url, boolean isCompressed, MediaType mediaType) throws IOException, URISyntaxException, InterruptedException {
    List<String> fileInLines = new ArrayList<>();

    logger.info("Check file: {}", url);
    InputStream dataStream;
    HttpRequest request = HttpRequest.newBuilder()
            .uri(url.toURI())
            .header(CONTENT_TYPE, mediaType.toString())
            .method("GET", HttpRequest.BodyPublishers.noBody())
            .version(HttpClient.Version.HTTP_1_1)
            .build();

    HttpClient client = HttpClient.newBuilder().followRedirects(NORMAL).build();
    HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
    if (response.statusCode() >= 400)
      throw new RuntimeException("Received error response!");

    dataStream = response.body();

    if (isCompressed)
      dataStream = new GZIPInputStream(dataStream);

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(dataStream))) {
      String line;

      while ((line = reader.readLine()) != null) {
        fileInLines.add(line);
      }
    }
    return fileInLines;
  }

  protected FeatureCollection readTestFeatureCollection(String filePath) throws IOException {
    return XyzSerializable.deserialize( new String(ByteStreams.toByteArray(this.getClass().getResourceAsStream(filePath))).trim(), FeatureCollection.class);
  }
}
