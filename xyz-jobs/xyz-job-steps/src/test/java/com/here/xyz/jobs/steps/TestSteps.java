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

package com.here.xyz.jobs.steps;

import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.jobs.util.S3Client;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.DatabaseSettings;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.PooledDataSources;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildSpaceTableDropIndexQueries;

public class TestSteps {
  private static final HubWebClient hubWebClient;
  private static final S3Client s3Client;
  private static LambdaClient lambdaClient;
  private static final String PG_HOST = "localhost";
  private static final String PG_DB = "postgres";
  private static final String PG_USER = "postgres";
  private static final String PG_PW = "password";
  private static final String SCHEMA = "public";

  static {
    try {
      new Config();
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

  public enum S3ContentType {
    APPLICATION_JSON("application/json"),
    TEXT_CSV("text/csv");

    private final String value;
    S3ContentType(String value) { this.value = value; }
  }

  private static DataSourceProvider getDataSourceProvider() {
    return new PooledDataSources(new DatabaseSettings("testPSQL")
            .withHost(PG_HOST)
            .withDb(PG_DB)
            .withUser(PG_USER)
            .withPassword(PG_PW)
            .withDbMaxPoolSize(2));
  }

  protected static void createSpace(String spaceId) {
    try {
      hubWebClient.createSpace(spaceId, "test space for jobs");
    } catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
  }

  protected static void deleteSpace(String spaceId) {
    try {
      hubWebClient.deleteSpace(spaceId);
    } catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
  }

  protected static StatisticsResponse getStatistics(String spaceId) {
    try {
      return hubWebClient.loadSpaceStatistics(spaceId, ContextAwareEvent.SpaceContext.EXTENSION);
    } catch (XyzWebClient.WebClientException e) {
      System.out.println("Hub Error: " + e.getMessage());
    }
    return null;
  }

  protected static List<String> listExistingIndexes(String spaceId) throws SQLException {
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

  protected void uploadFileToS3(String s3Key, S3ContentType contentType, byte[] data, boolean gzip) throws IOException {
    s3Client.putObject(s3Key, contentType.value, data, gzip);
  }

  protected void cleanS3Files(String s3Prefix) {
    s3Client.deleteFolder(s3Prefix);
  }

  protected void invokeLambda(String lambdaArn, byte[] payload) {
    lambdaClient.invoke(InvokeRequest.builder()
            .functionName(lambdaArn)
            .payload(SdkBytes.fromByteArray(payload))
            .build());
  }

}
