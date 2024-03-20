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

package com.here.xyz.jobs.config;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.service.Config;
import com.here.xyz.util.service.aws.dynamo.DynamoClient;
import com.here.xyz.util.service.aws.dynamo.IndexDefinition;
import io.vertx.core.Future;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DynamoJobConfigClient extends JobConfigClient {
  private static final Logger logger = LogManager.getLogger();
  private final Table jobTable;
  private final DynamoClient dynamoClient;

  public DynamoJobConfigClient(String tableArn) {
    dynamoClient = new DynamoClient(tableArn, null);
    logger.debug("Instantiating a reference to Dynamo Table {}", dynamoClient.tableName);
    jobTable = dynamoClient.db.getTable(dynamoClient.tableName);
  }

  public static class Provider extends JobConfigClient.Provider {
    @Override
    public boolean chooseMe() {
      return Config.instance.JOBS_DYNAMODB_TABLE_ARN != null && !"test".equals(System.getProperty("scope"));
    }

    @Override
    protected JobConfigClient getInstance() {
      return new DynamoJobConfigClient(Config.instance.JOBS_DYNAMODB_TABLE_ARN);
    }
  }

  @Override
  public Future<Job> loadJob(String resourceKey, String jobId) {
    return dynamoClient.executeQueryAsync(() -> {
      Item jobItem = jobTable.getItem("resourceKey", resourceKey, "id", jobId);
      return jobItem != null ? XyzSerializable.fromMap(jobItem.asMap(), Job.class) : null;
    });
  }

  @Override
  public Future<List<Job>> loadJobs(State state) {
    return dynamoClient.executeQueryAsync(() -> {
      List<Job> jobs = new LinkedList<>();
      jobTable.getIndex("state-index")
          .query(new QuerySpec().withHashKey("state", state.toString()))
          .pages()
          .forEach(page -> page.forEach(jobItem -> jobs.add(XyzSerializable.fromMap(jobItem.asMap(), Job.class))));
      return jobs;
    });
  }

  @Override
  public Future<List<Job>> loadJobs(String resourceKey) {
    return dynamoClient.executeQueryAsync(() -> {
      List<Job> jobs = new LinkedList<>();
      jobTable.query("resourceKey", resourceKey)
          .pages()
          .forEach(page -> page.forEach(jobItem -> jobs.add(XyzSerializable.fromMap(jobItem.asMap(), Job.class))));
      return jobs;
    });
  }

  @Override
  public Future<Void> storeJob(String resourceKey, Job job) {
    return dynamoClient.executeQueryAsync(() -> {
      //TODO: Ensure that concurrent writes do not produce invalid state-transitions using atomic writes
      jobTable.putItem(convertJobToItem(resourceKey, job));
      return null;
    });
  }

  @Override
  public Future<Void> updateState(String resourceKey, Job job, State expectedPreviousState) {
    if (expectedPreviousState == job.getStatus().getState())
      //Nothing to do
      return Future.succeededFuture();

    return dynamoClient.executeQueryAsync(() -> {
      jobTable.updateItem(new UpdateItemSpec()
          .withPrimaryKey("resourceKey", resourceKey, "id", job.getId())
          .withUpdateExpression("SET status.state = :newState")
          .withConditionExpression("status.state = :oldState")
          .withValueMap(Map.of(":newState", job.getStatus().getState().toString(), ":oldState", expectedPreviousState.toString())));
      return null;
    });
  }

  private Item convertJobToItem(String resourceKey, Job job) {
    Map<String, Object> jobItemData = job.toMap(Static.class);
    jobItemData.put("keepUntil", job.getKeepUntil() / 1000);
    jobItemData.put("resourceKey", resourceKey);
    return Item.fromMap(jobItemData);
  }

  @Override
  public Future<Void> deleteJob(String resourceKey, String jobId) {
    return dynamoClient.executeQueryAsync(() -> {
      jobTable.deleteItem(new DeleteItemSpec().withPrimaryKey("resourceKey", resourceKey, "id", jobId));
      return null;
    });
  }

  @Override
  public Future<Void> init() {
    if (dynamoClient.isLocal()) {
      logger.info("DynamoDB running locally, initializing Jobs table.");
      try {
        List<IndexDefinition> indexes = List.of(new IndexDefinition("state"));
        dynamoClient.createTable(jobTable.getTableName(), "resourceKey:S,id:S,state:S", "resourceKey,id", indexes, "keepUntil");
        //TODO: Register a dynamo stream (also in CFN) to ensure we're getting informed when a job expires
      }
      catch (Exception e) {
        logger.error("Failure during creating table on " + getClass().getSimpleName() + "#init()", e);
        return Future.failedFuture(e);
      }
    }
    return Future.succeededFuture();
  }
}
