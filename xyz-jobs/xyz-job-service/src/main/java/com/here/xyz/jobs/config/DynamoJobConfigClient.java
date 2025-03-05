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
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.service.Config;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.util.service.aws.dynamo.DynamoClient;
import com.here.xyz.util.service.aws.dynamo.IndexDefinition;
import io.vertx.core.Future;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
      return !"test".equals(System.getProperty("scope"));
    }

    @Override
    protected JobConfigClient getInstance() {
      if (Config.instance == null || Config.instance.JOBS_DYNAMODB_TABLE_ARN == null)
        throw new NullPointerException("Config variable JOBS_DYNAMODB_TABLE_ARN is not defined");
      return new DynamoJobConfigClient(Config.instance.JOBS_DYNAMODB_TABLE_ARN);
    }
  }

  @Override
  public Future<Job> loadJob(String jobId) {
    return dynamoClient.executeQueryAsync(() -> {
      Item jobItem = jobTable.getItem("id", jobId);
      return jobItem != null ? XyzSerializable.fromMap(jobItem.asMap(), Job.class) : null;
    });
  }

  @Override
  public Future<List<Job>> loadJobs() {
    return dynamoClient.executeQueryAsync(() -> {
      List<Job> jobs = new LinkedList<>();
      jobTable.scan()
          .pages()
          .forEach(page -> page.forEach(jobItem -> jobs.add(XyzSerializable.fromMap(jobItem.asMap(), Job.class))));
      return jobs;
    });
  }

  @Override
  public Future<List<Job>> loadJobs(boolean newerThan, long createdAt) {
    //TODO: Use index with sort-key on "createdAt" attribute instead of scan()
    return dynamoClient.executeQueryAsync(() -> {
      List<Job> jobs = new LinkedList<>();
      jobTable.scan("#createdAt " + (newerThan ? ">" : "<") + " :ts", Map.of("#createdAt", "createdAt"), Map.of(":ts",  createdAt))
          .pages()
          .forEach(page -> page.forEach(jobItem -> jobs.add(XyzSerializable.fromMap(jobItem.asMap(), Job.class))));
      return jobs;
    });
  }

  @Override
  public Future<List<Job>> loadJobs(State state) {
    return dynamoClient.executeQueryAsync(() -> {
      List<Job> jobs = new LinkedList<>();
      jobTable.getIndex("state-index")
          .query("state", state.toString())
          .pages()
          .forEach(page -> page.forEach(jobItem -> jobs.add(XyzSerializable.fromMap(jobItem.asMap(), Job.class))));
      return jobs;
    });
  }

  public Future<Set<Job>> loadJobs(String resourceKey, String secondaryResourceKey) {
    if (secondaryResourceKey == null)
      return loadJobs(resourceKey, resourceKey);

    return Future.all(
        loadJobs(resourceKey),
        loadJobs(secondaryResourceKey)
    ).map(cf -> cf.<Set<Job>>list().stream().flatMap(jobs -> jobs.stream()).collect(Collectors.toSet()));
  }

  @Override
  public Future<Set<Job>> loadJobs(String resourceKey) {
    return dynamoClient.executeQueryAsync(() -> {
      Set<Job> jobs = new HashSet<>();
      jobs.addAll(queryIndex("resourceKey", resourceKey));
      jobs.addAll(queryIndex("secondaryResourceKey", resourceKey));
      return jobs;
    });
  }

  private Set<Job> queryIndex(String resourceKeyFieldName, String resourceKey) {
    Set<Job> jobs = new HashSet<>();
    jobTable.getIndex(resourceKeyFieldName + "-index")
        .query(resourceKeyFieldName, resourceKey)
        .pages()
        .forEach(page -> page.forEach(jobItem -> jobs.add(XyzSerializable.fromMap(jobItem.asMap(), Job.class))));
    return jobs;
  }

  @Override
  public Future<List<Job>> loadJobs(String resourceKey, State state) {
    if (resourceKey != null)
      //TODO: Use an index with hash- *and* range-key
      return loadJobs(resourceKey).map(jobs -> jobs.stream().filter(job -> state == null || job.getStatus().getState() == state).toList());
    else if (state != null)
      return loadJobs(state);
    else
      return loadJobs();
  }

  @Override
  public Future<List<Job>> loadJobs(String resourceKey, String secondaryResourceKey, State state) {
    if (resourceKey != null)
      //TODO: Use an index with hash- *and* range-key
      return loadJobs(resourceKey, secondaryResourceKey).map(jobs -> jobs.stream().filter(job -> state == null || job.getStatus().getState() == state).toList());
    else if (state != null)
      return loadJobs(state);
    else
      return loadJobs();
  }

  @Override
  public Future<Void> storeJob(Job job) {
    return dynamoClient.executeQueryAsync(() -> {
      //TODO: Ensure that concurrent writes do not produce invalid state-transitions using atomic writes
      jobTable.putItem(convertJobToItem(job));
      return null;
    });
  }

  @Override
  public Future<Void> updateState(Job job, State expectedPreviousState) {
    if (expectedPreviousState == job.getStatus().getState())
      //Nothing to do
      return Future.succeededFuture();

    return dynamoClient.executeQueryAsync(() -> {
      jobTable.updateItem(new UpdateItemSpec()
          .withPrimaryKey("id", job.getId())
          .withUpdateExpression("SET #state = :newState")
          .withConditionExpression("#state = :oldState")
          .withNameMap(Map.of("#state", "status.state"))
          .withValueMap(Map.of(":newState", job.getStatus().getState().toString(), ":oldState", expectedPreviousState.toString())));
      return null;
    });
  }

  @Override
  public Future<Void> updateStatus(Job job, State expectedPreviousState) {
    return dynamoClient.executeQueryAsync(() -> {
      final Map<String, String> nameMap = Map.of("#status", "status", "#state", "state");
      final Map<String, Object> valueMap = new HashMap<>(Map.of(":newStatus", job.getStatus().toMap(), ":newState",
          job.getStatus().getState().toString()));
      final UpdateItemSpec updateItemSpec = new UpdateItemSpec()
          .withPrimaryKey("id", job.getId())
          .withUpdateExpression("SET #status = :newStatus, #state = :newState")
          .withNameMap(nameMap)
          .withValueMap(valueMap);

      if (expectedPreviousState != null) {
        //TODO: Allow multiple expected previous steps?
        valueMap.put(":oldState", expectedPreviousState.toString());
        updateItemSpec.withConditionExpression("#state = :oldState")
            .withValueMap(valueMap);
      }

      jobTable.updateItem(updateItemSpec);
      return null;
    });
  }

  @Override
  public Future<Void> updateStep(Job job, Step<?> newStep) {
    final String stepPath = buildStepPath(job, newStep);
    final List<State> finalStates = Stream.of(State.values())
        .filter(state -> state.isFinal())
        .collect(Collectors.toUnmodifiableList());
    Map<String, Object> valueMap = new HashMap<>(Map.of(":newStep", newStep.toMap()));
    finalStates.forEach(state -> valueMap.put(":" + state, state.toString()));

    return dynamoClient.executeQueryAsync(() -> {
      try {
        jobTable.updateItem(new UpdateItemSpec()
            .withPrimaryKey("id", job.getId())
            .withUpdateExpression("SET " + stepPath + " = :newStep")
            //NOTE: The condition ensures that we're not further updating a step that has a final state
            .withConditionExpression(finalStates.stream()
                .map(state -> "#state <> :" + state)
                .collect(Collectors.joining(" AND ")))
            .withNameMap(Map.of("#state", stepPath + ".status.state"))
            .withValueMap(valueMap));
        return null;
      }
      catch (Exception e) {
        logger.error(e);
        return null;
      }
    });
  }

  private String buildStepPath(Job job, Step<?> step) {
    return "steps." + job.getSteps().findStepPath(step.getId());
  }

  private Item convertJobToItem(Job job) {
    Map<String, Object> jobItemData = job.toMap(Static.class);
    jobItemData.put("keepUntil", job.getKeepUntil() / 1000);
    jobItemData.put("state", job.getStatus().getState().toString());
    return Item.fromMap(jobItemData);
  }

  @Override
  public Future<Void> deleteJob(String jobId) {
    return dynamoClient.executeQueryAsync(() -> {
      jobTable.deleteItem(new DeleteItemSpec().withPrimaryKey("id", jobId));
      return null;
    });
  }

  @Override
  public Future<Void> init() {
    if (dynamoClient.isLocal()) {
      logger.info("DynamoDB running locally, initializing Jobs table.");
      try {
        List<IndexDefinition> indexes = List.of(
            new IndexDefinition("resourceKey"),
            new IndexDefinition("state"),
            new IndexDefinition("secondaryResourceKey")
        );
        dynamoClient.createTable(jobTable.getTableName(), "id:S,resourceKey:S,secondaryResourceKey:S,state:S", "id", indexes, "keepUntil");
        //TODO: Register a dynamo stream (in local dynamodb) to ensure we're getting informed when a job expires
      }
      catch (Exception e) {
        logger.error("Failure during creating table on " + getClass().getSimpleName() + "#init()", e);
        return Future.failedFuture(e);
      }
    }
    return Future.succeededFuture();
  }
}
