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

package com.here.xyz.jobs.config;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.collect.Lists;
import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.service.Config;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.util.ARN;
import com.here.xyz.util.service.aws.dynamo.DynamoClient;
import com.here.xyz.util.service.aws.dynamo.IndexDefinition;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DynamoJobConfigClient extends JobConfigClient {

  private static final Logger logger = LogManager.getLogger();
  public static final int MAX_RESOURCE_KEYS = 256;
  public static final IndexDefinition JOB_ID_GSI = new IndexDefinition("jobId");
  public static final IndexDefinition STATE_GSI = new IndexDefinition("state");
  public static final IndexDefinition RESOURCE_KEY_GSI = new IndexDefinition("resourceKey");
  public static final IndexDefinition SECONDARY_RESOURCE_KEY_GSI = new IndexDefinition("secondaryResourceKey");
  private final Table jobTable;
  private final Table resourceKeyTable;
  private final DynamoClient dynamoClient;

  public DynamoJobConfigClient(String tableArn, String resourceKeysTableArn) {
    dynamoClient = new DynamoClient(tableArn, null);
    logger.debug("Instantiating a reference to Dynamo Table {}", dynamoClient.tableName);
    jobTable = dynamoClient.db.getTable(dynamoClient.tableName);
    resourceKeyTable = dynamoClient.db.getTable(new ARN(resourceKeysTableArn).getResourceWithoutType());
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
      return new DynamoJobConfigClient(Config.instance.JOBS_DYNAMODB_TABLE_ARN, Config.instance.RESOURCE_KEYS_DYNAMODB_TABLE_ARN);
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
  public Future<List<Job>> loadJobs(FilteredValues<Long> newerThan, FilteredValues<String> sourceTypes,
                                    FilteredValues<String> targetTypes, FilteredValues<String> processTypes,
                                    FilteredValues<String> resourceKeys, FilteredValues<State> stateTypes) {
    List<Job> jobs = new LinkedList<>();

    List<String> filters = new ArrayList<>();
    Map<String, String> attrNames = new HashMap<>();
    Map<String, Object> attrValues = new HashMap<>();

    // --- createdAt special case ---
    if (newerThan != null && !newerThan.values().isEmpty()) {
      Long ts = newerThan.values().iterator().next(); // Only use one timestamp
      filters.add("#createdAt " + (newerThan.include() ? ">" : "<=") + " :ts");
      attrNames.put("#createdAt", "createdAt");
      attrValues.put(":ts", ts);
    }

    // --- IN / NOT IN filters ---
    Optional.ofNullable(buildInFilter("source.type", sourceTypes, attrNames, attrValues)).ifPresent(filters::add);
    Optional.ofNullable(buildInFilter("target.type", targetTypes, attrNames, attrValues)).ifPresent(filters::add);
    Optional.ofNullable(buildInFilter("process.type", processTypes, attrNames, attrValues)).ifPresent(filters::add);
    Optional.ofNullable(buildInFilter("status.state", stateTypes, attrNames, attrValues)).ifPresent(filters::add);

    // --- resourceKeys: contains / NOT contains ---
    if (resourceKeys != null && !resourceKeys.values().isEmpty()) {
      List<String> subFilters = new ArrayList<>();
      int i = 0;

      for (String key : resourceKeys.values()) {
        String paramKey = ":rk" + i++;
        attrValues.put(paramKey, key);
        subFilters.add("contains(#resourceKeys, " + paramKey + ")");
      }

      attrNames.put("#resourceKeys", "resourceKeys");

      if (resourceKeys.include()) {
        filters.add("(" + String.join(" OR ", subFilters) + ")");
      } else {
        filters.add("(" + subFilters.stream().map(f -> "NOT " + f).collect(Collectors.joining(" AND ")) + ")");
      }
    }

    String filterExpr = filters.isEmpty() ? null : String.join(" AND ", filters);
    if (attrNames.isEmpty()) attrNames = null;
    if (attrValues.isEmpty()) attrValues = null;

    jobTable.scan(filterExpr, attrNames, attrValues)
            .pages()
            .forEach(page ->
                    page.forEach(item -> jobs.add(XyzSerializable.fromMap(item.asMap(), Job.class)))
            );

    return Future.succeededFuture(jobs);
  }

  private String buildInFilter(String fieldPath, FilteredValues<?> fv,
                               Map<String, String> attrNames, Map<String, Object> attrValues) {
    if (fv == null || fv.values().isEmpty()) return null;

    String[] parts = fieldPath.split("\\.");
    StringBuilder fieldExpr = new StringBuilder();
    for (String part : parts) {
      String key = "#" + part;
      fieldExpr.append(key).append(".");
      attrNames.put(key, part);
    }
    fieldExpr.setLength(fieldExpr.length() - 1); // Remove trailing dot
    String fieldRef = fieldExpr.toString();

    List<String> conditions = new ArrayList<>();
    int i = 0;
    for (Object val : fv.values()) {
      String paramKey = ":" + parts[parts.length - 1] + i++;
      attrValues.put(paramKey, val instanceof Enum<?> e ? e.name() : val);

      if (fv.include()) {
        conditions.add(paramKey);
      } else {
        conditions.add(fieldRef + " <> " + paramKey);
      }
    }

    return fv.include()
            ? fieldRef + " IN (" + String.join(", ", conditions) + ")"
            : String.join(" AND ", conditions);
  }

  public Future<List<Job>> loadJobs(
          boolean newerThan,
          long createdAt,
          String sourceType,
          String targetType,
          String processType,
          String resourceKey,
          State state
  ) {
    //TODO: Use indexes instead of scan()
    return dynamoClient.executeQueryAsync(() -> {
      List<Job> jobs = new LinkedList<>();

      List<String> filters = new ArrayList<>();
      Map<String, String> attrNames = new HashMap<>();
      Map<String, Object> attrValues = new HashMap<>();

      if (createdAt != 0) {
        filters.add( "#createdAt " + (newerThan ? ">" : "<") + " :ts");
        attrNames.put("#createdAt", "createdAt");
        attrValues.put(":ts", createdAt);
      }

      if (processType != null && !processType.isEmpty()) {
        filters.add("#process.#type = :processType");
        attrNames.put("#process", "process");
        attrNames.put("#type", "type");
        attrValues.put(":processType", processType);
      }

      if (sourceType != null && !sourceType.isEmpty()) {
        filters.add("#source.#type = :sourceType");
        attrNames.put("#source", "source");
        attrNames.put("#type", "type");
        attrValues.put(":sourceType", sourceType);
      }

      if (targetType != null && !targetType.isEmpty()) {
        filters.add("#target.#type = :targetType");
        attrNames.put("#target", "target");
        attrNames.put("#type", "type");
        attrValues.put(":targetType", targetType);
      }

      if (state != null) {
        filters.add("#status.#state = :state");
        attrNames.put("#status", "status");
        attrNames.put("#state", "state");
        attrValues.put(":state", state.name());
      }

      if (resourceKey != null && !resourceKey.isEmpty()) {
        filters.add("contains(#resourceKeys, :resourceKey)");
        attrNames.put("#resourceKeys", "resourceKeys");
        attrValues.put(":resourceKey", resourceKey);
      }

      String filterExpr = String.join(" AND ", filters);

      jobTable
              .scan(filterExpr, attrNames, attrValues)
              .pages()
              .forEach(page ->
                      page.forEach(item -> jobs.add(XyzSerializable.fromMap(item.asMap(), Job.class)))
              );

      return jobs;
    });
  }

  @Override
  public Future<List<Job>> loadJobs(State state) {
    return dynamoClient.executeQueryAsync(() -> queryIndex(jobTable, STATE_GSI, state.toString())
        .stream()
        .map(jobItem -> XyzSerializable.fromMap(jobItem.asMap(), Job.class))
        .toList());
  }

  @Override
  public Future<Set<Job>> loadJobs(String resourceKey) {
    if (resourceKey == null)
      return Future.succeededFuture(Set.of());
    return loadJobs(Set.of(resourceKey));
  }

  @Override
  public Future<List<Job>> loadJobs(String resourceKey, State state) {
    if (resourceKey != null)
      return loadJobs(Set.of(resourceKey), state).map(jobs -> jobs.stream().toList());
    else if (state != null)
      return loadJobs(state);
    else
      return loadJobs();
  }

  @Override
  public Future<Set<Job>> loadJobs(Set<String> resourceKeys, State state) {
    return loadJobs(resourceKeys)
        .map(jobs -> jobs.stream()
            .filter(job -> state == null || job.getStatus().getState() == state)
            .collect(Collectors.toSet()));
  }

  //TODO: Remove resourceKey GSIs after migration

  private Future<Set<Job>> loadJobs(Set<String> resourceKeys) {
    return findJobIds(resourceKeys).compose(jobIds -> {

      //Do parallel batch request to dynamo in case more than 100 job IDs
      List<List<String>> batchedJobIds = Lists.partition(jobIds.stream().toList(), 100);
      List<Future<Set<Job>>> futureList = new ArrayList<>();
      for (List<String> batch : batchedJobIds) {
        futureList.add(batchLoadJobsById(batch));
      }

      return Future.all(futureList)
              .map(cf -> cf.list().stream()
                      .flatMap(set -> ((Set<Job>) set).stream())
                      .collect(Collectors.toSet()));
    });
  }

  private Future<Set<Job>> batchLoadJobsById(List<String> jobIds) {
    if(jobIds == null || jobIds.isEmpty())
      return Future.succeededFuture(Set.of());

    return dynamoClient.executeQueryAsync(() -> {
      List<Map<String, AttributeValue>> requestKeys = jobIds.stream()
          .map(jobId -> Map.of("id", new AttributeValue().withS(jobId)))
          .toList();
      BatchGetItemRequest request = new BatchGetItemRequest()
          .withRequestItems(Map.of(jobTable.getTableName(), new KeysAndAttributes().withKeys(requestKeys)));
      BatchGetItemResult result = dynamoClient.client.batchGetItem(request);

      if (!result.getUnprocessedKeys().isEmpty())
        //TODO: Need to implement retry / paging in this case?
        throw new RuntimeException("Error while batch loading jobs. Some Items were not processed. Requested IDs: "
            + String.join(", ", jobIds));

      List<Map<String, AttributeValue>> rawItems = result.getResponses().get(jobTable.getTableName());

      return ItemUtils.toItemList(rawItems).stream()
          .map(jobItem -> XyzSerializable.fromMap(jobItem.asMap(), Job.class))
          .collect(Collectors.toSet());
    });
  }

  /**
   * Returns the IDs of all jobs that are related to any of the specified resource keys.
   * @param resourceKeys The resource keys for which to find the job IDs
   * @return A set of job IDs
   */
  private Future<Set<String>> findJobIds(Set<String> resourceKeys) {
    //Run the queries for all resource keys in parallel
    List<Future<Set<String>>> bwcLoaded = resourceKeys.stream().map(resourceKey -> loadJobIdsByResourceKeyBWC(resourceKey)).toList();
    List<Future<Set<String>>> newStyle = resourceKeys.stream().map(resourceKey -> findJobIds(resourceKey)).toList();
    List<Future<Set<String>>> combined = new ArrayList<>();
    combined.addAll(bwcLoaded);
    combined.addAll(newStyle);

    return Future.all(combined)
        .map(cf -> cf.list().stream()
            .flatMap(set -> ((Set<String>) set).stream())
            .collect(Collectors.toSet()));
  }

  @Deprecated
  private Future<Set<String>> loadJobIdsByResourceKeyBWC(String resourceKey) {
    return dynamoClient.executeQueryAsync(() -> {
      Set<String> jobs = new HashSet<>();
      jobs.addAll(queryIndex(jobTable, RESOURCE_KEY_GSI, resourceKey).stream().map(item -> item.getString("id"))
          .collect(Collectors.toSet()));
      jobs.addAll(queryIndex(jobTable, SECONDARY_RESOURCE_KEY_GSI, resourceKey).stream().map(item -> item.getString("id"))
          .collect(Collectors.toSet()));
      return jobs;
    });
  }


  /**
   * Returns the IDs of all jobs that are related to the specified resource key.
   * @param resourceKey The resource key for which to find the job IDs
   * @return A set of job IDs
   */
  private Future<Set<String>> findJobIds(String resourceKey) {
    return dynamoClient.executeQueryAsync(() -> {
      Set<String> jobIds = new HashSet<>();
      resourceKeyTable.query("resourceKey", resourceKey)
          .pages()
          .forEach(page -> page.forEach(relationItem -> jobIds.add(relationItem.getString("jobId"))));
      return jobIds;
    });
  }

  @Override
  public Future<Void> storeJob(Job job) {
    return dynamoClient.executeQueryAsync(() -> {
      batchWriteResourceKeys(job.getId(), job.getResourceKeys(), job.getKeepUntil() / 1000);
      //TODO: Ensure that concurrent writes do not produce invalid state-transitions using atomic writes
      jobTable.putItem(convertJobToItem(job));
      return null;
    });
  }

  private void batchWriteResourceKeys(String jobId, Set<String> resourceKeys, long keepUntil) {
    if (resourceKeys.size() > MAX_RESOURCE_KEYS)
      throw new RuntimeException("Resource keys exceeds maximum allowed size of " + MAX_RESOURCE_KEYS);

    List<WriteRequest> writeRequests = resourceKeys.stream()
        .map(resourceKey -> new WriteRequest(new PutRequest(Map.of(
            "resourceKey", new AttributeValue().withS(resourceKey),
            "jobId", new AttributeValue().withS(jobId),
            "keepUntil", new AttributeValue().withN(Long.toString(keepUntil))
        ))))
        .toList();

    executeBatchWriteRequest(Map.of(resourceKeyTable.getTableName(), writeRequests));
  }

  private void executeBatchWriteRequest(Map<String, List<WriteRequest>> requestItems) {
    BatchWriteItemResult result = dynamoClient.client.batchWriteItem(new BatchWriteItemRequest().withRequestItems(requestItems));

    if (!result.getUnprocessedItems().isEmpty())
      executeBatchWriteRequest(result.getUnprocessedItems());
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
      deleteResourceKeys(jobId);
      return null;
    });
  }

  private void deleteResourceKeys(String jobId) {
    deleteResourceKeys(jobId, loadAllResourceKeys(jobId));
  }

  private Set<String> loadAllResourceKeys(String jobId) {
    return queryIndex(resourceKeyTable, JOB_ID_GSI, jobId)
        .stream()
        .map(resourceKeyItem -> resourceKeyItem.getString("resourceKey"))
        .collect(Collectors.toSet());
  }

  private void deleteResourceKeys(String jobId, Set<String> resourceKeys) {
    List<WriteRequest> writeRequests = resourceKeys.stream()
        .map(resourceKey -> new WriteRequest(new DeleteRequest(Map.of(
            "resourceKey", new AttributeValue().withS(resourceKey),
            "jobId", new AttributeValue().withS(jobId)
        ))))
        .toList();

    executeBatchWriteRequest(Map.of(resourceKeyTable.getTableName(), writeRequests));
  }

  private List<Item> queryIndex(Table table, IndexDefinition index, String hashKeyValue) {
    List<Item> items = new ArrayList<>();
    table.getIndex(index.getName())
        .query(index.getHashKey(), hashKeyValue)
        .pages()
        .forEach(page -> page.forEach(item -> items.add(item)));
    return items;
  }

  @Override
  public Future<Void> init() {
    if (dynamoClient.isLocal()) {
      logger.info("DynamoDB running locally, initializing Jobs table.");
      try {
        List<IndexDefinition> indexes = List.of(
            RESOURCE_KEY_GSI, //TODO: Remove
            STATE_GSI,
            SECONDARY_RESOURCE_KEY_GSI //TODO: Remove
        );
        dynamoClient.createTable(jobTable.getTableName(), "id:S,resourceKey:S,secondaryResourceKey:S,state:S", "id", indexes,
            "keepUntil");
        dynamoClient.createTable(resourceKeyTable.getTableName(), "resourceKey:S,jobId:S", "resourceKey,jobId",
            List.of(JOB_ID_GSI), "keepUntil");
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
