/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.util.ARN;
import com.here.xyz.util.service.aws.dynamo.DynamoClient;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Client for storing and loading step configs in DynamoDB.
 */
public class StepConfigClient {

  private final Table stepTable;
  private final DynamoClient dynamoClient;

  public StepConfigClient(String tableArn) {
    dynamoClient = new DynamoClient(tableArn, null);
    stepTable = dynamoClient.db.getTable(new ARN(tableArn).getResourceWithoutType());
  }

  /**
   * Stores all given steps of a job, one item per step (batched).
   *
   * @param steps     the steps to persist; a {@code null}/empty list is a no-op
   * @param keepUntil the item TTL in epoch seconds (same as the owning job, so orphans self-expire as a backstop)
   */
  public void storeSteps(List<Step<?>> steps, long keepUntil) {
    if (steps == null || steps.isEmpty()) {
      return;
    }
    List<WriteRequest> writeRequests = steps.stream()
        .map(step -> new WriteRequest(new PutRequest(ItemUtils.toAttributeValues(toItem(step, keepUntil)))))
        .toList();
    dynamoClient.batchWrite(stepTable.getTableName(), writeRequests);
  }

  /**
   * Upserts a single step config item in the step-config table.
   *
   * @param step      the step to persist (its {@code jobId} + {@code id} form the item key)
   * @param keepUntil the item TTL in epoch seconds
   */
  public void storeStep(Step<?> step, long keepUntil) {
    final List<State> finalStates = Stream.of(State.values()).filter(State::isFinal).toList();

    Map<String, Object> valueMap = new HashMap<>();
    valueMap.put(":step", step.toMap(Static.class));
    valueMap.put(":state", stateOf(step));
    valueMap.put(":keepUntil", keepUntil);
    finalStates.forEach(state -> valueMap.put(":" + state, state.toString()));

    stepTable.updateItem(new UpdateItemSpec()
        .withPrimaryKey("jobId", step.getJobId(), "id", step.getId())
        .withUpdateExpression("SET #step = :step, #state = :state, keepUntil = :keepUntil")
        //Do not overwrite a step that is already in a final state.
        .withConditionExpression("attribute_not_exists(#state) OR (" + finalStates.stream()
            .map(state -> "#state <> :" + state)
            .collect(Collectors.joining(" AND ")) + ")")
        .withNameMap(Map.of("#step", "step", "#state", "state"))
        .withValueMap(valueMap));
  }

  /**
   * Loads all step configs of the given job in a single {@code query} on the partition key {@code jobId}. Used by
   * {@code DynamoJobConfigClient} to re-hydrate a job's step graph after reading the lean job item.
   *
   * @param jobId the job whose steps to load
   * @return the job's step configs (empty if none)
   */
  public List<StepConfig> loadStepsForJob(String jobId) {
    List<StepConfig> steps = new LinkedList<>();
    stepTable.query("jobId", jobId)
        .pages()
        .forEach(page -> page.forEach(item -> steps.add(XyzSerializable.fromMap(item.asMap(), StepConfig.class))));
    return steps;
  }

  /**
   * Deletes all step configs of the given job (queries the step table by {@code jobId}, then batch-deletes).
   *
   * @param jobId the job whose steps to delete
   */
  public void deleteSteps(String jobId) {
    List<String> stepIds = new ArrayList<>();
    stepTable.query("jobId", jobId)
        .pages()
        .forEach(page -> page.forEach(item -> stepIds.add(item.getString("id"))));
    deleteSteps(jobId, stepIds);
  }

  /**
   * Deletes the given steps of a job (batched).
   *
   * @param jobId   the owning job id
   * @param stepIds the step ids to delete; a {@code null}/empty list is a no-op
   */
  public void deleteSteps(String jobId, List<String> stepIds) {
    if (stepIds == null || stepIds.isEmpty()) {
      return;
    }
    List<WriteRequest> writeRequests = stepIds.stream()
        .map(stepId -> new WriteRequest(new DeleteRequest(Map.of(
            "jobId", new AttributeValue().withS(jobId),
            "id", new AttributeValue().withS(stepId)))))
        .toList();
    dynamoClient.batchWrite(stepTable.getTableName(), writeRequests);
  }

  /**
   * Creates the step-config table when running against a local DynamoDB.
   */
  public void initLocalTable() {
    if (dynamoClient.isLocal()) {
      dynamoClient.createTable(stepTable.getTableName(), "jobId:S,id:S", "jobId,id", List.of(), null);
    }
  }

  private static String stateOf(Step<?> step) {
    return step.getStatus() == null ? State.NONE.toString() : step.getStatus().getState().toString();
  }

  private Item toItem(Step<?> step, long keepUntil) {
    return Item.fromMap(new StepConfig()
        .withJobId(step.getJobId())
        .withId(step.getId())
        .withStep(step.toMap(Static.class))
        .withState(stateOf(step))
        .withKeepUntil(keepUntil)
        .toMap());
  }

  /**
   * POJO mirroring one step-config item ({@code jobId}, {@code id}, the step config {@code step} map, its {@code state} and
   * {@code keepUntil} TTL).
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class StepConfig implements XyzSerializable {

    @JsonProperty
    private String jobId;
    @JsonProperty
    private String id;
    @JsonProperty
    private Map<String, Object> step;
    @JsonProperty
    private String state;
    @JsonProperty
    private long keepUntil;

    public String getJobId() {
      return jobId;
    }

    public void setJobId(String jobId) {
      this.jobId = jobId;
    }

    public StepConfig withJobId(String jobId) {
      setJobId(jobId);
      return this;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public StepConfig withId(String id) {
      setId(id);
      return this;
    }

    public Map<String, Object> getStep() {
      return step;
    }

    public void setStep(Map<String, Object> step) {
      this.step = step;
    }

    public StepConfig withStep(Map<String, Object> step) {
      setStep(step);
      return this;
    }

    public String getState() {
      return state;
    }

    public void setState(String state) {
      this.state = state;
    }

    public StepConfig withState(String state) {
      setState(state);
      return this;
    }

    public long getKeepUntil() {
      return keepUntil;
    }

    public void setKeepUntil(long keepUntil) {
      this.keepUntil = keepUntil;
    }

    public StepConfig withKeepUntil(long keepUntil) {
      setKeepUntil(keepUntil);
      return this;
    }
  }
}
