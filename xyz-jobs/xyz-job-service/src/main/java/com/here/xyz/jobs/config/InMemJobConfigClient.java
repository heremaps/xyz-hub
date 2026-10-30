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

import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.RuntimeStatus;
import com.here.xyz.jobs.steps.Step;
import io.vertx.core.Future;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InMemJobConfigClient extends JobConfigClient {
  private Map<String, Job> jobMap = new ConcurrentHashMap<>();

  public static class Provider extends JobConfigClient.Provider {
    @Override
    public boolean chooseMe() {
      return "test".equals(System.getProperty("scope"));
    }

    @Override
    protected JobConfigClient getInstance() {
      return new InMemJobConfigClient();
    }
  }

  /**
   * Like a real storage, this client stores and returns copies of the jobs.
   * That way, the conditions of the conditional writes are checked against the stored state rather than the caller's instance.
   */
  private static Job copy(Job job) {
    return job == null ? null : XyzSerializable.fromMap(job.toMap(Static.class), Job.class);
  }

  private Stream<Job> storedJobs() {
    return jobMap.values().stream().map(InMemJobConfigClient::copy);
  }

  @Override
  public Future<Job> loadJob(String jobId) {
    return Future.succeededFuture(copy(jobMap.get(jobId)));
  }

  @Override
  public Future<List<Job>> loadJobs() {
    return Future.succeededFuture(storedJobs().toList());
  }

  public Future<List<Job>> loadJobs(FilteredValues<Long> newerThan, FilteredValues<Long> olderThan, FilteredValues<String> sourceTypes,
      FilteredValues<String> targetTypes, FilteredValues<String> processTypes, FilteredValues<String> resourceKeys,
      FilteredValues<State> stateTypes) {
    return Future.succeededFuture(
        storedJobs()
            .filter(job -> {
              if (newerThan == null || newerThan.values().isEmpty())
                return true;

              Long ts = newerThan.values().iterator().next();
              return newerThan.include() ? job.getCreatedAt() > ts : job.getCreatedAt() <= ts;
            })

            .filter(job -> {
              if (olderThan == null || olderThan.values().isEmpty())
                return true;

              Long ts = olderThan.values().iterator().next();
              return olderThan.include() ? job.getCreatedAt() < ts : job.getCreatedAt() >= ts;
            })

            .filter(job -> matchesFilteredValues(sourceTypes,
                job.getSource() != null ? job.getSource().getClass().getSimpleName() : null))

            .filter(job -> matchesFilteredValues(targetTypes,
                job.getTarget() != null ? job.getTarget().getClass().getSimpleName() : null))

            .filter(job -> matchesFilteredValues(processTypes,
                job.getProcess() != null ? job.getProcess().getClass().getSimpleName() : null))

            .filter(job -> matchesFilteredValues(stateTypes,
                job.getStatus() != null ? State.valueOf(job.getStatus().getState().name()) : null))

            .filter(job -> {
              if (resourceKeys == null || resourceKeys.values().isEmpty())
                return true;

              Set<String> jobKeys = job.getResourceKeys();
              if (jobKeys == null || jobKeys.isEmpty())
                return !resourceKeys.include(); // Exclude when filtering for presence

              boolean match = resourceKeys.values().stream().anyMatch(jobKeys::contains);
              return resourceKeys.include() ? match : !match;
            })
            .toList()
    );
  }

  public Future<List<Job>> loadJobs(
          boolean newerThan, long createdAt, String sourceType,
          String targetType, String processType, String resourceKey, State state
  ) {
    return Future.succeededFuture(storedJobs()
            .filter(job -> newerThan ? job.getCreatedAt() > createdAt : job.getCreatedAt() < createdAt)
            .filter(job -> processType == null || processType.isEmpty() ||
                    (job.getProcess() != null && processType.equals(job.getProcess().getClass().getSimpleName())))
            .filter(job -> sourceType == null || sourceType.isEmpty() ||
                    (job.getSource() != null && sourceType.equals(job.getSource().getClass().getSimpleName())))
            .filter(job -> targetType == null || targetType.isEmpty() ||
                    (job.getTarget() != null && targetType.equals(job.getTarget().getClass().getSimpleName())))
            .filter(job -> state == null &&
                    (job.getStatus() != null && state.equals(job.getStatus().getState().name())))
            .filter(job -> resourceKey == null || resourceKey.isEmpty() ||
                    (job.getResourceKeys() != null && job.getResourceKeys().contains(resourceKey)))
            .toList()
    );
  }

  @Override
  public Future<List<Job>> loadJobs(RuntimeInfo.State state) {
    List<Job> jobs = storedJobs()
        .filter(job -> job.getStatus().getState() == state)
        .collect(Collectors.toList());
    return Future.succeededFuture(jobs);
  }

  @Override
  public Future<Set<Job>> loadJobsByPrimaryResourceKey(String resourceKey) {
    if (resourceKey == null)
      return Future.succeededFuture(Set.of());
    return loadJobs().map(jobs -> jobs.stream()
            .filter(job -> resourceKey.equals(job.getResourceKey()))
            .collect(Collectors.toSet()));
  }

  @Override
  public Future<List<Job>> loadJobs(String resourceKey, State state) {
    return loadJobs(Set.of(resourceKey)).map(jobs -> jobs.stream()
        .filter(job -> state == null || job.getStatus().getState() == state)
        .toList());
  }

  private Future<List<Job>> loadJobs(Set<String> resourceKeys) {
    return loadJobs().map(jobs -> jobs.stream()
        .filter(job -> resourceKeys.stream().anyMatch(resourceKey -> job.getResourceKeys().contains(resourceKey)))
        .toList());
  }

  @Override
  public Future<Set<Job>> loadJobs(Set<String> resourceKeys, State state) {
    return loadJobs().map(jobs ->
        jobs.stream()
            .filter(job -> resourceKeys.stream().anyMatch(resourceKey -> job.getResourceKeys().contains(resourceKey))
                && (state == null || job.getStatus().getState() == state))
            .collect(Collectors.toSet()));
  }

  //NOTE: All writes are synchronized and replace the stored instance by an updated copy, so that the stored instances are never modified

  @Override
  public synchronized Future<Void> storeJob(Job job) {
    jobMap.put(job.getId(), copy(job));
    return Future.succeededFuture();
  }

  @Override
  public synchronized Future<Void> updateState(Job job, State expectedPreviousState) {
    if (expectedPreviousState == job.getStatus().getState())
      return Future.succeededFuture();

    Job storedJob = copy(jobMap.get(job.getId()));
    if (storedJob == null || storedJob.getStatus().getState() != expectedPreviousState)
      return Future.failedFuture(stateConflict(job, expectedPreviousState));

    Map<String, Object> status = storedJob.getStatus().toMap();
    status.put("state", job.getStatus().getState().toString());
    jobMap.put(job.getId(), storedJob.withStatus(XyzSerializable.fromMap(status, RuntimeStatus.class)));
    return Future.succeededFuture();
  }

  @Override
  public synchronized Future<Void> updateStatus(Job job, State expectedPreviousState) {
    Job storedJob = copy(jobMap.get(job.getId()));
    if (expectedPreviousState != null && (storedJob == null || storedJob.getStatus().getState() != expectedPreviousState))
      return Future.failedFuture(stateConflict(job, expectedPreviousState));

    RuntimeStatus status = XyzSerializable.fromMap(job.getStatus().toMap(), RuntimeStatus.class);
    jobMap.put(job.getId(), storedJob == null ? copy(job) : storedJob.withStatus(status));
    return Future.succeededFuture();
  }

  private static ConditionalCheckFailedException stateConflict(Job job, State expectedPreviousState) {
    return new ConditionalCheckFailedException("The state of job " + job.getId() + " is not " + expectedPreviousState);
  }

  @Override
  public synchronized Future<Void> updateStep(Job job, Step<?> newStep) {
    Job storedJob = copy(jobMap.get(job.getId()));
    Step<?> storedStep = storedJob == null || storedJob.getSteps() == null ? null : storedJob.getStepById(newStep.getId());
    //Like the DynamoJobConfigClient, do not overwrite a step that is already in a final state
    if (storedStep == null || storedStep.getStatus().getState().isFinal())
      return Future.succeededFuture();

    storedJob.getSteps().replaceStep(XyzSerializable.fromMap(newStep.toMap(Static.class), Step.class));
    jobMap.put(job.getId(), storedJob);
    return Future.succeededFuture();
  }

  @Override
  public Future<Step> loadStep(String jobId, String stepId) {
    Job job = copy(jobMap.get(jobId));
    return Future.succeededFuture(job == null ? null : job.getStepById(stepId));
  }

  @Override
  public Future<Job> hydrateJob(Map<String, Object> jobItem) {
    return Future.succeededFuture(XyzSerializable.fromMap(jobItem, Job.class));
  }

  @Override
  public Future<Void> deleteJob(String jobId) {
    jobMap.remove(jobId);
    return Future.succeededFuture();
  }

  @Override
  public Future<Void> deleteStepConfigs(String jobId) {
    return Future.succeededFuture();
  }
}
