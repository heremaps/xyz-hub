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

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.steps.Step;
import io.vertx.core.Future;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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

  @Override
  public Future<Job> loadJob(String jobId) {
    return Future.succeededFuture(jobMap.get(jobId));
  }

  @Override
  public Future<List<Job>> loadJobs() {
    return Future.succeededFuture(List.copyOf(jobMap.values()));
  }

  public Future<List<Job>> loadJobs(FilteredValues<Long> newerThan, FilteredValues<String> sourceTypes,
                                    FilteredValues<String> targetTypes, FilteredValues<String> processTypes,
                                    FilteredValues<String> resourceKeys, FilteredValues<State> stateTypes) {
    return Future.succeededFuture(
            jobMap.values().stream()
                    .filter(job -> {
                      if (newerThan == null || newerThan.values().isEmpty()) return true;
                      Long ts = newerThan.values().iterator().next();
                      return newerThan.include() ? job.getCreatedAt() > ts : job.getCreatedAt() < ts;
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
                      if (resourceKeys == null || resourceKeys.values().isEmpty()) return true;
                      Set<String> jobKeys = job.getResourceKeys();
                      if (jobKeys == null || jobKeys.isEmpty()) return !resourceKeys.include(); // Exclude when filtering for presence
                      boolean match = resourceKeys.values().stream().anyMatch(jobKeys::contains);
                      return resourceKeys.include() ? match : !match;
                    })

                    .toList()
    );
  }

  private static <T> boolean matchesFilteredValues(FilteredValues<T> filter, T actualValue) {
    if (filter == null || filter.values().isEmpty()) return true;
    boolean match = filter.values().contains(actualValue);
    return filter.include() ? match : !match;
  }

  public Future<List<Job>> loadJobs(
          boolean newerThan, long createdAt, String sourceType,
          String targetType, String processType, String resourceKey, State state
  ) {
    return Future.succeededFuture(jobMap.values().stream()
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
    List<Job> jobs = jobMap.values().stream()
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

  @Override
  public Future<Void> storeJob(Job job) {
    jobMap.put(job.getId(), job);
    return Future.succeededFuture();
  }

  @Override
  public Future<Void> updateState(Job job, State expectedPreviousState) {
    return storeJob(job);
  }

  @Override
  public Future<Void> updateStatus(Job job, State expectedPreviousState) {
    return storeJob(job);
  }

  @Override
  public Future<Void> updateStep(Job job, Step<?> newStep) {
    return storeJob(job);
  }

  @Override
  public Future<Void> deleteJob(String jobId) {
    jobMap.remove(jobId);
    return Future.succeededFuture();
  }
}
