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

  @Override
  public Future<List<Job>> loadJobs(boolean newerThan, long createdAt) {
    return Future.succeededFuture(jobMap.values().stream()
        .filter(job -> newerThan ? job.getCreatedAt() > createdAt : job.getCreatedAt() < createdAt).toList());
  }

  @Override
  public Future<List<Job>> loadJobs(RuntimeInfo.State state) {
    List<Job> jobs = jobMap.values().stream()
        .filter(job -> job.getStatus().getState() == state)
        .collect(Collectors.toList());
    return Future.succeededFuture(jobs);
  }

  @Override
  public Future<Set<Job>> loadJobs(String resourceKey) {
    Set<Job> jobs = jobMap.values().stream()
        .filter(job -> resourceKey.equals(job.getResourceKey()))
        .collect(Collectors.toSet());
    return Future.succeededFuture(jobs);
  }

  @Override
  public Future<List<Job>> loadJobs(String resourceKey, State state) {
    return loadJobs().map(jobs -> jobs.stream().filter(job -> (resourceKey == null || job.getResourceKey().equals(resourceKey))
        && (state == null || job.getStatus().getState() == state)).toList());
  }

  @Override
  public Future<List<Job>> loadJobs(String resourceKey, String secondaryResourceKey, State state) {
    if(secondaryResourceKey == null)
      return loadJobs(resourceKey, resourceKey, state);
    return loadJobs().map(jobs ->
            jobs.stream()
                    .filter(job ->
                            (resourceKey == null || job.getResourceKey().equals(resourceKey)
                              || (secondaryResourceKey != null && job.getResourceKey().equals(secondaryResourceKey)))
                            && (state == null || job.getStatus().getState() == state)
                    )
                    .toList()
    );
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
