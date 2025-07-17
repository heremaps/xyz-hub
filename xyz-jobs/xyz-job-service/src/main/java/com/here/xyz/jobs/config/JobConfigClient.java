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

import static com.here.xyz.jobs.RuntimeInfo.State.FAILED;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.service.JobService;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.util.di.ImplementationProvider;
import com.here.xyz.util.service.Initializable;
import io.vertx.core.Future;
import java.util.List;
import java.util.Set;

public abstract class JobConfigClient implements Initializable {

  public static JobConfigClient getInstance() {
    return Provider.provideInstance();
  }

  public static abstract class Provider implements ImplementationProvider {
    private static JobConfigClient client;
    public Provider() {}
    protected abstract JobConfigClient getInstance();
    private static JobConfigClient provideInstance() {
      if (client == null)
        client = ImplementationProvider.loadProvider(Provider.class).getInstance();
      return client;
    }
  }

  public abstract Future<Job> loadJob(String jobId);

  /**
   * Load all jobs.
   * @return A list of all jobs
   */
  public abstract Future<List<Job>> loadJobs();

  /**
   * Loads a list of {@link Job} entities filtered by the provided criteria.
   * <p>
   * Each filter uses {@link FilteredValues}, which defines a set of values and whether to include or exclude matches.
   * If a filter is {@code null} or has an empty value set, it is ignored.
   *
   * @param newerThan    Filter on the job creation timestamp. If {@code include} is true, selects jobs with {@code createdAt > value};
   *                     otherwise, selects jobs with {@code createdAt <= value}. Only the first value in the set is used.
   * @param sourceTypes  Filters jobs by source type class name. If {@code include} is true, matches any source type in the set;
   *                     if false, excludes those source types.
   * @param targetTypes  Filters jobs by target type class name. Works the same way as {@code sourceTypes}.
   * @param processTypes Filters jobs by process type class name. Works the same way as {@code sourceTypes}.
   * @param resourceKeys Filters jobs by resource keys. If {@code include} is true, includes jobs containing any of the keys;
   *                     if false, excludes jobs containing all of the keys.
   * @param stateTypes   Filters jobs by their {@link State}. If {@code include} is true, includes jobs with any of the given states;
   *                     otherwise, excludes them.
   * @return a {@link Future} containing the filtered list of jobs
   */
  public abstract Future<List<Job>> loadJobs(FilteredValues<Long> newerThan, FilteredValues<String> sourceTypes, FilteredValues<String> targetTypes,
                                               FilteredValues<String> processTypes, FilteredValues<String> resourceKeys, FilteredValues<State> stateTypes);

  /**
   * Load all jobs that are having the specified state.
   * @param state
   * @return
   */
  public abstract Future<List<Job>> loadJobs(State state);

  /**
   * Load all jobs related to a specified resourceKey (e.g., space ID).
   * @param resourceKey
   * @return
   */
  public abstract Future<Set<Job>> loadJobs(String resourceKey);

  /**
   * Load all jobs related to a specified resourceKey (e.g., space ID) that are having the specified state.
   * @param resourceKey
   * @return
   */
  public abstract Future<List<Job>> loadJobs(String resourceKey, State state);

  /**
   * Load all jobs related to any of the specified resource keys (e.g., space ID) that are having the specified state.
   * @param resourceKeys A set of resource keys for which to find the jobs
   * @param state A state by which to filter the resulting jobs
   * @return
   */
  public abstract Future<Set<Job>> loadJobs(Set<String> resourceKeys, State state);

  public abstract Future<Void> storeJob(Job job);

  /**
   * Atomically updates the state of a job by ensuring the expectedPreviousState is matching just before writing.
   * @param job
   * @param expectedPreviousState
   * @return
   */
  public abstract Future<Void> updateState(Job job, State expectedPreviousState);

  /**
   * Updates only the status of the specified job atomically, by ensuring a specific previous state
   * @param job The job of which to update the status object
   * @param expectedPreviousState The previous state of the job's status or null if the previous state should not be checked
   * @return A future which succeeds if the update was performed and fails if there was an error or the previous state was not matching
   */
  public abstract Future<Void> updateStatus(Job job, State expectedPreviousState);

  /**
   * Updates only one step of the specified job without overwriting the whole job configuration.
   * @param job The job of which to update one step
   * @param newStep The step to be overwritten on the job
   * @return
   */
  public abstract Future<Void> updateStep(Job job, Step<?> newStep);

  public abstract Future<Void> deleteJob(String jobId);


  /*
  TODO: Provide a more generic variant of the method #loadJobs ...
  E.g.:
  public abstract Future<List<Job>> loadJobs(List<State> states, long completedBefore);
   */

  @Deprecated
  public Future<List<Job>> loadNonResumableFailedJobsOlderThan(int olderThanInMs) {
    return loadJobs(FAILED)
        .map(failedJobs -> failedJobs.stream()
            //Keep only jobs that are non-resumable
            .filter(job -> !job.isResumable())
            //Keep only jobs that are older than provided timestamp
            .filter(job -> (JobService.currentTimeMillis() - job.getCreatedAt()) > olderThanInMs)
            .toList());
  }

  public record FilteredValues<T>(boolean include, Set<T> values){ }
}
