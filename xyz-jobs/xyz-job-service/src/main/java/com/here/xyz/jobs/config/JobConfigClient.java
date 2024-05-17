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
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.service.JobService;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.util.di.ImplementationProvider;
import com.here.xyz.util.service.Initializable;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.List;

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
   * Load all jobs with a specified type.
   * @param state
   * @return
   */
  public abstract Future<List<Job>> loadJobs(State state);

  /**
   * Load all jobs related to a specified resourceKey (e.g., space ID).
   * @param resourceKey
   * @return
   */
  public abstract Future<List<Job>> loadJobs(String resourceKey);

  public abstract Future<Void> storeJob(Job job);

  /**
   * Atomically updates the state of a job by ensuring the expectedPreviousState is matching just before writing.
   * @param job
   * @param expectedPreviousState
   * @return
   */
  public abstract Future<Void> updateState(Job job, State expectedPreviousState);

  public abstract Future<Void> updateStatus(Job job, State expectedPreviousState);

  public abstract Future<Void> updateStep(Job job, Step<?> newStep);

  public abstract Future<Void> deleteJob(String jobId);


  /*
  TODO: Provide a more generic variant of the method #loadJobs ...
  E.g.:
  public abstract Future<List<Job>> loadJobs(State[] state, long completedBefore);
   */

  public Future<List<Job>> loadFailedAndSucceededJobsOlderThan(Integer olderThanInMs) {
    List<Job> jobList = new ArrayList<>();

    return loadJobs(State.SUCCEEDED)
            .compose(succeedJobs -> { jobList.addAll(succeedJobs); return loadJobs(State.FAILED);})
            .compose(failedJobs -> Future.succeededFuture(failedJobs.stream().filter(job -> !job.isResumable()).toList()))
            .compose(notResumableFailedJobs -> { jobList.addAll(notResumableFailedJobs); return Future.succeededFuture(jobList);})
            .compose(result -> {
                      if (olderThanInMs != null)
                        /** Filter all results which are older than provided timestamp */
                        return Future.succeededFuture(
                                result.stream().filter(job -> (JobService.currentTimeMillis() - job.getCreatedAt()) > olderThanInMs).toList()
                        );
                      return Future.succeededFuture(result);
                    }
            );
  }
}
