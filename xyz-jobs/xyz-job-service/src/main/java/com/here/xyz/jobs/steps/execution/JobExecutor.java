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

package com.here.xyz.jobs.steps.execution;

import static com.here.xyz.jobs.RuntimeInfo.State.PENDING;
import static com.here.xyz.jobs.RuntimeInfo.State.RUNNING;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.config.JobConfigClient;
import com.here.xyz.jobs.steps.StepGraph;
import io.vertx.core.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class JobExecutor {
  private static final Logger logger = LogManager.getLogger();
  private static final JobExecutor instance = new StateMachineExecutor(); //TODO: Use a different impl locally
  private static final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
  private static volatile boolean running;
  private static volatile boolean stopRequested;

  {
    exec.scheduleWithFixedDelay(this::checkPendingJobs, 10, 60, TimeUnit.SECONDS);
  }

  public static JobExecutor getInstance() {
    return instance;
  }

  public final Future<Void> startExecution(Job job, String formerExecutionId) {
    //TODO: Care about concurrency between nodes when it comes to resource-load calculation within this thread
    if (mayExecute(job)) {
      //Update the job status atomically, that now is RUNNING to make sure no other node will try to start it.
      job.getStatus().setState(RUNNING);
      //TODO: Update / invalidate the reserved unit maps?
      return job.store() //TODO: Make sure in JobConfigClient, that state updates are always atomic
          .compose(v -> formerExecutionId != null ? resume(job, formerExecutionId) : execute(job))
          //Execution was started successfully, store the execution ID.
          .compose(executionId -> job.withExecutionId(executionId).store());
    }
    else
      //The Job remains in PENDING state and will be checked be later again if it may be executed
      return Future.succeededFuture();
  }

  private void checkPendingJobs() {
    running = true;
    if (stopRequested)
      //Do not start an execution if a stop was requested
      return;

    try {
      JobConfigClient.getInstance().loadJobs(PENDING)
          .onSuccess(pendingJobs -> {
            for (Job pendingJob : pendingJobs) {
              if (stopRequested)
                return;
              //Try to start the execution of the pending job
              startExecution(pendingJob, pendingJob.getExecutionId());
              Thread.yield();
            }
          })
          .onFailure(t -> logger.error("Error checking PENDING jobs", t))
          .onComplete(ar -> running = false);
    }
    finally {
      running = false;
    }
  }

  protected abstract Future<String> execute(Job job);

  protected abstract Future<String> execute(StepGraph formerGraph, Job job);

  protected abstract Future<String> resume(Job job, String executionId);

  public abstract Future<Boolean> cancel(String executionId);

  /**
   * Checks for all resource-loads of the specified job whether they can be fulfilled. If yes, the job may be executed.
   *
   * @param job The job to be checked
   * @return true, if the job may be executed / enough resources are free
   */
  private boolean mayExecute(Job job) {
    //Check for all needed resource-loads whether they can be fulfilled
    return job.calculateResourceLoads().stream().allMatch(load -> load.getEstimatedVirtualUnits() < load.getResource().getFreeVirtualUnits());
  }

  public static void shutdown() throws InterruptedException {
    stopRequested = true;
    exec.shutdownNow();
    //Make sure to defer system shutdown until a potential run of #checkPendingJobs() is completed
    while (running)
      Thread.sleep(100);
  }

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        shutdown();
      }
      catch (InterruptedException e) {
        //Nothing to do
      }
    }));
  }
}
