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

import static com.here.xyz.jobs.RuntimeInfo.State.CANCELLED;
import static com.here.xyz.jobs.RuntimeInfo.State.CANCELLING;
import static com.here.xyz.jobs.RuntimeInfo.State.FAILED;
import static com.here.xyz.jobs.RuntimeInfo.State.PENDING;
import static com.here.xyz.jobs.RuntimeInfo.State.RUNNING;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.config.JobConfigClient;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.resources.ResourcesRegistry;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.Initializable;
import io.vertx.core.Future;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class JobExecutor implements Initializable {
  private static final Logger logger = LogManager.getLogger();
  private static final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
  private static final JobExecutor instance = new StateMachineExecutor();
  private static volatile boolean running;
  private static volatile boolean stopRequested;
  private static AtomicBoolean cancellationCheckRunning = new AtomicBoolean();
  private static final long CANCELLATION_TIMEOUT = 10 * 60 * 1_000; //10 min
  private static final long CANCELLATION_CHECK_RERUN_PERIOD = 10_000; //10 sec
  private static final long JOB_START_TIMEOUT = 60_000;

  {
    exec.scheduleWithFixedDelay(this::checkPendingJobs, 10, 60, SECONDS);
  }

  protected JobExecutor() {}

  public static JobExecutor getInstance() {
    return instance;
  }

  public final Future<Void> startExecution(Job job, String formerExecutionId) {
    //TODO: Care about concurrency between nodes when it comes to resource-load calculation within this thread
    return mayExecute(job)
        .compose(executionAllowed -> {
          if (!executionAllowed)
            //The Job remains in PENDING state and will be checked later again if it may be executed
            return Future.succeededFuture();

          //Update the job status atomically to RUNNING to make sure no other node will try to start it.
          job.getStatus()
              .withState(RUNNING)
              .withInitialEndTimeEstimation(Core.currentTimeMillis()
                  + job.getSteps().stepStream().mapToInt(step -> step.getEstimatedExecutionSeconds()).sum() * 1_000);

          //TODO: Update / invalidate the reserved unit maps?
          return job.storeStatus(PENDING)
              .compose(v -> formerExecutionId != null ? resume(job, formerExecutionId) : execute(job))
              //Execution was started successfully, store the execution ID.
              .compose(executionId -> job.withExecutionId(executionId).store());
        });
  }

  private void checkPendingJobs() {
    running = true;
    if (stopRequested) {
      //Do not start an execution if a stop was requested
      running = false;
      return;
    }

    try {
      JobConfigClient.getInstance().loadJobs(PENDING)
          .onSuccess(pendingJobs -> {
            try {
              pendingJobs.sort(Comparator.comparingLong(Job::getCreatedAt));
              logger.info("Checking {} PENDING jobs if they can be executed ...", pendingJobs.size());
              for (Job pendingJob : pendingJobs) {
                if (stopRequested)
                  return;
                //Try to start the execution of the pending job
                if (tryAndWaitForStart(pendingJob))
                  return;
              }
            }
            catch (Exception e) {
              logger.error("Error checking PENDING jobs", e);
            }
          })
          .onFailure(t -> logger.error("Error checking PENDING jobs", t))
          .onComplete(ar -> running = false);
    }
    catch (Exception e) {
      logger.error("Error checking PENDING jobs", e);
    }
    finally {
      running = false;
    }
  }

  /**
   *
   * @param pendingJob
   * @return true if there was a request to stop the PENDING-check thread
   */
  private boolean tryAndWaitForStart(Job pendingJob) {
    try {
      Future<Void> executionStarted = startExecution(pendingJob, pendingJob.getExecutionId());
      long waitStart = Core.currentTimeMillis();
      while (!executionStarted.isComplete()) {
        if (Core.currentTimeMillis() - waitStart > JOB_START_TIMEOUT) {
          logger.error("Timeout while trying to start PENDING job {}", pendingJob.getId());
          return false;
        }
        try {
          if (stopRequested)
            return true;
          Thread.sleep(200);
        }
        catch (InterruptedException ignore) {}
      }
    }
    catch (Exception e) {
      logger.error("Error trying to start the execution of job {}", pendingJob.getId(), e);
    }
    return false;
  }

  @Override
  public Future<Void> init() {
    checkCancellations();
    return Initializable.super.init();
  }

  /**
   * Starts a process which performs the following actions *once*:
   *  - Check if there exist any jobs which are currently in CANCELLING state
   *  - For each job in CANCELLING state
   *    - wait until all its steps are in state CANCELLED
   *    - Once that is the case update the job to state CANCELLED
   *  - Complete the process once all the affected jobs are in state CANCELLED
   *  - If the affected jobs are not all in state CANCELLED after a specified timeout do the following:
   *    - Update the remaining jobs to the FAILED state and set them to be not resumable
   *    - Throw an exception to stop the process
   */
  protected static void checkCancellations() {
    if (cancellationCheckRunning.compareAndSet(false, true))
      JobConfigClient.getInstance().loadJobs(CANCELLING)
          .compose(jobs -> Future.all(jobs.stream().map(job -> {
            if (job.getSteps().stepStream().map(step -> cancelNonRunningStep(job, step)).allMatch(step -> step.getStatus().getState().isFinal())) {
              job.getStatus().withState(CANCELLED).withDesiredAction(null);
              return job.storeStatus(CANCELLING);
            }
            return Future.succeededFuture();
          }).collect(Collectors.toList())).map(jobs.stream().filter(job -> job.getStatus().getState() != CANCELLED).collect(Collectors.toList())))
          .compose(remainingJobs -> {
            if (remainingJobs.isEmpty())
              return Future.succeededFuture(false);

            //Fail all jobs of which the cancellation did not work within <CANCELLATION_TIMEOUT> ms
            List<Job> jobsToFail = remainingJobs
                .stream()
                .filter(job -> job.getStatus().getUpdatedAt() + CANCELLATION_TIMEOUT < Core.currentTimeMillis())
                .collect(Collectors.toList());
            jobsToFail.forEach(job -> {
              job.getStatus()
                  .withState(FAILED)
                  .withFailedRetryable(false)
                  .withErrorMessage("The cancellation of the job could not be completed within the specified amount of time.")
                  .withErrorCause("CANCELLATION_TIMEOUT")
                  .withDesiredAction(null);

              job.storeStatus(null);
            });

            //If there are still remaining jobs, run the cancellation check again
            return Future.succeededFuture(jobsToFail.size() < remainingJobs.size());
          })
          .onFailure(t -> logger.error("Error in checkCancellations process:", t))
          .onSuccess(runAgain -> {
            if (runAgain)
              exec.schedule(() -> checkCancellations(), CANCELLATION_CHECK_RERUN_PERIOD, MILLISECONDS);
          })
          .onComplete(ar -> cancellationCheckRunning.set(false));
  }

  private static Step cancelNonRunningStep(Job job, Step step) {
    final State stepState = step.getStatus().getState();
    if (stepState != RUNNING && stepState.isValidSuccessor(CANCELLING)) {
      step.getStatus().withState(CANCELLING).withState(CANCELLED);
      job.storeUpdatedStep(step);
    }
    return step;
  }

  protected abstract Future<String> execute(Job job);

  protected abstract Future<String> execute(StepGraph formerGraph, Job job);

  protected abstract Future<String> resume(Job job, String executionId);

  public abstract Future<Void> cancel(String executionId);

  public abstract Future<Void> delete(String executionId);

  public abstract Future<List<String>> list();

  /**
   * Checks for all resource-loads of the specified job whether they can be fulfilled. If yes, the job may be executed.
   *
   * @param job The job to be checked
   * @return true, if the job may be executed / enough resources are free
   */
  private Future<Boolean> mayExecute(Job job) {
    //Check for all needed resource loads whether they can be fulfilled
    return ResourcesRegistry.getFreeVirtualUnits()
        .map(freeVirtualUnits -> job.calculateResourceLoads().stream()
            .allMatch(load -> {
              final boolean sufficientFreeUnits = load.getEstimatedVirtualUnits() < freeVirtualUnits.get(load.getResource());
              if (!sufficientFreeUnits)
                logger.info("Job {} can not be executed (yet) as there are not sufficient units available of resource {}. "
                        + "Needed units: {}, currently available units: {}",
                    job.getId(), load.getResource(), load.getEstimatedVirtualUnits(), freeVirtualUnits.get(load.getResource()));
              return sufficientFreeUnits;
            }));
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
