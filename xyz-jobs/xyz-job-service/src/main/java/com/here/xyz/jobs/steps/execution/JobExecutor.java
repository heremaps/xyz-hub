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

package com.here.xyz.jobs.steps.execution;

import static com.here.xyz.jobs.RuntimeInfo.State.CANCELLED;
import static com.here.xyz.jobs.RuntimeInfo.State.CANCELLING;
import static com.here.xyz.jobs.RuntimeInfo.State.FAILED;
import static com.here.xyz.jobs.RuntimeInfo.State.PENDING;
import static com.here.xyz.jobs.RuntimeInfo.State.RUNNING;
import static com.here.xyz.jobs.RuntimeInfo.State.SUCCEEDED;
import static com.here.xyz.jobs.steps.execution.GraphFusionTool.fuseGraphs;
import static java.util.Comparator.comparingLong;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.config.JobConfigClient;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.inputs.ModelBasedInput;
import com.here.xyz.jobs.steps.resources.ResourcesRegistry;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.Initializable;
import io.vertx.core.Future;
import java.util.ArrayList;
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
    return Future.succeededFuture()
        .compose(v -> formerExecutionId == null ? reuseExistingJobIfPossible(job) : Future.succeededFuture())
        .compose(v -> needsExecution(job))
        .compose(executionNeeded -> executionNeeded ? mayExecute(job) : Future.succeededFuture(false))
        .compose(shouldExecute -> {
          if (!shouldExecute)
            /*
            Two different cases:
            1. The job is complete already because all its steps are re-using old steps. In that case, the job is succeeded already.
            2. The job may currently not be executed because of short resources.
              It remains in PENDING state and will be checked later again if it may be executed.
             */
            return Future.succeededFuture();

          //Update the job status atomically to RUNNING to make sure no other node will try to start it.
          job.getStatus()
              .withState(RUNNING)
              .withInitialEndTimeEstimation(Core.currentTimeMillis()
                  + job.getSteps().stepStream().mapToInt(step -> step.getEstimatedExecutionSeconds()).sum() * 1_000);

          //TODO: Update / invalidate the reserved unit maps?
          return job.storeStatus(PENDING)
              .compose(v -> job.isPipeline() ? setStepsRunning(job) : Future.succeededFuture())
              .compose(v -> formerExecutionId != null ? resume(job, formerExecutionId) : execute(job))
              //Execution was started successfully, store the execution ID.
              .compose(executionId -> job.withExecutionId(executionId).store());
        })
        .onFailure(e -> {
          //E.g.: Resource got deleted during lifetime of job!
          logger.error("Start Execution of job '{}' is failed!", job.getId(), e);
          job.getStatus().setState(FAILED);
          job.storeStatus(null);
        });
  }

  private Future<Void> setStepsRunning(Job job) {
    job.getSteps().stepStream().forEach(step -> {
      if (!(step instanceof DelegateStep))
        step.getStatus().setState(RUNNING);
    });
    return job.store();
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
          .compose(pendingJobs -> {
            Future<Void> taskChain = Future.succeededFuture();
            try {
              pendingJobs.sort(comparingLong(Job::getCreatedAt));
              logger.info("Checking {} PENDING jobs if they can be executed ...", pendingJobs.size());
              if (stopRequested)
                return Future.succeededFuture();

              //Now start one job after each other in the correct order
              for (Job pendingJob : pendingJobs)
                /*
                Add a delay to prevent starting a job 2 times. That could happen in rare cases because also in the
                normal starting procedure, the job is in PENDING state for a very short period. (But that should be normally some seconds at max)
                 */
                if (Core.currentTimeMillis() > pendingJob.getStatus().getUpdatedAt() + 60_000)
                  //Try to start the execution of the pending job
                  taskChain = taskChain.compose(v -> tryAndWaitForStart(pendingJob));
            }
            catch (Exception e) {
              logger.error("Error checking PENDING jobs", e);
            }
            /*
            The task chain is now fully created and will continue to be worked through, even if this thread ends now.
            The "running" variable will be set to false again after the completion of the task chain (see: #onComplete() call below)
            */
            return taskChain;
          })
          .onFailure(t -> logger.error("Error checking PENDING jobs", t))
          .onComplete(ar -> running = false);
    }
    catch (Exception e) {
      logger.error("Error checking PENDING jobs", e);
      running = false;
    }
  }

  private static long calculateEstimatedExecutionTime(Job job) {
    return job.getSteps().stepStream().mapToInt(step -> step.getEstimatedExecutionSeconds()).sum() * 1_000;
  }

  //TODO: integrate updatePendingTimeEstimations again.
  //    return updatePendingTimeEstimations(pendingJobs.stream().filter(job -> job.getStatus().getState() == PENDING)
  //            .collect(Collectors.toUnmodifiableList()));
  private Future<Void> updatePendingTimeEstimations(List<Job> pendingJobs) {
    return JobConfigClient.getInstance().loadJobs(RUNNING)
        .compose(runningJobs -> {
          if (runningJobs.isEmpty()) {
            //No jobs are running, but we have PENDING jobs, are the jobs trying to use too many resources of the system overall?
            logger.warn("Following jobs are PENDING, but there are no running jobs, does the system provide enough resources to "
                + "start these jobs at all? : {}", pendingJobs.stream().map(Job::getId).collect(Collectors.joining(", ")));
            //Set the estimated PENDING time to "unknown"
            pendingJobs.forEach(pendingJob -> pendingJob.getStatus().setEstimatedStartTime(-1));
            return Future.succeededFuture();
          }

          List<Job> activeJobs = new ArrayList<>(runningJobs);

          //NOTE: The following is an algorithm that only provides a very rough estimation of the start time
          for (Job pendingJob : pendingJobs) {
            Job earliestCompletingJob = activeJobs.stream().min(comparingLong(job -> job.getStatus().getEstimatedEndTime())).get();
            long estimatedStartTime = earliestCompletingJob.getStatus().getEstimatedEndTime();
            pendingJob.getStatus()
                .withEstimatedStartTime(estimatedStartTime)
                .withInitialEndTimeEstimation(estimatedStartTime + calculateEstimatedExecutionTime(pendingJob));
            //Replace the (already "consumed") active job by the consuming pending job for the calculation in the next loop iteration
            activeJobs.remove(earliestCompletingJob);
            activeJobs.add(pendingJob);
          }
          return Future.succeededFuture();
        });
  }

  /**
   *
   * @param pendingJob
   * @return A future that succeeds when the job was started and fails if there was an error starting the job or
   *  when there was a request to stop the PENDING-check thread
   */
  private Future<Void> tryAndWaitForStart(Job pendingJob) {
    //TODO: re-introduce starting timeout
    Future<Void> executionStarted = startExecution(pendingJob, pendingJob.getExecutionId());
    if (stopRequested)
      return executionStarted
              .compose(v -> Future.failedFuture(new RuntimeException("The PENDING-check thread has been stopped without completing.")));
    return executionStarted;
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

  public abstract Future<Void> sendInput(Job job, ModelBasedInput input);

  protected abstract Future<String> resume(Job job, String executionId);

  public abstract Future<Void> cancel(String executionId);

  /**
   * Deletes all execution resources of the specified execution ID.
   * @param executionId
   * @return Whether a deletion actually took place. (Resources could've been deleted before already)
   */
  public abstract Future<Boolean> deleteExecution(String executionId);

  /**
   * Checks for all resource-loads of the specified job whether they can be fulfilled. If yes, the job may be executed.
   *
   * @param job The job to be checked
   * @return true, if the job may be executed / enough resources are free
   */
  private Future<Boolean> mayExecute(Job job) {
    logger.info("[{}] Checking whether there are enough resources to execute the job ...", job.getId());
    //Check for all necessary resource loads whether they can be fulfilled
    return ResourcesRegistry.getFreeVirtualUnits()
        .compose(freeVirtualUnits -> job.calculateResourceLoads()
            .map(neededResources -> neededResources.stream().allMatch(load -> {
              final boolean sufficientFreeUnits = freeVirtualUnits.containsKey(load.getResource())
                  && load.getEstimatedVirtualUnits() < freeVirtualUnits.get(load.getResource());
              if (!sufficientFreeUnits)
                logger.info("Job {} can not be executed (yet) as the resource {} is not available or there are not sufficient "
                        + "resource units available. Needed units: {}, currently available units: {}",
                    job.getId(), load.getResource(), load.getEstimatedVirtualUnits(), freeVirtualUnits.get(load.getResource()));
              return sufficientFreeUnits;
            })));
  }

  private Future<Boolean> needsExecution(Job job) {
    return Future.succeededFuture()
        .compose(v -> {
          int succeededSteps = (int) job.getSteps().stepStream()
              .filter(step -> step.getStatus().getState().equals(SUCCEEDED))
              .count();
          job.getStatus().setSucceededSteps(succeededSteps);

          if (succeededSteps < job.getStatus().getOverallStepCount())
            return Future.succeededFuture(true);

          //All Steps are already succeeded - No need to execute the job
          job.getStatus().setState(SUCCEEDED);
          return job.store().map(false);
        });
  }

  /**
   * Tries to find other existing jobs, that are completed and that already performed parts of the
   * tasks that the provided job would have to perform.
   * If such jobs are found, the provided job's StepGraph will re-use these parts of the already executed
   * job and will be shrunk accordingly to lower its execution time and save cost.
   *
   * @param job The new job that is about to be started
   * @return An empty future (NOTE: If the job's graph was adjusted / shrunk, it will be also stored)
   */
  private static Future<Void> reuseExistingJobIfPossible(Job job) {
    logger.info("[{}] Checking for reusable existing jobs ...", job.getId());
    if (job.getResourceKey() == null || job.getSteps().stepStream().anyMatch(step -> step instanceof DelegateStep))
      return Future.succeededFuture();
    return JobConfigClient.getInstance().loadJobs(job.getResourceKey(), job.getSecondaryResourceKey(), SUCCEEDED)
        .compose(candidates -> Future.succeededFuture(candidates.stream()
            .filter(candidate -> !job.getId().equals(candidate.getId())) //Do not try to compare the job to itself
            .map(candidate -> fuseGraphs(job, candidate.getSteps()))
            .max(comparingLong(candidateGraph -> candidateGraph.stepStream()
                .filter(step -> step instanceof DelegateStep).count())) //Take the candidate with the largest matching subgraph
            .orElse(null)))
        .compose(fusedGraph -> fusedGraph == null ? Future.succeededFuture() : job.withSteps(fusedGraph).store());
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
