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
import static com.here.xyz.jobs.RuntimeInfo.State.SUCCEEDED;
import static com.here.xyz.jobs.steps.execution.GraphFusionTool.fuseGraphs;
import static java.util.Comparator.comparingLong;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.jobs.config.JobConfigClient;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.Step.InputSet;
import com.here.xyz.jobs.steps.StepExecution;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.inputs.ModelBasedInput;
import com.here.xyz.jobs.steps.resources.ResourcesRegistry;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.Initializable;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
        }).onFailure( e -> {
            //E.g.: Resource got deleted during lifetime of job!
            logger.error("Start Execution of job '{}' is failed!", job.getId(), e);
            job.getStatus().setState(FAILED);
            job.storeStatus(null);
        });
  }

  private Future<Void> setStepsRunning(Job job) {
    job.getSteps().stepStream().forEach(step -> {
      if(step.getStatus().getState().equals(SUCCEEDED))
        return;
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
    //Job has reused a completed execution.
    if(job.getStatus().getState() == SUCCEEDED)
      return Future.succeededFuture(false);

    //Check for all needed resource loads whether they can be fulfilled
    return ResourcesRegistry.getFreeVirtualUnits()
        .map(freeVirtualUnits -> job.calculateResourceLoads().stream()
            .allMatch(load -> {
              final boolean sufficientFreeUnits = freeVirtualUnits.containsKey(load.getResource())
                  && load.getEstimatedVirtualUnits() < freeVirtualUnits.get(load.getResource());
              if (!sufficientFreeUnits)
                logger.info("Job {} can not be executed (yet) as the resource {} is not available or there are not sufficient "
                        + "resource units available. Needed units: {}, currently available units: {}",
                    job.getId(), load.getResource(), load.getEstimatedVirtualUnits(), freeVirtualUnits.get(load.getResource()));

              logger.info("Job {} can be executed. Resource {}, needed units: {}, currently available units: {}",
                      job.getId(), load.getResource(), load.getEstimatedVirtualUnits(), freeVirtualUnits.get(load.getResource()));
              return sufficientFreeUnits;
            }));
  }

  private Future<Boolean> needsExecution(Job job) {
    return Future.succeededFuture()
        .compose(v -> {
          if (job.getSteps().stepStream().anyMatch(step -> !step.getStatus().getState().equals(SUCCEEDED)))
            return Future.succeededFuture(true);

          //All Steps are already succeeded - No need to execute the job
          job.getStatus().setState(SUCCEEDED);
          job.getStatus().setSucceededSteps(job.getStatus().getOverallStepCount());
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
    if (job.getResourceKey() == null || job.getSteps().stepStream().anyMatch(step -> step instanceof DelegateStep))
      return Future.succeededFuture();
    return JobConfigClient.getInstance().loadJobs(job.getResourceKey(), job.getSecondaryResourceKey(), SUCCEEDED)
        .compose(candidates -> Future.succeededFuture(candidates.stream()
            .filter(candidate -> !job.getId().equals(candidate.getId())) //Do not try to compare the job to itself
            .map(candidate -> fuseGraphs(job, candidate.getSteps()))
            .max(comparingLong(candidateGraph -> candidateGraph.stepStream().filter(step -> step instanceof DelegateStep).count())) //Take the candidate with the largest matching subgraph
            .orElse(null)))
        .compose(newGraphWithReusedSteps -> newGraphWithReusedSteps == null
            ? Future.succeededFuture()
            : job.withSteps(newGraphWithReusedSteps).store());
  }

  //TODO: emrParamReplacement(shrunkGraph, collectDelegateReplacements(shrunkGraph)); ... Use InputSets instead?

  //TODO: Use new fuseGraphs() method instead for the tests
  @Deprecated
  public static Future<StepGraph> shrinkGraphByReusingOtherGraph(Job job, StepGraph reusedGraph) {
    if (reusedGraph == null || reusedGraph.isEmpty())
      return Future.succeededFuture();

    StepGraph shrunkGraph = new StepGraph()
            .withParallel(reusedGraph.isParallel())
            .withExecutions(calculateShrunkForest(job, reusedGraph));

    /**
     * TODO: It could be that other implementations have other references which we need to update. Target
     *  should be a generic implementation - without the need to adjust this function specifically.
     */
    //if we have delegate, we need to update references for EMR
    emrParamReplacement(shrunkGraph, collectDelegateReplacements(shrunkGraph));

    return Future.succeededFuture(shrunkGraph);
  }

  /**
   * Replaces all StepExecutions, which could get reused, with DelegateOutputsSteps.
   * This is required to be able to access the existing outputs.
   *
   * @param executions  StepExecutions of current Graph
   * @param reusedExecutions StepExecutions which should get reused
   */
  @Deprecated
  private static void replaceWithDelegateOutputSteps(List<StepExecution> executions, List<StepExecution> reusedExecutions) {
    for (int i = 0; i < reusedExecutions.size(); i++) {
      StepExecution execution = executions.get(i);
      StepExecution reusedExecution = reusedExecutions.get(i);

      if (execution instanceof StepGraph graph && reusedExecution instanceof StepGraph reusedGraph) {
        replaceWithDelegateOutputSteps(graph.getExecutions(), reusedGraph.getExecutions());
      }
      else if (execution instanceof Step step && reusedExecution instanceof Step reusedStep) {
        // Replace the execution with DelegateOutputsPseudoStep
        executions.set(i, new DelegateOutputsPseudoStep(reusedStep.getJobId(), reusedStep.getId(), step.getJobId(), step.getId())
            .withOutputMetadata(reusedStep.getOutputMetadata()));
      }
    }
  }

  /**
   * Calculates a modified version of the current job's execution graph by replacing reusable
   * portions with delegateOutputSteps and connecting new executions to the reused graph where applicable.
   *
   * <p>This method performs the following operations:</p>
   * <ul>
   *   <li>Replaces all executions in the current graph that overlap with the reused graph
   *       by substituting them with {@code DelegateOutputsPseudoStep} instances.</li>
   *   <li>Links the first new execution node (if present) to the leaf nodes of the reused graph.</li>
   *   <li>Adjusts step dependencies by injecting the previous step IDs from the reused graph into
   *       subsequent steps in the current graph.</li>
   * </ul>
   *
   * @param job the current job being executed, which provides the existing execution graph.
   * @param reusedGraph the graph containing reusable executions to be integrated into the current job's graph.
   * @return a list containing a single {@code StepGraph} object that represents the updated execution graph
   *         with reusable portions replaced and dependencies adjusted.
   */
  @Deprecated
  private static List<StepExecution> calculateShrunkForest(Job job, StepGraph reusedGraph) {
    List<StepExecution> executions = job.getSteps().getExecutions();
    //Replace all executions, which can get reused form reusedGraph, in current graph with DelegateOutputSteps.
    replaceWithDelegateOutputSteps(executions, reusedGraph.getExecutions());
    resolveReusedInputs(job.getSteps());
    //TODO: Move re-weaving of previousStepIds into first traversal implementation

    int sizeReusedGraph = reusedGraph.getExecutions().size();
    int sizeResultGraph = executions.size();

    if (sizeResultGraph > sizeReusedGraph) {
      //patch first item and build link to reused Graph
      Step firstNewExecutionNode = getFirstExecutionNode(executions.get(sizeReusedGraph));
      //get last nodes (leafs) of reusedGraph
      List<Step> lastExecutionNodesOfReusedGraph = getAllLeafExecutionNodes(reusedGraph.getExecutions()
          .get((reusedGraph.getExecutions()).size() - 1));

      //get previousStepIds of nodes that come after the reused Graph
      Set<String> previousStepIdsOfFirstNewNode = firstNewExecutionNode.getPreviousStepIds().isEmpty()
          ? null
          : firstNewExecutionNode.getPreviousStepIds();

      //get all stepIds of last nodes (leafs) of reusedGraph
      Set<String> stepIdsOfLastReusedNodes = new HashSet<>();
      lastExecutionNodesOfReusedGraph.forEach(step -> stepIdsOfLastReusedNodes.add(step.getId()));

      //update ExecutionDependencies of current graph - goal is reusability of outputs of reused steps
      updateExecutionDependencies(executions, previousStepIdsOfFirstNewNode, stepIdsOfLastReusedNodes);
    }

    return executions;
  }

  /**
   * Resolves all output-to-input assignments of the graph that is currently being fused by re-weaving the re-used outputs with the
   * consuming steps.
   * That is being done by overwriting the {@link InputSet}s of the consuming steps with the new references of the according
   * {@link DelegateOutputsPseudoStep}s.
   * Should be only called for a graph **after** performing all delegation-replacements.
   *
   * @param graph
   */
  @Deprecated
  static void resolveReusedInputs(StepGraph graph) {
    graph.stepStream().forEach(step -> resolveReusedInputs(step, graph));
  }

  /**
   * Resolves the inputSets of a new step that is potentially consuming outputs of an old step referenced by a {@link DelegateStep}.
   *
   * @param step
   * @param containingStepGraph
   */
  @Deprecated
  private static void resolveReusedInputs(Step step, StepGraph containingStepGraph) {
    List<InputSet> newInputSets = new ArrayList<>();
    for (InputSet compiledInputSet : (List<InputSet>) step.getInputSets()) {
      if (compiledInputSet.stepId() == null || !(containingStepGraph.getStep(compiledInputSet.stepId()) instanceof DelegateOutputsPseudoStep replacementStep))
        //NOTE: stepId == null on an InputSet refers to the USER-inputs
        newInputSets.add(compiledInputSet);
      else
        //Now we know that inputSet is one that should be replaced by one that is pointing to the outputs of the old graph
        newInputSets.add(new InputSet(replacementStep.getDelegateJobId(), replacementStep.getDelegateStepId(), compiledInputSet.name(),
            compiledInputSet.modelBased()));
    }
    step.setInputSets(newInputSets);
  }

  /**
   * Recursively injects updated step dependencies and input step IDs into a list of step executions.
   *
   * <p>This method traverses a list of {@code StepExecution} objects, replacing references to reusable
   * steps from a previous graph with updated dependencies and inputs. If a {@code StepExecution} is
   * a {@code StepGraph}, the method processes its nested executions recursively.</p>
   *
   * <p>The following updates are made to each {@code Step}:</p>
   * <ul>
   *   <li><b>Previous Step IDs:</b> Replaces any IDs matching the previous step IDs of the reused graph
   *   with the IDs of the last steps in the reused graph.</li>
   *   <li><b>Input Step IDs:</b> Updates input step IDs to reference the reused graph's job ID and step IDs.</li>
   * </ul>
   *
   * @param executions the list of {@code StepExecution} objects to be updated, which may include nested {@code StepGraph} instances.
   * @param previousStepIdsOfFirstNewNode the set of previous step IDs from the first new execution node, used for replacement.
   * @param stepIdsOfLastReusedNodes the set of step IDs from the leaf nodes of the reused graph, used to replace previous step IDs.
   */
  @Deprecated
  private static void updateExecutionDependencies(List<StepExecution> executions, Set<String> previousStepIdsOfFirstNewNode,
      Set<String> stepIdsOfLastReusedNodes) {
    for (StepExecution executionNode : executions) {
      if (executionNode instanceof StepGraph graph) {
        //Recursive injection for nested StepGraph
        updateExecutionDependencies(graph.getExecutions(), previousStepIdsOfFirstNewNode, stepIdsOfLastReusedNodes);
      }
      else if (executionNode instanceof Step<?> step) {
        if(previousStepIdsOfFirstNewNode == null)
          continue;
        //Replace previous StepIds
        if (!step.getPreviousStepIds().isEmpty() && step.getPreviousStepIds().removeAll(previousStepIdsOfFirstNewNode))
          step.getPreviousStepIds().addAll(stepIdsOfLastReusedNodes);
      }
    }
  }

  @Deprecated
  private static List<String[]> collectDelegateReplacements(StepGraph stepGraph) {
    List<String[]> replacements = new ArrayList<>();

    for (StepExecution stepExecution : stepGraph.getExecutions()) {
      if (stepExecution instanceof StepGraph graph) {
        // Recursively collect replacements from nested StepGraphs
        replacements.addAll(collectDelegateReplacements(graph));
      } else if (stepExecution instanceof DelegateOutputsPseudoStep delegateStep) {
        // Add the replacement paths from DelegateOutputsPseudoStep
        replacements.add(delegateStep.getReplacementPathForFiles());
      }
    }

    return replacements;
  }

  @Deprecated
  protected static void emrParamReplacement(StepGraph stepGraph, List<String[]> replacements) {
    stepGraph.stepStream().forEach(step -> {
      if (step instanceof RunEmrJob emrJobStep) {
        List<String> scriptParams = emrJobStep.getScriptParams();
        for (int i = 0; i < scriptParams.size(); i++) {
          String param = scriptParams.get(i);

          for (String[] replacement : replacements)
            param = param.replaceAll(replacement[0], replacement[1]);

          scriptParams.set(i, param);
        }
        emrJobStep.setScriptParams(scriptParams);
      }
    });
  }

  @Deprecated
  private static Step getFirstExecutionNode(StepExecution executionNode) {
    if (executionNode instanceof StepGraph previousNodeGraph) {
      //Get the first node of the graph
      return getFirstExecutionNode(previousNodeGraph.getExecutions().get(0));
    }
    return (Step) executionNode;
  }

  /**
   * Recursively retrieves all leaf nodes from the given {@code StepExecution}.
   *
   * <p>This method traverses a hierarchy of {@code StepExecution} objects. If the execution node
   * is a {@code StepGraph}, it recursively processes all its child executions. If the execution
   * node is a {@code Step}, it is considered a leaf node and added to the result.</p>
   *
   * <p>The method ensures that all terminal steps (leaves) in a potentially nested graph
   * structure are collected into a flat list.</p>
   *
   * @param executionNode the root {@code StepExecution} from which to collect all leaf nodes.
   *                      This may be a {@code Step} or a {@code StepGraph}.
   * @return a list of {@code Step} objects representing all leaf nodes in the execution graph.
   */
  @Deprecated
  public static List<Step> getAllLeafExecutionNodes(StepExecution executionNode) {
    List<Step> leafNodes = new ArrayList<>();

    if (executionNode instanceof StepGraph graph) {
      // Traverse all child executions in the graph
      for (StepExecution child : graph.getExecutions()) {
        leafNodes.addAll(getAllLeafExecutionNodes(child));
      }
    } else if (executionNode instanceof Step) {
      // If it's a leaf node, add it to the list
      leafNodes.add((Step) executionNode);
    }

    return leafNodes;
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
