/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

package com.here.xyz.httpconnector.util.jobs;

import static com.here.xyz.httpconnector.util.jobs.Job.Status.aborted;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.executed;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.failed;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.waiting;
import static com.here.xyz.httpconnector.util.scheduler.JobQueue.updateJobStatus;
import static io.netty.handler.codec.http.HttpResponseStatus.PRECONDITION_FAILED;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.util.jobs.datasets.DatasetDescription;
import com.here.xyz.httpconnector.util.jobs.datasets.Files;
import com.here.xyz.httpconnector.util.jobs.datasets.Spaces;
import com.here.xyz.httpconnector.util.web.HubWebClient;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.HttpException;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A job which consists of multiple child-jobs.
 * This CombinedJob does not process any data by itself. Instead, it's used to manage & hold a set of child-jobs of which each
 * is performing data processing.
 *
 * Some rules apply to CombinedJobs:
 * - The contained child-jobs of a CombinedJob are created by the CombinedJob itself, not separately by the user.
 * - Child-jobs are not handled separately by the user. The status of child-jobs can only be adjusted through the containing CombinedJob.
 * - When aborting a CombinedJob, all child-jobs are aborted.
 * - When one of the child-jobs fails, the containing CombinedJob is set to failed as well.
 * - When a CombinedJob fails, all contained child-jobs, which are not in a final state yet, are aborted.
 *
 * NOTE:
 * Currently CombinedJobs only support the source type "Spaces" and the target type "Files".
 */
public class CombinedJob extends Job<CombinedJob> {

  private static final Logger logger = LogManager.getLogger();

  protected List<Job> children = new ArrayList<>();

  @JsonIgnore
  private AtomicBoolean executing = new AtomicBoolean();

  private RuntimeStatus status = new RuntimeStatus();

  public CombinedJob() {
    super();
    setId(generateRandomId());
  }

  @Override
  public Future<CombinedJob> init() {
    //Set basic defaults
    setStatus(waiting); //TODO: Initialize fields at instantiation time (once DynamoJobConfigClient is fixed to use STATIC_MAPPER)
    //Instantiate / fill the child-jobs
    return createChildren();
  }

  @Override
  public Future<Job> prepareStart() {
    List<Future<Job>> futures = new ArrayList<>();
    for (Job childJob : children)
      futures.add(childJob.prepareStart());
    return Future.all(futures).map(cf -> this);
  }

  private Future<CombinedJob> createChildren() {
    List<Future<Job>> childFutures = new ArrayList<>();
    List<DatasetDescription.Space> childSpaces = ((Spaces) getSource()).createChildEntities();
    for (int i = 0; i < childSpaces.size(); i++) {
      final int childNo = i;
      DatasetDescription.Space childSpace = childSpaces.get(childNo);
      childFutures.add(HubWebClient.getSpace(childSpace.getId())
          .compose(space -> {
            Export job = new Export()
                .withId(getId() + "-" + childNo)
                .withSource(childSpace)
                .withTarget(getTarget());
            setChildJobParams(job, space);
            return Future.succeededFuture(job);
          }));
    }

    return Future.all(childFutures).map(compositeFuture -> {
      children.addAll(compositeFuture.list());
      return this;
    });
  }

  protected void setChildJobParams(Job childJob, Space space) {
    childJob.setChildJob(true); //TODO: Replace that hack once the scheduler flow was refactored
    childJob.init(); //TODO: Do field initialization at instance initialization time
    childJob.withTargetConnector(space.getStorage().getId());
    childJob.addParam("versionsToKeep", space.getVersionsToKeep());
    childJob.addParam("persistExport", space.isPersistExport());
  }

  @Override
  protected Future<Job> isValidForStart() {
    return super.isValidForStart()
        .compose(job -> {
          List<Future<Job>> futures = new ArrayList<>();
          for (Job childJob : children)
            futures.add(childJob.isValidForStart());
          return Future.all(futures).map(cf -> this);
        });
  }

  /**
   * @deprecated This is a workaround which is needed until the scheduler flow is fixed / refactored.
   * Please do not rely on this method or use it for other purposes than the current usage.
   * @return
   */
  @Deprecated
  private Future<List<Job>> reloadChildren() {
    List<Future<Job>> reloadFutures = children
        .stream()
        .map(childJob -> CService.jobConfigClient.get(getMarker(), childJob.getId()))
        .collect(Collectors.toList());
    return Future.all(reloadFutures)
        .map(cf -> {
          List<Job> reloadedChildren = cf.list();
          for (int i = 0; i < reloadedChildren.size(); i++)
            if (reloadedChildren.get(i) == null) {
              reloadedChildren.set(i, children.get(i));
              logger.warn(getMarker(), "Child job with Id {} could not be reloaded.", children.get(i).getId());
            }
          return reloadedChildren;
        });

  }

  @Override
  public Future<Job> executeStart() {
    return isValidForStart()
        .compose(job -> prepareStart())
        .compose(job -> enqueue());
  }

  private Future<Job> enqueue() {
    CService.exportQueue.addJob(this);
    children.forEach(childJob -> CService.exportQueue.addJob(childJob));
    return Future.succeededFuture(this);
  }

  @Override
  public Future<Job> executeAbort() {
    return super.executeAbort()
        .compose(job -> updateJobStatus(job, aborted))
        .compose(job -> abortAllNonFinalChildren());
  }

  @Override
  protected void isValidForRetry() throws HttpException {
    throw new HttpException(PRECONDITION_FAILED, "Retry is not supported for CombinedJobs.");
  }

  @Override
  public void resetToPreviousState() throws Exception {
    throw new HttpException(PRECONDITION_FAILED, "Retry is not supported for CombinedJobs.");
    //TODO: implement once retries are supported
  }

  @Override
  public String getQueryIdentifier() {
    //NOTE: Not needed for CombinedJobs, as the CombinedJob itself does not run any queries on the DB
    return null;
  }

  @Override
  public Future<Job> isProcessingPossible() {
    List<Future<Job>> futures = new ArrayList<>();
    for (Job childJob : children)
      futures.add(childJob.isProcessingPossible());
    return Future.any(futures).map(cf -> this);
  }

  @Override
  public void execute() {
    if (executing.compareAndSet(false, true)) {
      setExecutedAt(Core.currentTimeMillis() / 1000L);
      new Thread(() -> {
        while (children.stream().anyMatch(childJob -> !childJob.getStatus().isFinal())) {
          reloadChildren().onSuccess(reloadedChildren -> {
            children = reloadedChildren;
            store();
          });
          checkForNonSucceededChildren();
          try {
            Thread.sleep(1000);
          }
          catch (InterruptedException ignored) {}
        }
        checkForNonSucceededChildren();
        //Everything is processed
        logger.info("job[{}] CombinedJob completely succeeded!", getId());
//        addStatistic(statistic);
        if (!getStatus().isFinal())
          updateJobStatus(this, executed);
      }).start();
    }
  }

  private void checkForNonSucceededChildren() {
    Status nonSucceededStatus = null;
    if (children.stream().anyMatch(childJob -> childJob.getStatus() == failed))
      nonSucceededStatus = failed;
    else if (children.stream().anyMatch(childJob -> childJob.getStatus() == aborted))
      nonSucceededStatus = aborted;

    if (nonSucceededStatus != null) {
      Status combinedEndStatus = nonSucceededStatus;
      abortAllNonFinalChildren()
          .compose(job -> updateJobStatus(job, combinedEndStatus));
    }
  }

  private Future<Job> abortAllNonFinalChildren() {
    List<Job> nonFinalChildren = children
        .stream()
        .filter(childJob -> !childJob.getStatus().isFinal())
        .collect(Collectors.toList());
    List<Future<Job>> childrenAbortFutures = new ArrayList<>();
    for (Job childJob : nonFinalChildren)
      childrenAbortFutures.add(childJob.executeAbort());

    return Future
        .all(childrenAbortFutures)
        .map(compositeFuture -> this);
  }

  public List<Job> getChildren() {
    return new ArrayList<>(children);
  }

  @Override
  public Future<CombinedJob> validate() {
    //Currently only spaces -> files export is supported by this class
    if (!(getSource() instanceof Spaces))
      return Future.failedFuture(new ValidationException("CombinedJob supports only a source of type \"Spaces\"."));
    if (!(getTarget() instanceof Files))
      return Future.failedFuture(new ValidationException("CombinedJob supports only a target of type \"Files\"."));

    return validateChildren();
  }

  protected Future<CombinedJob> validateChildren() {
    List<Future<Job>> futures = new ArrayList<>();
    for (Job childJob : children)
      futures.add(childJob.validate());
    return Future.all(futures).map(cf -> this);
  }

  public Future<CombinedJob> store() {
    return CService.jobConfigClient.store(getMarker(), this)
        .map(job -> (CombinedJob) job);
  }

  @JsonIgnore
  public RuntimeStatus getRuntimeStatus() {
    return status;
  }
}
