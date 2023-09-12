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

import static com.here.xyz.httpconnector.util.jobs.Job.Status.waiting;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.util.jobs.DatasetDescription.Files;
import com.here.xyz.httpconnector.util.jobs.DatasetDescription.Spaces;
import com.here.xyz.httpconnector.util.web.HubWebClient;
import com.here.xyz.hub.connectors.models.Space;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.List;

/**
 * A job which consists of multiple child-jobs.
 * This CombinedJob itself does not process any data by itself. Instead, it's used to manage & hold a set of child-jobs of which each
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

  private List<Job> children = new ArrayList<>();

  public CombinedJob() {
    super();
    //Set basic defaults
    setStatus(waiting);
  }

  @Override
  public Future<CombinedJob> init() {
    //Instantiate / fill the child-jobs
    return createChildren();
  }

  @Override
  public Future<Job> prepareStart() {
    return null;
  }

  @Override
  public Future<Job> executeStart() {
    return null;
  }

  private Future<CombinedJob> createChildren() {
    //Currently only spaces -> files export is supported by this class
    if (!(getSource() instanceof Spaces))
      return Future.failedFuture(new ValidationException("CombinedJob supports only a source of type \"Spaces\"."));
    if (!(getTarget() instanceof Files))
      return Future.failedFuture(new ValidationException("CombinedJob supports only a target of type \"Files\"."));

    List<Future<Void>> childFutures = new ArrayList<>();
    List<String> spaceIds = ((Spaces) getSource()).getSpaceIds();
    for (int i = 0; i < spaceIds.size(); i++) {
      final int childNo = i;
      String spaceId = spaceIds.get(childNo);
      childFutures.add(HubWebClient.getSpace(spaceId)
          .compose(space -> {
            Export job = new Export()
                .withId(getId() + "-" + childNo)
                .withSource(new DatasetDescription.Space().withId(spaceId))
                .withTarget(getTarget());

            setChildJobParams(job, space);

            children.add(job);

            return Future.succeededFuture();
          }));
    }

    return Future.all(childFutures).map(v -> this);
  }

  private void setChildJobParams(Job childJob, Space space) {
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

  @Override
  public void finalizeJob() {
    children.forEach(childJob -> childJob.finalizeJob());
    super.finalizeJob();
  }

  @Override
  public void resetToPreviousState() throws Exception {

  }

  @Override
  public String getQueryIdentifier() {
    return null;
  }

  @Override
  public void execute() {

  }

  public List<Job> getChildren() {
    return children;
  }

  @Override
  public Future<CombinedJob> validate() {
    return super.validate()
        .compose(job -> {
          List<Future<Job>> futures = new ArrayList<>();
          for (Job childJob : children)
            futures.add(childJob.validate());
          return Future.all(futures).map(cf -> this);
        });
  }

  public Future<CombinedJob> store() {
    return CService.jobConfigClient.store(getMarker(), this)
        .map(job -> (CombinedJob) job);
  }
}
