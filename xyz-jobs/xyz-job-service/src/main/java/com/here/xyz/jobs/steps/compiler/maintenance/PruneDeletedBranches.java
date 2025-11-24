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

package com.here.xyz.jobs.steps.compiler.maintenance;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.datasets.System;
import com.here.xyz.jobs.processes.Prune;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.StepExecution;
import com.here.xyz.jobs.steps.compiler.JobCompilationInterceptor;
import com.here.xyz.jobs.steps.impl.maintenance.DeleteBranchTable;
import com.here.xyz.models.hub.Branch.DeletedBranch;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.util.Comparator;
import java.util.List;

public class PruneDeletedBranches implements JobCompilationInterceptor {

  @Override
  public boolean chooseMe(Job job) {
    return job.getProcess() instanceof Prune;
  }

  /*
  NOTE: Sorting the branches by their dependencies is an optimization to prevent
  keeping branches just because they have been referenced by another deleted (dependent) branch.
  That way the referenced branches can be also deleted directly instead of waiting for the next pruning.
   */
  private static final Comparator<DeletedBranch> BRANCH_DEPENDENCY_COMPARATOR = (branch1, branch2) ->
      branch1.getBaseRef().getBranch().equals(branch2.getId())
          ? -1
          : branch2.getBaseRef().getBranch().equals(branch1.getId()) ? 1 : 0;

  @Override
  public CompilationStepGraph compile(Job job) {
    try {
      List<DeletedBranch> deletedBranches = HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadAllDeletedBranches()
          .stream()
          .sorted(BRANCH_DEPENDENCY_COMPARATOR)
          .toList();
      List<StepExecution> deleteSteps = deletedBranches.stream().map(branch -> (StepExecution) new DeleteBranchTable()
              .withSpaceId(branch.getSpaceId())
              .withBranch(branch))
          .toList();

      job.withSource(new System())
          .withTarget(new System());

      return (CompilationStepGraph) new CompilationStepGraph()
          .withExecutions(deleteSteps);
    }
    catch (WebClientException e) {
      throw new RuntimeException("Unable to retrieve the list of deleted branches.", e);
    }
  }
}
