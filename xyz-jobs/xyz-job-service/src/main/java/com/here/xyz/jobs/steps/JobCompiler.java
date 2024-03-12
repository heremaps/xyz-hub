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

package com.here.xyz.jobs.steps;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.steps.impl.AnalyzeSpaceTable;
import com.here.xyz.jobs.steps.impl.CreateIndex;
import com.here.xyz.jobs.steps.impl.DropIndexes;
import com.here.xyz.jobs.steps.impl.ImportFilesToSpace;
import com.here.xyz.jobs.steps.impl.MarkForMaintenance;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper;
import io.vertx.core.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.NotImplementedException;

public class JobCompiler {

  public Future<StepGraph> compile(Job job) {
    if (job.getSource() instanceof Files && job.getTarget() instanceof DatasetDescription.Space) {
      String spaceId = job.getTarget().getKey();
      StepGraph graph = new StepGraph()
          .addExecution(new DropIndexes().withSpaceId(spaceId)) // Drop all existing indices
          .addExecution(new ImportFilesToSpace().withSpaceId(spaceId)) // Perform import
          //TODO: Do not create *all* indices in parallel, make sure to (at least) keep the viz-index sequential #postgres-issue-with-partitions
          .addExecution(new StepGraph() // Create all the base indices in parallel
              .withExecutions(Stream.of(XyzSpaceTableHelper.Index.values())
                  .map(index -> new CreateIndex().withIndex(index).withSpaceId(spaceId)).collect(Collectors.toList())).withParallel(true))
          .addExecution(new AnalyzeSpaceTable().withSpaceId(spaceId))
          .addExecution(new MarkForMaintenance().withSpaceId(spaceId));

      return Future.succeededFuture(graph);
    }
    else
      return Future.failedFuture(new NotImplementedException("Only Space Import job is currently supported"));
  }

  public static JobCompiler getInstance() {
    //TODO: Return singleton instance using SPI
    return new JobCompiler();
  }
}
