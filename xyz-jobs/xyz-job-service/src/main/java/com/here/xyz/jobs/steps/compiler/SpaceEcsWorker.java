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

package com.here.xyz.jobs.steps.compiler;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.DatasetDescription.Space;
import com.here.xyz.jobs.datasets.filters.Filters;
import com.here.xyz.jobs.processes.Ecsosphere;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.JobCompiler.CompilationError;
import com.here.xyz.jobs.steps.execution.EcsTaskStep;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.responses.StatisticsResponse;
import java.util.Map;

public class SpaceEcsWorker implements JobCompilationInterceptor {

  private Map<String, Object> ecsTaskConfig;

  public void setEcsTaskConfig(Map<String, Object> ecsTaskConfig) {
    this.ecsTaskConfig = ecsTaskConfig;
  }

  @Override
  public boolean chooseMe(Job job) {
    return    job.getProcess() instanceof Ecsosphere
           && job.getSource() instanceof DatasetDescription.Space;
  }

  @Override
  public CompilationStepGraph compile(Job job) {
    DatasetDescription.Space source = (DatasetDescription.Space) job.getSource();
    DatasetDescription.Space target = (DatasetDescription.Space) job.getTarget();

    return compile(job.getId(), source, target, null, ecsTaskConfig);
  }

  public static CompilationStepGraph compile(String jobId, DatasetDescription.Space source, DatasetDescription.Space target,
      Map<String, String> outputMetadata, Map<String, Object> ecsTaskConfig ) {

    String sourceSpaceId = source.getId();
    //String targetSpaceId = target.getId();
    Filters filters = source.getFilters();

    if (source.getVersionRef() == null)
      throw new CompilationError("The source versionRef may not be null.");

    SpaceContext sourceContext, targetContext;
    StatisticsResponse sourceStatistics, targetStatistics;
    try {

      Ref versionRef = source.getVersionRef();
          /*resolvedVersionRef = hubWebClient().resolveRef(sourceSpaceId, sourceContext, versionRef) */

      EcsTaskStep ecsTaskStep = new EcsTaskStep().withAdditionalEcsInfo("some additonal information for ecs processing");

      ecsTaskStep.setEcsTaskConfig(ecsTaskConfig);

      CompilationStepGraph cGraph = new CompilationStepGraph();
      cGraph.addExecution(ecsTaskStep);


      return cGraph;

    }
    catch (Exception e) {
      throw new CompilationError("Error resolving job and resource information: " + e.getMessage(), e);
    }
  }
}
