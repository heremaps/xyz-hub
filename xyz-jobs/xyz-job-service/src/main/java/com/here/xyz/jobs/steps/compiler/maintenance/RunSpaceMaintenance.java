/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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
import com.here.xyz.jobs.datasets.DatasetDescription.Space;
import com.here.xyz.jobs.processes.Maintain;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.JobCompiler;
import com.here.xyz.jobs.steps.compiler.JobCompilationInterceptor;
import com.here.xyz.jobs.steps.compiler.tools.IndexCompilerHelper;
import com.here.xyz.jobs.steps.impl.DropIndexes;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.impl.maintenance.SpawnMaintenanceJobs;
import com.here.xyz.jobs.steps.impl.transport.ExtractJsonPathValues;
import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.util.db.pg.IndexHelper.Index;
import com.here.xyz.util.db.pg.IndexHelper.SystemIndex;
import java.util.List;
import java.util.stream.Stream;

import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RunSpaceMaintenance implements JobCompilationInterceptor {
  private static final Logger logger = LogManager.getLogger();

  @Override
  public boolean chooseMe(Job job) {
    return job.getProcess() instanceof Maintain && Space.class.equals(job.getSource().getClass());
  }

  @Override
  public CompilationStepGraph compile(Job job) {
    return (CompilationStepGraph) new CompilationStepGraph()
        .addExecution(compile((Space) job.getSource()));
  }

  public static CompilationStepGraph compile(Space source) {
    CompilationStepGraph stepGraph = new CompilationStepGraph();

    //White-list on-demand indexes based on active searchable properties and all the system indexes
    List<Index> whiteList = Stream.concat(Stream.of(SystemIndex.values()),
            IndexCompilerHelper.getActiveSearchableProperties(source.getId()).stream()).toList();

    //Drop indices which are not in the whitelist
    DropIndexes dropIndexes = new DropIndexes()
            .withSpaceDeactivation(false)
            .withSpaceId(source.getId())
            .withIndexWhiteList(whiteList);

    stepGraph.addExecution(dropIndexes);

    // Extract JSONPath values into "searchable" column
    if (getTableLayout(source.getId()) != ConnectorParameters.TableLayout.OLD_LAYOUT) {
      ExtractJsonPathValues extractJsonPathValues = new ExtractJsonPathValues()
              .withSpaceId(source.getId());

      stepGraph.addExecution(extractJsonPathValues);
    }

    //Create all indices that are defined - existing ones are getting skipped
    CompilationStepGraph onDemandIndexSteps = IndexCompilerHelper.compileOnDemandIndexSteps(source.getId());

    if (!onDemandIndexSteps.isEmpty())
      stepGraph.addExecution(onDemandIndexSteps);

    stepGraph.addExecution(new SpawnMaintenanceJobs().withSpaceId(source.getId()));

    return stepGraph;
  }

  private static ConnectorParameters.TableLayout getTableLayout(String spaceId){
    try{
      String connectorClass = HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadConnector(loadSpace(spaceId).getStorage().getId()).remoteFunction.className;
      return SpaceBasedStep.getTableLayoutFromConnectorClass(connectorClass);
    } catch (XyzWebClient.WebClientException e) {
      throw new JobCompiler.CompilationError("Unable to resolve Table-Layout for: " + spaceId, e);
    }
  }

  private static com.here.xyz.models.hub.Space loadSpace(String spaceId) throws JobCompiler.CompilationError {
    try {
      return HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadSpace(spaceId);
    }
    catch (XyzWebClient.WebClientException e) {
      throw new JobCompiler.CompilationError("Unable to resolve target: " + spaceId, e);
    }
  }
}
