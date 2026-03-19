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

import static com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles.EXPORTED_DATA;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.DatasetDescription.Space;
import com.here.xyz.jobs.processes.CopyViaFiles;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.JobCompiler.CompilationError;
import com.here.xyz.jobs.steps.Step.InputSet;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.ErrorResponseException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.util.List;
import java.util.Map;

/**
 * Compiler that exports data from a source space to files and then imports those files into a target space
 * using {@link TaskedImportFilesToSpace}.
 *
 * <p>This compiler is chosen when both source and target are {@link Space} instances and the job has no
 * process description set, and the source space is different from the target space.</p>
 *
 * <p>The compiled step graph is sequential:
 * <ol>
 *   <li>Export from the source space using {@link ExportSpaceToFiles}</li>
 *   <li>Import into the target space using {@link TaskedImportFilesToSpace}, consuming the exported files</li>
 * </ol>
 * </p>
 */
public class ExportToFilesAndImport implements JobCompilationInterceptor {

/* Jobconfiguration pattern :
 {
  "description": "ExportToFilesAndImport Test",
  "process": { "type": "CopyViaFiles" },
  "source": { "type": "Space", "id": "testExpImp-Source-01" },
  "target": { "type": "Space", "id": "testExpImp-Target-01" }
 }
*/

  @Override
  public boolean chooseMe(Job job) {
    return job.getProcess() instanceof CopyViaFiles
        && job.getSource() instanceof Space
        && job.getTarget() instanceof Space;
  }


  public static CompilationStepGraph compile(String jobId, DatasetDescription.Space source, DatasetDescription.Space target, String layerType )
  {
    String sourceSpaceId = source.getId();
    String targetSpaceId = target.getId();

    checkIfSpaceIsAccessible(sourceSpaceId);
    checkIfSpaceIsAccessible(targetSpaceId);

    Map<String, String> sourceMeta = java.util.Map.of( layerType == null ? source.getClass().getSimpleName().toLowerCase() : layerType , sourceSpaceId),
                        targetMeta = java.util.Map.of( layerType == null ? target.getClass().getSimpleName().toLowerCase() : layerType , targetSpaceId);

    // Step 1: Export from source space to files
    ExportSpaceToFiles exportStep = ExportToFiles.compile(source).withOutputMetadata(sourceMeta);

    // Step 2: Import the exported files into the target space, consuming the export's output
    TaskedImportFilesToSpace importStep = new TaskedImportFilesToSpace()
        .withSpaceId(targetSpaceId)
        .withVersionRef(new Ref(Ref.HEAD))
        .withOutputMetadata(targetMeta)
        .withInputSets(List.of(new InputSet(exportStep.getOutputSet(EXPORTED_DATA))));

    // Build a sequential graph: export first, then import (with index drop/create)
    CompilationStepGraph graph = new CompilationStepGraph();
    graph.addExecution(exportStep);

    if (importStep.keepIndices()) {
      graph.addExecution(importStep);
    }
    else {
      graph.addExecution(ImportFromFiles.compileTaskedImportSteps(importStep));
    }

    return graph;
  }

  @Override
  public CompilationStepGraph compile(Job job) {

    return compile(job.getId(),(DatasetDescription.Space) job.getSource(),(DatasetDescription.Space) job.getTarget(),null);

  }

  private static void checkIfSpaceIsAccessible(String spaceId) throws CompilationError {
    try {
      HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadSpaceStatistics(spaceId);
    }
    catch (WebClientException e) {
      if (e instanceof ErrorResponseException err && err.getStatusCode() == 428)
        throw new CompilationError("Layer " + spaceId + " is deactivated!");
      throw new CompilationError("Layer " + spaceId + " is not accessible! " + e.getMessage());
    }
  }
}
