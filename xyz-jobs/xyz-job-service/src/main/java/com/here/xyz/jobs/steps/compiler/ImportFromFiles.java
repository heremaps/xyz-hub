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

import static com.here.xyz.jobs.steps.Step.InputSet.USER_INPUTS;
import static com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.Format.CSV_GEOJSON;
import static com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.Format.CSV_JSON_WKB;
import static com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.Format.GEOJSON;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SystemIndex;

import com.google.common.collect.Lists;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.datasets.files.Csv;
import com.here.xyz.jobs.datasets.files.FileFormat;
import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.JobCompiler.CompilationError;
import com.here.xyz.jobs.steps.StepExecution;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.impl.AnalyzeSpaceTable;
import com.here.xyz.jobs.steps.impl.CreateIndex;
import com.here.xyz.jobs.steps.impl.DropIndexes;
import com.here.xyz.jobs.steps.impl.MarkForMaintenance;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.EntityPerLine;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.Format;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.ErrorResponseException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ImportFromFiles implements JobCompilationInterceptor {
  public static Set<Class<? extends DatasetDescription.Space>> allowedTargetTypes = new HashSet<>(Set.of(DatasetDescription.Space.class));

  @Override
  public boolean chooseMe(Job job) {
    return job.getProcess() == null && job.getSource() instanceof Files && allowedTargetTypes.contains(job.getTarget().getClass());
  }

  @Override
  public CompilationStepGraph compile(Job job) {
    DatasetDescription.Space target = (DatasetDescription.Space) job.getTarget();
    String spaceId = target.getId();

    final FileFormat sourceFormat = ((Files) job.getSource()).getInputSettings().getFormat();
    Format importStepFormat;
    if (sourceFormat instanceof GeoJson)
      importStepFormat = GEOJSON;
    else if (sourceFormat instanceof Csv csvFormat)
      importStepFormat = csvFormat.isGeometryAsExtraWkbColumn() ? CSV_JSON_WKB : CSV_GEOJSON;
    else
      throw new CompilationError("Unsupported import file format: " + sourceFormat.getClass().getSimpleName());

    ImportFilesToSpace importFilesStep = new ImportFilesToSpace() //Perform import
        .withSpaceId(spaceId)
        .withFormat(importStepFormat)
        .withEntityPerLine(getEntityPerLine(sourceFormat))
        .withJobId(job.getId())
        .withInputSets(List.of(USER_INPUTS.get()));

    //This validation check is necessary to deliver a constructive error to the user - otherwise keepIndices will throw a runtime error.
    checkIfSpaceIsAccessible(spaceId);

    if (importFilesStep.getExecutionMode().equals(LambdaBasedStep.ExecutionMode.SYNC) || importFilesStep.keepIndices())
      //Perform only the import Step
      return (CompilationStepGraph) new CompilationStepGraph()
          .addExecution(importFilesStep);

    //perform full Import with all 11 Steps (IDX deletion/creation..)
    return compileImportSteps(importFilesStep);
  }

  public static CompilationStepGraph compileImportSteps(ImportFilesToSpace importFilesStep) {
    String spaceId = importFilesStep.getSpaceId();

    //NOTE: VIZ index will be created separately in a sequential step afterwards (see below)
    List<XyzSpaceTableHelper.SystemIndex> indices = Stream.of(SystemIndex.values()).filter(index -> index != SystemIndex.VIZ).toList();
    //Split the work in three parallel tasks for now
    List<List<SystemIndex>> indexTasks = Lists.partition(indices, indices.size() / 3);

    CompilationStepGraph importStepGraph = (CompilationStepGraph) new CompilationStepGraph()
        .addExecution(new DropIndexes().withSpaceId(spaceId)) //Drop all existing indices
            // TODO: remove this step in the future, when the maintenance-service got shut down.
        .addExecution(new MarkForMaintenance().withSpaceId(spaceId).withIdxCreationCompleted(true))
        .addExecution(importFilesStep)
        //NOTE: Create *all* indices in parallel, make sure to (at least) keep the viz-index sequential #postgres-issue-with-partitions
        .addExecution(new CompilationStepGraph() //Create all the base indices semi-parallel
            .addExecution(new CompilationStepGraph().withExecutions(toSequentialSteps(spaceId, indexTasks.get(0))))
            .addExecution(new CompilationStepGraph().withExecutions(toSequentialSteps(spaceId, indexTasks.get(1))))
            .addExecution(new CompilationStepGraph().withExecutions(toSequentialSteps(spaceId, indexTasks.get(2))))
            .withParallel(true))
        .addExecution(new CreateIndex().withIndex(SystemIndex.VIZ).withSpaceId(spaceId));

    CompilationStepGraph onDemandIndexSteps = compileOnDemandIndexSteps(spaceId);
    if (!onDemandIndexSteps.isEmpty())
      importStepGraph.addExecution(onDemandIndexSteps);

    importStepGraph.addExecution(new AnalyzeSpaceTable().withSpaceId(spaceId));
    //TODO: remove this step in the future, when the maintenance-service got shut down.
    importStepGraph.addExecution(new MarkForMaintenance().withSpaceId(spaceId).withIdxCreationCompleted(false));

    return importStepGraph;
  }

  private EntityPerLine getEntityPerLine(FileFormat format) {
    return EntityPerLine.valueOf((format instanceof GeoJson geoJson
        ? geoJson.getEntityPerLine()
        : ((Csv) format).getEntityPerLine()).toString());
  }

  private static List<StepExecution> toSequentialSteps(String spaceId, List<SystemIndex> indices) {
    return indices.stream().map(index -> new CreateIndex().withIndex(index).withSpaceId(spaceId)).collect(Collectors.toList());
  }

  private void checkIfSpaceIsAccessible(String spaceId) throws CompilationError {
    try {
      HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadSpaceStatistics(spaceId);
    }
    catch (WebClientException e) {
      if (e instanceof ErrorResponseException err && err.getStatusCode() == 428)
        throw new CompilationError("Target Layer is deactivated!");
      throw new CompilationError("Target is not accessible!" + e.getMessage());
    }
  }

  /**
   * Creates a CompilationStepGraph for the on-demand indices of the given space.
   * @param spaceId The ID of the space for which to create the on-demand indices.
   * @return A CompilationStepGraph containing the steps to create the on-demand indices, which are getting executed in parallel.
   * @throws CompilationError If an error occurs while retrieving the searchable properties.
   */
  private static CompilationStepGraph compileOnDemandIndexSteps(String spaceId) throws CompilationError {

    Map<String, Boolean> activatedSearchableProperties = getActivatedSearchableProperties(spaceId);
    CompilationStepGraph onDemandIndicesGraph = (CompilationStepGraph) new CompilationStepGraph().withParallel(true);

    for(String property : activatedSearchableProperties.keySet()) {
      if (activatedSearchableProperties.get(property)) {
        // Create an OnDemandIndex step for each activated searchable property
        onDemandIndicesGraph.addExecution(new CreateIndex()
            .withIndex(new XyzSpaceTableHelper.OnDemandIndex().withPropertyPath(property))
            .withSpaceId(spaceId));
      }
    }
    return onDemandIndicesGraph;
  }

  /**
   * Retrieves the activated searchable properties for the given space ID.
   * @param spaceId The ID of the space for which to retrieve the searchable properties.
   * @return A map containing the activated searchable properties and their values.
   * @throws CompilationError If an error occurs while retrieving the searchable properties.
   */
  private static Map<String, Boolean> getActivatedSearchableProperties(String spaceId) throws CompilationError {
    try {
      Map<String, Boolean> searchableProperties = HubWebClient.getInstance(Config.instance.HUB_ENDPOINT)
              .loadSpace(spaceId).getSearchableProperties();

      return searchableProperties == null ? Map.of() : searchableProperties.entrySet().stream()
              .filter(Map.Entry::getValue)
              .collect(Collectors.toMap(
                      Map.Entry::getKey,
                      Map.Entry::getValue
              ));
    }
    catch (WebClientException e) {
      throw new CompilationError("Target is not accessible! "+ e.getMessage());
    }
  }
}
