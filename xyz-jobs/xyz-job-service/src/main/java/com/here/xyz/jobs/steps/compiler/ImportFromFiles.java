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

import com.google.common.collect.Lists;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.datasets.DatasetDescription.Space;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.datasets.files.Csv;
import com.here.xyz.jobs.datasets.files.FileFormat;
import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.JobCompiler.CompilationError;
import com.here.xyz.jobs.steps.StepExecution;
import com.here.xyz.jobs.steps.compiler.tools.IndexCompilerHelper;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.impl.AnalyzeSpaceTable;
import com.here.xyz.jobs.steps.impl.CreateIndex;
import com.here.xyz.jobs.steps.impl.DropIndexes;
import com.here.xyz.jobs.steps.impl.MarkForMaintenance;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.EntityPerLine;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.Format;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper.SystemIndex;
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
  public static Set<Class<? extends Space>> allowedTargetTypes = new HashSet<>(Set.of(Space.class));

  @Override
  public boolean chooseMe(Job job) {
    return job.getProcess() == null && job.getSource() instanceof Files files && isSupportedFormat(files)
        && allowedTargetTypes.contains(job.getTarget().getClass());
  }

  private boolean isSupportedFormat(Files files) {
    return files.getInputSettings().getFormat() instanceof GeoJson || files.getInputSettings().getFormat() instanceof Csv;
  }

  @Override
  public CompilationStepGraph compile(Job job) {
    Space target = (Space) job.getTarget();
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

  public static CompilationStepGraph compileWrapWithDropRecreateIndices(String spaceId, StepExecution stepExecution )
  {
    //NOTE: drop/create indices is also used by SpaceCopy compiler

    //NOTE: VIZ index will be created separately in a sequential step afterwards (see below)
    List<XyzSpaceTableHelper.SystemIndex> indices = Stream.of(SystemIndex.values()).filter(index -> index != SystemIndex.VIZ).toList();
    //Split the work in three parallel tasks for now
    List<List<SystemIndex>> indexTasks = Lists.partition(indices, indices.size() / 3);

    CompilationStepGraph wrappedStepGraph = (CompilationStepGraph) new CompilationStepGraph()
        .addExecution(new DropIndexes().withSpaceId(spaceId).withSpaceDeactivation(true)) //Drop all existing indices
        .addExecution(stepExecution)
        //NOTE: Create *all* indices in parallel, make sure to (at least) keep the viz-index sequential #postgres-issue-with-partitions
        .addExecution(new CompilationStepGraph() //Create all the base indices semi-parallel
            .addExecution(new CompilationStepGraph().withExecutions(toSequentialSteps(spaceId, indexTasks.get(0))))
            .addExecution(new CompilationStepGraph().withExecutions(toSequentialSteps(spaceId, indexTasks.get(1))))
            .addExecution(new CompilationStepGraph().withExecutions(toSequentialSteps(spaceId, indexTasks.get(2))))
            .withParallel(true))
        .addExecution(new CreateIndex().withIndex(SystemIndex.VIZ).withSpaceId(spaceId));

    CompilationStepGraph onDemandIndexSteps = IndexCompilerHelper.compileOnDemandIndexSteps(spaceId);
    if (!onDemandIndexSteps.isEmpty())
      wrappedStepGraph.addExecution(onDemandIndexSteps);

    wrappedStepGraph.addExecution(new AnalyzeSpaceTable().withSpaceId(spaceId));

    return wrappedStepGraph;

  }

  public static CompilationStepGraph compileImportSteps(ImportFilesToSpace importFilesStep) {
    return compileWrapWithDropRecreateIndices(importFilesStep.getSpaceId(),importFilesStep );
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


}
