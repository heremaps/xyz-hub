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

package com.here.xyz.jobs.steps.compiler;

import static com.here.xyz.jobs.steps.impl.imp.ImportFilesToSpace.Format.CSV_GEOJSON;
import static com.here.xyz.jobs.steps.impl.imp.ImportFilesToSpace.Format.CSV_JSONWKB;
import static com.here.xyz.jobs.steps.impl.imp.ImportFilesToSpace.Format.GEOJSON;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.Index.VIZ;

import com.google.common.collect.Lists;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.datasets.files.Csv;
import com.here.xyz.jobs.datasets.files.FileFormat;
import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.JobCompiler.CompilationError;
import com.here.xyz.jobs.steps.StepExecution;
import com.here.xyz.jobs.steps.impl.AnalyzeSpaceTable;
import com.here.xyz.jobs.steps.impl.CreateIndex;
import com.here.xyz.jobs.steps.impl.DropIndexes;
import com.here.xyz.jobs.steps.impl.MarkForMaintenance;
import com.here.xyz.jobs.steps.impl.imp.ImportFilesToSpace;
import com.here.xyz.jobs.steps.impl.imp.ImportFilesToSpace.Format;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper.Index;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ImportFromFiles implements JobCompilationInterceptor {

  @Override
  public boolean chooseMe(Job job) {
    return job.getSource() instanceof Files && job.getTarget() instanceof DatasetDescription.Space;
  }

  @Override
  public CompilationStepGraph compile(Job job) {
    String spaceId = job.getTarget().getKey();
    //NOTE: VIZ index will be created separately in a sequential step afterwards (see below)
    List<Index> indices = Stream.of(Index.values()).filter(index -> index != VIZ).toList();
    //Split the work in two parallel tasks for now
    List<List<Index>> indexTasks = Lists.partition(indices, indices.size() / 2);

    final FileFormat sourceFormat = ((Files) job.getSource()).getInputSettings().getFormat();
    Format importStepFormat;
    if (sourceFormat instanceof GeoJson)
      importStepFormat = GEOJSON;
    else if (sourceFormat instanceof Csv csvFormat)
      importStepFormat = csvFormat.isGeometryAsExtraWkbColumn() ? CSV_JSONWKB : CSV_GEOJSON;
    else
      throw new CompilationError("Unsupported import file format: " + sourceFormat.getClass().getSimpleName());

    return (CompilationStepGraph) new CompilationStepGraph()
        .addExecution(new DropIndexes().withSpaceId(spaceId)) //Drop all existing indices
        .addExecution(new ImportFilesToSpace() //Perform import
            .withSpaceId(spaceId)
            .withFormat(importStepFormat))
        //NOTE: Create *all* indices in parallel, make sure to (at least) keep the viz-index sequential #postgres-issue-with-partitions
        .addExecution(new CompilationStepGraph() //Create all the base indices semi-parallel
            .addExecution(new CompilationStepGraph().withExecutions(toSequentialSteps(spaceId, indexTasks.get(0))))
            .addExecution(new CompilationStepGraph().withExecutions(toSequentialSteps(spaceId, indexTasks.get(1))))
            .withParallel(true))
        .addExecution(new CreateIndex().withIndex(VIZ).withSpaceId(spaceId))
        .addExecution(new AnalyzeSpaceTable().withSpaceId(spaceId))
        .addExecution(new MarkForMaintenance().withSpaceId(spaceId));
  }

  private static List<StepExecution> toSequentialSteps(String spaceId, List<Index> indices) {
    return indices.stream().map(index -> new CreateIndex().withIndex(index).withSpaceId(spaceId)).collect(Collectors.toList());
  }
}
