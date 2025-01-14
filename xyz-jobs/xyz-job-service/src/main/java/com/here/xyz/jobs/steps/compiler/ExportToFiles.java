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

import static com.here.xyz.jobs.steps.Step.Visibility.USER;
import static com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles.STATISTICS;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.datasets.DatasetDescription.Space;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.datasets.filters.Filters;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.Step.Visibility;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.models.hub.Ref;
import java.util.Map;

public class ExportToFiles implements JobCompilationInterceptor {
  @Override
  public boolean chooseMe(Job job) {
    return job.getProcess() == null && job.getTarget() instanceof Files targetFiles
        && job.getSource().getClass().getSimpleName().equals(Space.class.getSimpleName())
        && targetFiles.getOutputSettings().getFormat() instanceof GeoJson;
  }

  @Override
  public CompilationStepGraph compile(Job job) {
    Space source = (Space) job.getSource();
    return compileSteps(source.getId(), job.getId(), source.getFilters(), source.getVersionRef(), Map.of("space", source.getId()));
  }

  public static CompilationStepGraph compileSteps(String spaceId, String jobId, Filters filters, Ref versionRef,
      Map<String, String> outputMetadata) {
    return (CompilationStepGraph) new CompilationStepGraph()
        .addExecution(compileExportStep(spaceId, jobId, filters, versionRef, USER, outputMetadata));
  }

  public static ExportSpaceToFiles compileExportStep(String spaceId, String jobId, Filters filters, Ref versionRef,
      Visibility statisticsVisibility, Map<String, String> outputMetadata) {
    return new ExportSpaceToFiles()
        .withSpaceId(spaceId)
        .withJobId(jobId)
        .withSpatialFilter(filters != null ? filters.getSpatialFilter() : null)
        .withPropertyFilter(filters != null ? filters.getPropertyFilter() : null)
        .withContext(filters != null ? filters.getContext() : null)
        .withVersionRef(versionRef)
        .withOutputSetVisibility(STATISTICS, statisticsVisibility)
        .withOutputMetadata(outputMetadata);
  }
}
