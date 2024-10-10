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

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.datasets.DatasetDescription.Space;
import com.here.xyz.jobs.datasets.Files;

import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.JobCompiler;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.web.HubWebClient;

import java.util.HashSet;
import java.util.Set;

public class ExportToFiles implements JobCompilationInterceptor {
  public static Set<Class<? extends Space>> allowedSourceTypes = new HashSet<>(Set.of(Space.class));

  @Override
  public boolean chooseMe(Job job) {
    return job.getTarget() instanceof Files
            && allowedSourceTypes.contains(job.getSource().getClass())
            && ((Files<?>) job.getTarget()).getOutputSettings().getFormat() instanceof GeoJson;
  }

  @Override
  public CompilationStepGraph compile(Job job) {
    Space source = (Space) job.getSource();
    resolveVersionRef(source);
    String spaceId = source.getId();

    ExportSpaceToFiles exportToFilesStep = new ExportSpaceToFiles()
        .withSpaceId(spaceId)
        .withJobId(job.getId())
        .withSpatialFilter(source.getFilters() != null ? source.getFilters().getSpatialFilter() : null)
        .withPropertyFilter(source.getFilters() != null ? source.getFilters().getPropertyFilter() : null)
        .withContext(source.getFilters() != null ? source.getFilters().getContext() : null)
        .withVersionRef(source.getVersionRef() != null ? source.getVersionRef() : null);

    return compileImportSteps(exportToFilesStep);
  }

  public static CompilationStepGraph compileImportSteps(ExportSpaceToFiles exportToFilesStep) {
    return (CompilationStepGraph) new CompilationStepGraph()
        .addExecution(exportToFilesStep);
  }

  private void resolveVersionRef(Space sourceSpace) {
    if(sourceSpace.getVersionRef() == null || sourceSpace.getVersionRef().isRange())
      return;

    try {
      if (sourceSpace.getVersionRef().isHead()) {
        StatisticsResponse statisticsResponse = HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadSpaceStatistics(sourceSpace.getId());
        sourceSpace.setVersionRef(new Ref(statisticsResponse.getMaxVersion().getValue()));
      } else if (sourceSpace.getVersionRef().isTag()) {
        long version = HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadTag(sourceSpace.getId(), sourceSpace.getVersionRef().getTag()).getVersion();
        if (version >= 0) {
          sourceSpace.setVersionRef(new Ref(version));
        }
      }
    }catch (Exception e) {
      throw new JobCompiler.CompilationError("Unable to resolve '" + sourceSpace.getVersionRef() + "' VersionRef!");
    }
  }
}
