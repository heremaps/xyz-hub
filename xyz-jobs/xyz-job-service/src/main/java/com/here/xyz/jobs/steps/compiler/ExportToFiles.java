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
import com.here.xyz.jobs.datasets.filters.Filters;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.JobCompiler;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.web.HubWebClient;

import java.util.HashSet;
import java.util.Map;
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
    return compileSteps(source.getId(), job.getId(), source.getFilters(), source.getVersionRef(), null);
  }

  public static CompilationStepGraph compileSteps(String spaceId, String jobId, Filters filters, Ref versionRef, Map<String, String> outputMetadata) {
    return (CompilationStepGraph) new CompilationStepGraph()
            .addExecution(compileExportStep(spaceId, jobId, filters, versionRef, false, true, outputMetadata));
  }

  public static ExportSpaceToFiles compileExportStep(String spaceId, String jobId, Filters filters, Ref versionRef, boolean useSystemOutput, boolean addStatistics, Map<String, String> outputMetadata) {
    versionRef = resolveVersionRef(spaceId, versionRef);
    return new ExportSpaceToFiles()
                    .withSpaceId(spaceId)
                    .withJobId(jobId)
                    .withSpatialFilter(filters != null ? filters.getSpatialFilter() : null)
                    .withPropertyFilter(filters != null ? filters.getPropertyFilter() : null)
                    .withContext(filters != null ? filters.getContext() : null)
                    .withVersionRef(versionRef)
                    .withUseSystemOutput(useSystemOutput)
                    .withAddStatisticsToUserOutput(addStatistics)
                    .withOutputMetadata(outputMetadata);
  }

  public static Ref resolveVersionRef(String spaceId, Ref versionRef) {
    if(versionRef == null || versionRef.isRange())
      return versionRef;

    try {
      Long resolvedVersion = null;
      if (versionRef.isHead()) {
        StatisticsResponse statisticsResponse = HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadSpaceStatistics(spaceId);
        resolvedVersion = statisticsResponse.getMaxVersion().getValue();
      } else if (versionRef.isTag()) {
        long version = HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadTag(spaceId, versionRef.getTag()).getVersion();
        if (version >= 0) {
          resolvedVersion = version;
        }
      }
      return resolvedVersion == null ? versionRef : new Ref(resolvedVersion);
    } catch (Exception e) {
      throw new JobCompiler.CompilationError("Unable to resolve '" + versionRef + "' VersionRef!");
    }
  }
}
