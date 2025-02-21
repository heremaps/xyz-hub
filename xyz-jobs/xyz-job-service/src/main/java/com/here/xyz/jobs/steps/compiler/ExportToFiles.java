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
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import java.util.Map;

public class ExportToFiles implements JobCompilationInterceptor {
  @Override
  public boolean chooseMe(Job job) {
    return job.getProcess() == null && Space.class.equals(job.getSource().getClass()) && job.getTarget() instanceof Files targetFiles
        && targetFiles.getOutputSettings().getFormat() instanceof GeoJson;
  }

  @Override
  public CompilationStepGraph compile(Job job) {
    return (CompilationStepGraph) new CompilationStepGraph()
        .addExecution(compile(job.getId(), (Space) job.getSource()));
  }

  public static ExportSpaceToFiles compile(String jobId, Space source) {
    return new ExportSpaceToFiles()
        .withSpaceId(source.getId())
        .withJobId(jobId)
        .withSpatialFilter(source.getFilters() != null ? source.getFilters().getSpatialFilter() : null)
        .withPropertyFilter(source.getFilters() != null ? source.getFilters().getPropertyFilter() : null)
        .withContext(source.getFilters() != null ? source.getFilters().getContext() : null)
        .withVersionRef(source.getVersionRef())
        .withOutputMetadata(Map.of(source.getClass().getSimpleName().toLowerCase(), source.getId()));
  }
}
