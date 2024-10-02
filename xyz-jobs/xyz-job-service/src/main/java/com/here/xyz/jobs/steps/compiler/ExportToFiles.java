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
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.Files;

import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;

import java.util.HashSet;
import java.util.Set;

public class ExportToFiles implements JobCompilationInterceptor {
  public static Set<Class<? extends DatasetDescription.Space>> allowedSourceTypes = new HashSet<>(Set.of(DatasetDescription.Space.class));

  @Override
  public boolean chooseMe(Job job) {
    return job.getTarget() instanceof Files
            && allowedSourceTypes.contains(job.getSource().getClass())
            && ((Files<?>) job.getTarget()).getOutputSettings().getFormat() instanceof GeoJson;
  }

  @Override
  public CompilationStepGraph compile(Job job) {
    DatasetDescription.Space source = (DatasetDescription.Space) job.getSource();
    String spaceId = source.getId();

    ExportSpaceToFiles exportToFilesStep = new ExportSpaceToFiles() //Perform import
        .withSpaceId(spaceId)
        .withJobId(job.getId());

    return compileImportSteps(exportToFilesStep);
  }

  public static CompilationStepGraph compileImportSteps(ExportSpaceToFiles exportToFilesStep) {
    return (CompilationStepGraph) new CompilationStepGraph()
        .addExecution(exportToFilesStep);
  }
}
