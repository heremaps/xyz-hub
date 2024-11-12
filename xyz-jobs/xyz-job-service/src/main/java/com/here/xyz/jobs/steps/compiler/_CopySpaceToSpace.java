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
import com.here.xyz.jobs.datasets.filters.Filters;
import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.impl.transport._CopySpace;
import com.here.xyz.models.hub.Ref;

public class _CopySpaceToSpace implements JobCompilationInterceptor {

  @Override
  public boolean chooseMe(Job job) {
    return job.getSource() instanceof DatasetDescription.Space && job.getTarget() instanceof DatasetDescription.Space;
  }

  @Override
  public CompilationStepGraph compile(Job job) {
    final String sourceSpaceId = job.getSource().getKey();
    final String targetSpaceId = job.getTarget().getKey();

    Filters filters = ((DatasetDescription.Space) job.getSource()).getFilters();
    Ref versionRef = ((DatasetDescription.Space) job.getSource()).getVersionRef();

    _CopySpace copySpaceStep = new _CopySpace()
            .withSpaceId(sourceSpaceId)
            .withTargetSpaceId(targetSpaceId)
            .withSourceVersionRef(versionRef);

    if(filters != null) {
      //filters.context is not supported
      //TODO: work with propertiesQueryObject
      //copySpaceStep.setPropertyFilter(filters.getPropertyFilter());

      SpatialFilter spatialFilter = filters.getSpatialFilter();
      if (spatialFilter != null) {
        copySpaceStep.setGeometry(spatialFilter.getGeometry());
        copySpaceStep.setRadius(spatialFilter.getRadius());
        copySpaceStep.setClipOnFilterGeometry(spatialFilter.isClip());
      }
    }

    return (CompilationStepGraph) new CompilationStepGraph()
            .addExecution(copySpaceStep);
  }
}
