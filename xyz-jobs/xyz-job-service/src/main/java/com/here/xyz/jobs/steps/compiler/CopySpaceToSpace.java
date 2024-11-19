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

import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.filters.Filters;
import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.JobCompiler;
import com.here.xyz.jobs.steps.impl.transport.CopySpace;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.WebClientException;

public class CopySpaceToSpace implements JobCompilationInterceptor {
  
  protected boolean validSubType( String subType )
  { return "Space".equals(subType); }

  @Override
  public boolean chooseMe(Job job) {
    return    job.getSource() instanceof DatasetDescription.Space
           && validSubType( job.getSource().getClass().getSimpleName() )
           && job.getTarget() instanceof DatasetDescription.Space
           && validSubType( job.getTarget().getClass().getSimpleName() );
  }



  @Override
  public CompilationStepGraph compile(Job job) {
    final String sourceSpaceId = job.getSource().getKey();
    final String targetSpaceId = job.getTarget().getKey();


    StatisticsResponse sourceStatistics = null, targetStatistics = null;
    try {
      sourceStatistics = HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadSpaceStatistics(sourceSpaceId);
      targetStatistics = HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadSpaceStatistics(targetSpaceId);
    } catch (WebClientException e) {
      String errMsg = String.format("Unable to get Staistics for %s", sourceStatistics != null ? sourceSpaceId : targetSpaceId );
      throw new JobCompiler.CompilationError(errMsg);
    }

    long sourceFeatureCount = sourceStatistics.getCount().getValue(),
         targetFeatureCount = targetStatistics.getCount().getValue();

    int PARALLELIZTATION_MIN_THRESHOLD = 10000, PARALLELIZTATION_THREAD_COUNT = 3,
        threadCount = sourceFeatureCount > PARALLELIZTATION_MIN_THRESHOLD ? PARALLELIZTATION_THREAD_COUNT : 1;

    Filters filters = ((DatasetDescription.Space<?>) job.getSource()).getFilters();
    Ref versionRef = ((DatasetDescription.Space<?>) job.getSource()).getVersionRef();

    SpatialFilter spatialFilter = filters != null ? filters.getSpatialFilter() : null;
    PropertiesQuery propertyFilter = filters != null ? filters.getPropertyFilter() : null;

    CompilationStepGraph cGraph = new CompilationStepGraph();
    
    for( int threadId = 0; threadId < threadCount; threadId++)
    {
      CopySpace copySpaceStep = new CopySpace()
              .withSpaceId(sourceSpaceId)
              .withTargetSpaceId(targetSpaceId)
              .withSourceVersionRef(versionRef)
              .withPropertyFilter(propertyFilter)
              .withThreadInfo(new int[]{ threadId, threadCount });

      if (spatialFilter != null) {
        copySpaceStep.setGeometry(spatialFilter.getGeometry());
        copySpaceStep.setRadius(spatialFilter.getRadius());
        copySpaceStep.setClipOnFilterGeometry(spatialFilter.isClip());
      }

      cGraph.addExecution(copySpaceStep).withParallel(true);
    }

    return cGraph;
  }
}
