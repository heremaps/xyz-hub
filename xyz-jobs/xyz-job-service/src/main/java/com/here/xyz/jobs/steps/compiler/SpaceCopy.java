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

import java.util.Map;
import java.util.Set;

import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.filters.Filters;
import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.JobCompiler;
import com.here.xyz.jobs.steps.impl.transport.CopySpace;
import com.here.xyz.jobs.steps.impl.transport.CopySpacePre;
import com.here.xyz.jobs.steps.impl.transport.CopySpacePost;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.WebClientException;

public class SpaceCopy implements JobCompilationInterceptor {
  
  protected boolean validSubType( String subType )
  { return "Space".equals(subType); }

  @Override
  public boolean chooseMe(Job job) {
    return    job.getSource() instanceof DatasetDescription.Space
           && validSubType( job.getSource().getClass().getSimpleName() )
           && job.getTarget() instanceof DatasetDescription.Space
           && validSubType( job.getTarget().getClass().getSimpleName() );
  }

  private static int threadCountCalc( long sourceFeatureCount, long targetFeatureCount )
  {
    long PARALLELIZTATION_THRESHOLD = 100000;
    int PARALLELIZTATION_THREAD_MAX = 8;
    
    if( sourceFeatureCount <=  1 * PARALLELIZTATION_THRESHOLD ) return 1;
    if( sourceFeatureCount <=  3 * PARALLELIZTATION_THRESHOLD ) return 2;
    if( sourceFeatureCount <= 12 * PARALLELIZTATION_THRESHOLD ) return 3;
    if( sourceFeatureCount <= 24 * PARALLELIZTATION_THRESHOLD ) return 6;
    return PARALLELIZTATION_THREAD_MAX; 
  }

 
  private static StatisticsResponse _loadSpaceStatistics(String spaceId) throws WebClientException
  {
   Space sourceSpace = HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadSpace(spaceId);
   boolean isExtended = sourceSpace.getExtension() != null;
   return HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadSpaceStatistics(spaceId, isExtended ? SpaceContext.EXTENSION : null);  
  }

  public static CompilationStepGraph compileSteps(String sourceId, String targetId, String jobId, Filters filters, Ref versionRef, String targetType) 
  {
    final String sourceSpaceId = sourceId,
                 targetSpaceId = targetId;

    StatisticsResponse sourceStatistics = null, targetStatistics = null;
    try {
      sourceStatistics = _loadSpaceStatistics(sourceSpaceId);
      targetStatistics = _loadSpaceStatistics(targetSpaceId);
    } catch (WebClientException e) {
      String errMsg = String.format("Unable to get Staistics for %s", sourceStatistics == null ? sourceSpaceId : targetSpaceId );
      throw new JobCompiler.CompilationError(errMsg);
    }

    CopySpacePre preCopySpace = new CopySpacePre().withSpaceId(targetSpaceId).withJobId(jobId);

    CompilationStepGraph startGraph = new CompilationStepGraph();
    startGraph.addExecution(preCopySpace); 

    long sourceFeatureCount = sourceStatistics.getCount().getValue(),
         targetFeatureCount = targetStatistics.getCount().getValue();

    int threadCount = threadCountCalc(sourceFeatureCount, targetFeatureCount);

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
              .withThreadInfo(new int[]{ threadId, threadCount })
              .withJobId(jobId);

      if (spatialFilter != null) {
        copySpaceStep.setGeometry(spatialFilter.getGeometry());
        copySpaceStep.setRadius(spatialFilter.getRadius());
        copySpaceStep.setClipOnFilterGeometry(spatialFilter.isClip());
      }

      cGraph.addExecution(copySpaceStep).withParallel(true);
    }

    startGraph.addExecution(cGraph);

    CopySpacePost postCopySpace = new CopySpacePost().withSpaceId(targetSpaceId)
                                                     .withJobId(jobId)
                                                     .withOutputMetadata(Map.of(targetType, targetSpaceId));

    startGraph.addExecution(postCopySpace);

    return startGraph;
 
  }

  @Override
  public CompilationStepGraph compile(Job job) {

    return compileSteps(job.getSource().getKey(), 
                        job.getTarget().getKey(), 
                        job.getId(),
                        ((DatasetDescription.Space<?>) job.getSource()).getFilters(),
                        ((DatasetDescription.Space<?>) job.getSource()).getVersionRef(),
                        "Space" );

  }
}
