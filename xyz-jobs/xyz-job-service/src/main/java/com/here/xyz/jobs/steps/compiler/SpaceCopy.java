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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.jobs.steps.impl.transport.CopySpacePre.VERSION;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.filters.Filters;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.JobCompiler.CompilationError;
import com.here.xyz.jobs.steps.Step.InputSet;
import com.here.xyz.jobs.steps.impl.transport.CopySpace;
import com.here.xyz.jobs.steps.impl.transport.CopySpacePost;
import com.here.xyz.jobs.steps.impl.transport.CopySpacePre;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SpaceCopy implements JobCompilationInterceptor {

  public static Set<Class> forbiddenTargetTypes = new HashSet<>();

  @Override
  public boolean chooseMe(Job job) {
    return job.getProcess() == null && job.getSource() instanceof DatasetDescription.Space
        && job.getTarget() instanceof DatasetDescription.Space && !forbiddenTargetTypes.contains(job.getTarget().getClass());
  }

  private static int threadCountCalc(long sourceFeatureCount, long targetFeatureCount) {
    long PARALLELIZTATION_THRESHOLD = 100000;
    int PARALLELIZTATION_THREAD_MAX = 8;

    if (sourceFeatureCount <= 1 * PARALLELIZTATION_THRESHOLD)
      return 1;
    if (sourceFeatureCount <= 3 * PARALLELIZTATION_THRESHOLD)
      return 2;
    if (sourceFeatureCount <= 12 * PARALLELIZTATION_THRESHOLD)
      return 3;
    if (sourceFeatureCount <= 24 * PARALLELIZTATION_THRESHOLD)
      return 6;
    return PARALLELIZTATION_THREAD_MAX;
  }

  private static HubWebClient hubWebClient() {
    return HubWebClient.getInstance(Config.instance.HUB_ENDPOINT);
  }

  @Override
  public CompilationStepGraph compile(Job job) {
    DatasetDescription.Space source = (DatasetDescription.Space) job.getSource();
    DatasetDescription.Space target = (DatasetDescription.Space) job.getTarget();

    return compile(job.getId(), source, target);
  }

  public static CompilationStepGraph compile(String jobId, DatasetDescription.Space source, DatasetDescription.Space target) {
    return compile(jobId, source, target, null);
  }

  public static CompilationStepGraph compile(String jobId, DatasetDescription.Space source, DatasetDescription.Space target,
      Map<String, String> outputMetadata) {
    String sourceSpaceId = source.getId();
    String targetSpaceId = target.getId();
    Filters filters = source.getFilters();

    if (source.getVersionRef() == null)
      throw new CompilationError("The source versionRef may not be null.");

    SpaceContext sourceContext, targetContext;
    StatisticsResponse sourceStatistics, targetStatistics;
    try {
      //TODO: Parallelize statistics loading (or get rid of it completely in compiler by merging the 3 copy steps into one)
      sourceContext = hubWebClient().loadSpace(sourceSpaceId).getExtension() != null ? EXTENSION : null;
      targetContext = hubWebClient().loadSpace(targetSpaceId).getExtension() != null ? EXTENSION : null;
      sourceStatistics = hubWebClient().loadSpaceStatistics(sourceSpaceId, sourceContext);
      targetStatistics = hubWebClient().loadSpaceStatistics(targetSpaceId, targetContext);

      Ref versionRef = source.getVersionRef(),
          resolvedVersionRef = hubWebClient().resolveRef(sourceSpaceId, sourceContext, versionRef);

      CopySpacePre preCopySpace = new CopySpacePre().withSpaceId(targetSpaceId).withJobId(jobId);

      CompilationStepGraph startGraph = new CompilationStepGraph();
      startGraph.addExecution(preCopySpace);

      long sourceFeatureCount = sourceStatistics.getCount().getValue(),
          targetFeatureCount = targetStatistics.getCount().getValue();

      int threadCount = threadCountCalc(sourceFeatureCount, targetFeatureCount);

      CompilationStepGraph copyGraph = new CompilationStepGraph();

      for (int threadId = 0; threadId < threadCount; threadId++) {

        CopySpace copySpaceStep = new CopySpace()
            .withSpaceId(sourceSpaceId)
            .withTargetSpaceId(targetSpaceId)
            .withSourceVersionRef(resolvedVersionRef)
            .withPropertyFilter(filters != null ? filters.getPropertyFilter() : null)
            .withSpatialFilter(filters != null ? filters.getSpatialFilter() : null)
            .withThreadInfo(new int[]{threadId, threadCount})
            .withJobId(jobId)
            .withInputSets(List.of(new InputSet(preCopySpace.getOutputSet(VERSION))));

        copyGraph.addExecution(copySpaceStep).withParallel(true);
      }

      if(copyGraph.size() == 0)
       throw new CompilationError("None of provided Layer contains a valid sourceVersion" );

      startGraph.addExecution(copyGraph);

      if (outputMetadata == null)
        outputMetadata = Map.of(target.getClass().getSimpleName().toLowerCase(), targetSpaceId);

      CopySpacePost postCopySpace = new CopySpacePost()
          .withSpaceId(targetSpaceId)
          .withJobId(jobId)
          .withOutputMetadata(outputMetadata)
          .withInputSets(List.of(new InputSet(preCopySpace.getOutputSet(VERSION))));

      startGraph.addExecution(postCopySpace);

      return startGraph;
    }
    catch (WebClientException e) {
      throw new CompilationError("Error resolving resource information: " + e.getMessage(), e);
    }
  }
}
