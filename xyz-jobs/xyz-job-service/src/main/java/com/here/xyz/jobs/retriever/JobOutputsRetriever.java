/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.jobs.retriever;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.util.pagination.Page;
import com.here.xyz.util.pagination.PagedDataRetriever;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JobOutputsRetriever implements PagedDataRetriever<Output, JobOutputsRetriever.OutputsParams> {
  private static final Logger logger = LogManager.getLogger();
  private final Job job;

  public JobOutputsRetriever(Job job) {
    this.job = job;
  }

  @Override
  public List<Output> getItems(OutputsParams params) {
    return job.getSteps().stepStream()
        .map(step -> (List<Output>) step.loadUserOutputs())
        .flatMap(ol -> ol.stream())
        .collect(Collectors.toList());
  }

  @Override
  public Page<Output> getPage(OutputsParams params, int limit, String nextPageToken) {
    logger.info("Loading outputs for job {} with group {} and name {} by token [{}] with limit [{}]", job.getId(), params.outputSetGroup, params.setName, nextPageToken, limit);
    Step<?> step = job.getSteps().getStepOrNull(params.outputSetGroup, params.setName);

    if (step == null) {
      throw new IllegalArgumentException("Nothing was found by requested group and name");
    }

    return step.loadUserOutputsPage(params.setName, limit, nextPageToken);
  }


  public static class OutputsParams {

    public final String outputSetGroup;
    public final String setName;

    public OutputsParams() {
      this(null, null);
    }

    public OutputsParams(String outputSetGroup, String setName) {
      this.outputSetGroup = outputSetGroup;
      this.setName = setName;
    }
  }
}