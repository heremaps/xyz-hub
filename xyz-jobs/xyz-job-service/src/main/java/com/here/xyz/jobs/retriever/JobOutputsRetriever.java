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
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.util.pagination.Page;
import com.here.xyz.util.pagination.PagedDataRetriever;

import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implements the {@link PagedDataRetriever} interface for retrieving job outputs with pagination. This class handles the pagination of
 * outputs across all steps in a job.
 */
public class JobOutputsRetriever implements PagedDataRetriever<Output, JobOutputsRetriever.OutputsParams> {

  private final Job job;

  public JobOutputsRetriever(Job job) {
    this.job = job;
  }

  @Override
  public List<Output> getItems(OutputsParams params) {
    return job.getSteps().stepStream()
        .map(step -> (List<Output>) step.loadUserOutputs())
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  @Override
  public Page<Output> getPage(OutputsParams params, int pageSize, String pageToken) {
    List<Output> allOutputs = getItems(params);

    int totalItems = allOutputs.size();
    int startIndex = 0;

    if (pageToken != null && !pageToken.isEmpty()) {
      startIndex = decodePageToken(pageToken);
    }

    if (startIndex >= totalItems) {
      return new Page<Output>().setItems(List.of()).setTotalItems(totalItems);
    }

    int endIndex = Math.min(startIndex + pageSize, totalItems);

    List<Output> pageItems = allOutputs.subList(startIndex, endIndex);

    String nextPageToken = null;
    if (endIndex < totalItems) {
      nextPageToken = encodePageToken(endIndex);
    }

    return new Page<Output>()
        .setItems(pageItems)
        .setNextPageToken(nextPageToken)
        .setTotalItems(totalItems);
  }

  private String encodePageToken(int index) {
    return Base64.getEncoder().encodeToString(String.valueOf(index).getBytes());
  }

  private int decodePageToken(String pageToken) {
    try {
      String decoded = new String(Base64.getDecoder().decode(pageToken));
      return Integer.parseInt(decoded);
    } catch (Exception e) {
      return 0;
    }
  }

  public static class OutputsParams {
    // add filters here
  }
}
