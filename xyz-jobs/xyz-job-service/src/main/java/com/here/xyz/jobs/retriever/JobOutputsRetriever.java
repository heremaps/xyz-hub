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

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

public class JobOutputsRetriever implements PagedDataRetriever<Output, JobOutputsRetriever.OutputsParams> {

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
    PaginationState state = parsePaginationToken(nextPageToken);

    List<Output> pageItems = new ArrayList<>();
    List<String> stepIds = getOrderedStepIds(params.outputSetGroup);
    int collected = 0;

    int currentStepIndex = state.stepIndex;
    String currentStepToken = state.stepToken;

    while (currentStepIndex < stepIds.size() && collected < limit) {
      String stepId = stepIds.get(currentStepIndex);
      Step<?> step = job.getSteps().getStep(stepId);

      if (step == null) {
        currentStepIndex++;
        currentStepToken = null;
        continue;
      }

      int remainingLimit = limit - collected;

      Page<Output> stepOutputsPage = step.loadUserOutputsPage(remainingLimit, currentStepToken);
      List<Output> stepOutputs = stepOutputsPage.getItems();

      pageItems.addAll(stepOutputs);
      collected += stepOutputs.size();

      if (stepOutputsPage.getNextPageToken() != null) {
        currentStepToken = stepOutputsPage.getNextPageToken();
        break; // the limit within this step reached
      } else {
        // this step is fully processed, move to next step
        currentStepIndex++;
        currentStepToken = null;
      }
    }

    String nextToken = null;
    if (currentStepIndex < stepIds.size() ||
        (currentStepIndex < stepIds.size() && currentStepToken != null)) {
      nextToken = createPaginationToken(currentStepIndex, currentStepToken);
    }

    return new Page<>(pageItems, nextToken);
  }

  private List<String> getOrderedStepIds(String outputSetGroup) {
    return job.getSteps().stepStream()
        .filter(step -> outputSetGroup.equals(step.getOutputSetGroup()))
        .map(Step::getId)
        .toList();
  }

  private PaginationState parsePaginationToken(String token) {
    if (token == null || token.isEmpty()) {
      return new PaginationState(0, null);
    }

    try {
      String decoded = new String(Base64.getDecoder().decode(token));
      String[] parts = decoded.split(":", 2);
      if (parts.length >= 1) {
        int stepIndex = Integer.parseInt(parts[0]);
        String stepToken = parts.length > 1 && !parts[1].isEmpty() ? parts[1] : null;
        return new PaginationState(stepIndex, stepToken);
      }
    } catch (Exception e) {
      // invalid token, start from beginning
    }

    return new PaginationState(0, null);
  }

  private String createPaginationToken(int stepIndex, String stepToken) {
    String tokenData = stepIndex + ":" + (stepToken != null ? stepToken : "");
    return Base64.getEncoder().encodeToString(tokenData.getBytes());
  }

  private static class PaginationState {

    final int stepIndex;
    final String stepToken;

    PaginationState(int stepIndex, String stepToken) {
      this.stepIndex = stepIndex;
      this.stepToken = stepToken;
    }
  }

  public static class OutputsParams {

    public final String outputSetGroup;
    public final String setName;

    public OutputsParams(String outputSetGroup, String setName) {
      this.outputSetGroup = outputSetGroup;
      this.setName = setName;
    }
  }
}