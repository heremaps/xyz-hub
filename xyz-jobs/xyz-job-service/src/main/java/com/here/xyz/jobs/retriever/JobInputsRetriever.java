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

import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.util.pagination.Page;
import com.here.xyz.util.pagination.PagedDataRetriever;

import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

public class JobInputsRetriever implements PagedDataRetriever<Input, JobInputsRetriever.InputsParams> {

  private final String jobId;

  public JobInputsRetriever(String jobId) {
    this.jobId = jobId;
  }

  @Override
  public List<Input> getItems(InputsParams params) {
    return Input.loadInputs(jobId, params.getSetName());
  }

  @Override
  public Page<Input> getPage(InputsParams params, int pageSize, String pageToken) {
    return Input.loadInputs(jobId, params.getSetName(), pageToken, pageSize);
  }

  public static class InputsParams {

    private String setName = "default";

    public String getSetName() {
      return setName;
    }

    public InputsParams setSetName(String setName) {
      this.setName = setName;
      return this;
    }
  }
}
