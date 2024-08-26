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

package com.here.xyz.jobs.steps.inputs;

import java.io.IOException;
import java.util.List;

public class InputsFromJob extends Input<InputsFromJob> {
  private String jobId;

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public InputsFromJob withJobId(String jobId) {
    setJobId(jobId);
    return this;
  }

  /**
   * De-references this delegator object by loading the inputs of the referenced job and writes the metadata
   * for the referencing job.
   *
   * Additionally, this method adds bidirectional references between the metadata objects of the two involved jobs.
   * That is necessary to prevent the deletion of the referenced inputs if they're still in use.
   *
   * @param referencingJobId The job that owns this delegator object
   * @throws IOException when the metadata for the referenced job could not be updated
   */
  public void dereference(String referencingJobId) throws IOException {
    //First load the inputs of the other job to ensure the other job's metadata actually have been written
    List<Input> inputs = Input.loadInputs(getJobId());
    updateInputMetaReferences(referencingJobId);
    //Store the metadata of the job that references the other job's metadata
    storeMetadata(referencingJobId, inputs, getJobId());
  }

  private void updateInputMetaReferences(String referencingJobId) throws IOException {
    InputsMetadata referencedMetadata = loadMetadata(getJobId());
    //Add the referencing job to the list of jobs referencing the metadata
    referencedMetadata.referencingJobs().add(referencingJobId);
    storeMetadata(getJobId(), referencedMetadata);
  }
}
