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

package com.here.xyz.jobs.steps.inputs;

import com.here.xyz.util.service.aws.S3Uri;
import java.util.List;

public class InputsFromS3 extends Input<InputsFromS3> {
  private String bucket;
  private String prefix;

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    if (prefix.startsWith("/"))
      prefix = prefix.substring(1);
    this.prefix = prefix;
  }

  public InputsFromS3 withPrefix(String prefix) {
    setPrefix(prefix);
    return this;
  }

  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public InputsFromS3 withBucket(String bucket) {
    setBucket(bucket);
    return this;
  }

  public void dereference(String forJob, String setName) {
    //First load the inputs from the (foreign) bucket
    List<Input> inputs = loadInputsInParallel(getBucket(), getPrefix());
    inputs.forEach(input -> input.setS3Bucket(getBucket()));
    //Store the metadata for the job that accesses the bucket
    storeMetadata(forJob, inputs, null, new S3Uri(getBucket(), getPrefix()), setName);
  }
}
