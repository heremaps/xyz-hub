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

package com.here.xyz.jobs.steps.outputs;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.xyz.Typed;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@JsonSubTypes({
    @JsonSubTypes.Type(value = DownloadUrl.class, name = "DownloadUrl"),
    @JsonSubTypes.Type(value = ModelBasedOutput.class, name = "ModelBasedOutput")
})
public abstract class Output<T extends Output> implements Typed {
  public static final String MODEL_BASED_PREFIX = "/modelBased";
  @JsonAnySetter
  private Map<String, String> metadata;
  @JsonIgnore
  private String s3Key;

  public abstract void store(String s3Key) throws IOException;

  public static String stepOutputS3Prefix(String jobId, String stepId, boolean userOutput, boolean onlyModelBased) {
    return stepOutputS3Prefix(jobId + "/" + stepId, userOutput, onlyModelBased);
  }

  public static String stepOutputS3Prefix(String stepS3Prefix, boolean userOutput, boolean onlyModelBased) {
    return stepS3Prefix + "/outputs" + (userOutput ? "/user" : "/system") + (onlyModelBased ? MODEL_BASED_PREFIX : "");
  }

  @JsonAnyGetter
  public Map<String, String> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public T withMetadata(Map<String, String> metadata) {
    setMetadata(metadata);
    return (T) this;
  }

  public String getS3Key() {
    return s3Key;
  }

  public void setS3Key(String s3Key) {
    this.s3Key = s3Key;
  }

  public T withS3Key(String s3Key) {
    setS3Key(s3Key);
    return (T) this;
  }

  public T withMetadata(String key, String value) {
    if (getMetadata() == null)
      setMetadata(new HashMap<>());
    getMetadata().put(key, value);
    return (T) this;
  }

  @JsonIgnore
  protected boolean hasMetadata() {
    return false;
  }
}
