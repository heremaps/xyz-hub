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

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.xyz.Typed;
import com.here.xyz.jobs.util.S3Client;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@JsonSubTypes({
    @JsonSubTypes.Type(value = UploadUrl.class, name = "UploadUrl")
})
public abstract class Input <T extends Input> implements Typed {
  @JsonIgnore
  private String s3Key;
  private static Map<String, List<Input>> inputsCache = new WeakHashMap<>(); //TODO: Expire keys after <24h
  private static Set<String> submittedJobs = new HashSet<>();

  public static String inputS3Prefix(String jobId) {
    return jobId + "/inputs";
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

  public static List<Input> loadInputs(String jobId) {
    //Only cache inputs of jobs which are submitted already
    if (submittedJobs.contains(jobId)) {
      List<Input> inputs = inputsCache.get(jobId);
      if (inputs == null) {
        inputs = loadInputsInParallel(jobId);
        inputsCache.put(jobId, inputs);
      }
      return inputs;
    }
    return loadInputsInParallel(jobId);
  }

  private static List<Input> loadInputsInParallel(String jobId) {
    ForkJoinPool tmpPool = new ForkJoinPool(10);
    List<Input> inputs = null;
    try {
      inputs = tmpPool.submit(() -> loadAndTransformInputs(jobId, -1)).get();
    }
    catch (InterruptedException | ExecutionException ignore) {}
    tmpPool.shutdown();
    return inputs;
  }

  public static int currentInputsCount(String jobId, Class<? extends Input> inputType) {
    //TODO: Support ModelBasedInputs
    return S3Client.getInstance().scanFolder(Input.inputS3Prefix(jobId)).size();
  }

  public static <T extends Input> List<T> loadInputsSample(String jobId, int maxSampleSize, Class<T> inputType) {
    //TODO: Support ModelBasedInputs
    return (List<T>) loadAndTransformInputs(jobId, maxSampleSize);
  }

  private static List<Input> loadAndTransformInputs(String jobId, int maxReturnSize) {
    //TODO: Support ModelBasedInputs
    Stream<Input> inputsStream = S3Client.getInstance().scanFolder(Input.inputS3Prefix(jobId))
        .parallelStream()
        .map(s3ObjectSummary -> new UploadUrl()
            .withS3Key(s3ObjectSummary.getKey())
            .withByteSize(s3ObjectSummary.getSize())
            .withCompressed(inputIsCompressed(s3ObjectSummary.getKey())));

    if (maxReturnSize > 0)
      inputsStream = inputsStream.unordered().limit(maxReturnSize);

    return inputsStream.collect(Collectors.toList());
  }

  private static boolean inputIsCompressed(String s3Key) {
    //TODO: Check compression in another way (e.g. file suffix?)
    ObjectMetadata metadata = S3Client.getInstance().loadMetadata(s3Key);
    return metadata.getContentEncoding() != null && metadata.getContentEncoding().equalsIgnoreCase("gzip");
  }

  public static void registerSubmittedJob(String jobId) {
    submittedJobs.add(jobId);
  }
}
