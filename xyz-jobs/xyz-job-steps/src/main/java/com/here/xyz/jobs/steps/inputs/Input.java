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

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.util.S3Client;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@JsonSubTypes({
    @JsonSubTypes.Type(value = UploadUrl.class, name = "UploadUrl"),
    @JsonSubTypes.Type(value = InputsFromJob.class, name = "InputsFromJob")
})
public abstract class Input <T extends Input> implements Typed {
  private static final Logger logger = LogManager.getLogger();
  protected long byteSize;
  protected boolean compressed;
  @JsonIgnore
  private String s3Key;
  private static Map<String, List<Input>> inputsCache = new WeakHashMap<>();
  private static Set<String> submittedJobs = new HashSet<>();

  public static String inputS3Prefix(String jobId) {
    return jobId + "/inputs";
  }

  private static String inputMetaS3Key(String jobId) {
    return jobId + "/meta/inputs.json";
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
        inputs = loadInputsAndWriteMetadata(jobId);
        inputsCache.put(jobId, inputs);
      }
      return inputs;
    }
    return loadInputsAndWriteMetadata(jobId);
  }

  private static List<Input> loadInputsAndWriteMetadata(String jobId) {
    try {
      InputsMetadata metadata = loadMetadata(jobId);
      return metadata.inputs.entrySet().stream()
          .map(metaEntry -> createInput(metaEntry.getKey(), metaEntry.getValue().byteSize, metaEntry.getValue().compressed))
          .toList();
    }
    catch (IOException | AmazonS3Exception ignore) {}

    final List<Input> inputs = loadInputsInParallel(jobId);
    //Only write metadata of jobs which are submitted already
    if (inputs != null && submittedJobs.contains(jobId))
      storeMetadata(jobId, inputs);

    return inputs;
  }

  static InputsMetadata loadMetadata(String jobId) throws IOException {
    InputsMetadata metadata = XyzSerializable.deserialize(S3Client.getInstance().loadObjectContent(inputMetaS3Key(jobId)),
        InputsMetadata.class);
    return metadata;
  }

  static void storeMetadata(String jobId, InputsMetadata metadata) {
    try {
      S3Client.getInstance().putObject(inputMetaS3Key(jobId), "application/json", metadata.serialize());
    }
    catch (IOException e) {
      logger.error("Error writing inputs metadata file for job {}.", jobId, e);
      //NOTE: Next call to this method will try it again
    }
  }

  private static void storeMetadata(String jobId, List<Input> inputs) {
    storeMetadata(jobId, inputs, null);
  }

  static void storeMetadata(String jobId, List<Input> inputs, String referencedJobId) {
    logger.info("Storing inputs metadata for job {} ...", jobId);
    Map<String, InputMetadata> metadata = inputs.stream()
        .collect(Collectors.toMap(input -> input.s3Key, input -> new InputMetadata(input.byteSize, input.compressed)));
    storeMetadata(jobId, new InputsMetadata(metadata, Set.of(jobId), referencedJobId));
  }

  private static List<Input> loadInputsInParallel(String jobId) {
    logger.info("Scanning inputs for job {} ...", jobId);
    ForkJoinPool tmpPool = new ForkJoinPool(10);
    List<Input> inputs = null;
    try {
      inputs = tmpPool.submit(() -> loadAndTransformInputs(jobId, -1, Input.class)).get();
    }
    catch (InterruptedException | ExecutionException ignore) {}
    finally {
      tmpPool.shutdown();
    }
    return inputs;
  }

  public static int currentInputsCount(String jobId, Class<? extends Input> inputType) {
    return (int) loadInputs(jobId).stream().filter(input -> inputType.isAssignableFrom(input.getClass())).count();
  }

  public static <T extends Input> List<T> loadInputsSample(String jobId, int maxSampleSize, Class<T> inputType) {
    return loadAndTransformInputs(jobId, maxSampleSize, inputType);
  }

  private static <T extends Input> List<T> loadAndTransformInputs(String jobId, int maxReturnSize, Class<T> inputType) {
    Stream<Input> inputsStream = S3Client.getInstance().scanFolder(Input.inputS3Prefix(jobId))
        .parallelStream()
        .map(s3ObjectSummary -> createInput(s3ObjectSummary.getKey(), s3ObjectSummary.getSize(), inputIsCompressed(s3ObjectSummary.getKey())))
        .filter(input -> inputType.isAssignableFrom(input.getClass()));

    if (maxReturnSize > 0)
      inputsStream = inputsStream.unordered().limit(maxReturnSize);

    return (List<T>) inputsStream.collect(Collectors.toList());
  }

  public static ModelBasedInput resolveRawInput(Map<String, Object> rawInput) {
    if (rawInput == null)
      throw new NullPointerException("The raw input may not be null");
    //TODO: Support resolving InputReferences here
    return XyzSerializable.fromMap(rawInput, ModelBasedInput.class);
  }

  public static void deleteInputs(String jobId) {
    deleteInputs(jobId, jobId);
  }

  private static void deleteInputs(String owningJobId, String referencingJob) {
    InputsMetadata metadata = null;
    try {
      metadata = loadMetadata(owningJobId);
      metadata.referencingJobs().remove(referencingJob);
    }
    catch (IOException ignore) {}

    //Only delete the inputs if no other job is referencing them anymore
    if (metadata == null || metadata.referencingJobs().isEmpty()) {
      if (metadata != null && metadata.referencedJob() != null)
        /*
        The owning job referenced the inputs of another job, remove the owning job from the list of referencing jobs
        and check whether the referenced inputs may be deleted now.
         */
        deleteInputs(metadata.referencedJob(), owningJobId);

      S3Client.getInstance().deleteFolder(inputS3Prefix(owningJobId));
    }
    else if (metadata != null)
      storeMetadata(owningJobId, metadata);
  }

  private static Input createInput(String s3Key, long byteSize, boolean compressed) {
    //TODO: Support ModelBasedInputs
    return new UploadUrl()
        .withS3Key(s3Key)
        .withByteSize(byteSize)
        .withCompressed(compressed);
  }

  private static boolean inputIsCompressed(String s3Key) {
    //TODO: Check compression in another way (e.g. file suffix?)
    ObjectMetadata metadata = S3Client.getInstance().loadMetadata(s3Key);
    return metadata.getContentEncoding() != null && metadata.getContentEncoding().equalsIgnoreCase("gzip");
  }

  public static void registerSubmittedJob(String jobId) {
    submittedJobs.add(jobId);
  }

  @JsonIgnore
  public long getByteSize() {
    return byteSize;
  }

  public void setByteSize(long byteSize) {
    this.byteSize = byteSize;
  }

  public T withByteSize(long byteSize) {
    setByteSize(byteSize);
    return (T) this;
  }

  @JsonIgnore
  public boolean isCompressed() {
    return compressed;
  }

  public void setCompressed(boolean compressed) {
    this.compressed = compressed;
  }

  public T withCompressed(boolean compressed) {
    setCompressed(compressed);
    return (T) this;
  }

  public record InputMetadata(@JsonProperty long byteSize, @JsonProperty boolean compressed) {}
  public record InputsMetadata(@JsonProperty Map<String, InputMetadata> inputs, @JsonProperty Set<String> referencingJobs,
                               @JsonProperty String referencedJob) implements XyzSerializable {}
}
