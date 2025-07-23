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

import static com.here.xyz.jobs.util.S3Client.getBucketFromS3Uri;
import static com.here.xyz.jobs.util.S3Client.getKeyFromS3Uri;

import com.amazonaws.AmazonServiceException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.payloads.StepPayload;
import com.here.xyz.jobs.util.S3Client;
import com.here.xyz.util.pagination.Page;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.aws.s3.S3ObjectSummary;
import com.here.xyz.util.service.aws.s3.S3Uri;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

@JsonSubTypes({
    @JsonSubTypes.Type(value = UploadUrl.class, name = "UploadUrl"),
    @JsonSubTypes.Type(value = InputsFromJob.class, name = "InputsFromJob"),
    @JsonSubTypes.Type(value = InputsFromS3.class, name = "InputsFromS3")
})
public abstract class Input <T extends Input> extends StepPayload<T> {
  private static final Logger logger = LogManager.getLogger();
  protected long byteSize;
  protected boolean compressed;
  @JsonIgnore
  private String s3Bucket;
  @JsonIgnore
  private String s3Key;
  private static Map<String, Map<String, InputsMetadata>> metadataCache = new WeakHashMap<>();
  private static Map<String, Map<String, List<Input>>> inputsCache = new WeakHashMap<>(); //TODO: Expire keys after <24h
  private static Set<String> inputsCacheActive = new HashSet<>();

  public static String inputS3Prefix(String jobId) {
    return jobId + "/inputs";
  }

  public static String inputS3Prefix(String jobId, String setName) {
    return jobId + "/inputs/" + setName;
  }

  private static String inputMetaS3Prefix(String jobId) {
    return jobId + "/meta";
  }

  private static String inputMetaS3Key(String jobId, String setName) {
    return inputMetaS3Prefix(jobId) + "/" + setName + ".json";
  }

  public static String defaultBucket() {
    return Config.instance.JOBS_S3_BUCKET;
  }

  public String getS3Bucket() {
    if (s3Bucket == null)
      return defaultBucket();
    return s3Bucket;
  }

  public void setS3Bucket(String s3Bucket) {
    this.s3Bucket = s3Bucket;
  }

  public T withS3Bucket(String s3Bucket) {
    setS3Bucket(s3Bucket);
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

  public static Page<Input> loadInputs(String jobId, String setName) {
    return loadInputs(jobId, setName, null, -1);
  }

  public static Page<Input> loadInputs(String jobId, String setName, String nextPageToken, int limit) {
    //Only cache inputs of jobs which are submitted already
    if (inputsCacheActive.contains(jobId) && nextPageToken == null && limit == -1) {
      List<Input> inputs = getFromInputCache(jobId, setName);
      if (inputs == null) {
        inputs = loadInputsAndWriteMetadata(jobId, setName, limit, nextPageToken, Input.class);
        putToInputCache(jobId, setName, inputs);
      }
      return inputs;
    }
    return loadInputsAndWriteMetadata(jobId, setName, limit, nextPageToken, Input.class);
  }

  private synchronized static void putToInputCache(String jobId, String setName, List<Input> inputs) {
    Map<String, List<Input>> cachedInputs = inputsCache.get(jobId);
    if (cachedInputs == null)
      cachedInputs = new ConcurrentHashMap<>();
    cachedInputs.put(setName, inputs);
    inputsCache.put(jobId, cachedInputs);
  }

  private static List<Input> getFromInputCache(String jobId, String setName) {
    Map<String, List<Input>> inputs = inputsCache.get(jobId);
    return inputs == null ? null: inputs.get(setName);
  }

  private synchronized static void putToMetadataCache(String jobId, String setName, InputsMetadata metadata) {
    Map<String, InputsMetadata> cachedMetadata = metadataCache.get(jobId);
    if (cachedMetadata == null)
      cachedMetadata = new ConcurrentHashMap<>();
    cachedMetadata.put(setName, metadata);
    metadataCache.put(jobId, cachedMetadata);
  }

  private static InputsMetadata getFromMetadataCache(String jobId, String setName) {
    Map<String, InputsMetadata> metadata = metadataCache.get(jobId);
    return metadata == null ? null : metadata.get(setName);
  }

  private static <T extends Input> Page<T> loadInputsAndWriteMetadata(String jobId, String setName, int maxReturnSize, Class<T> inputType) {
    return loadInputsAndWriteMetadata(jobId, setName, maxReturnSize, null, inputType);
  }

  private static <T extends Input> Page<T> loadInputsAndWriteMetadata(String jobId, String setName, int maxReturnSize, String nextPageToken, Class<T> inputType) {
    try {
      InputsMetadata metadata = loadMetadata(jobId, setName);
      Stream<T> inputs = metadata.inputs.entrySet().stream()
          .filter(input -> input.getValue().byteSize > 0)
          .map(metaEntry -> {
            final String metaKey = metaEntry.getKey();
            String s3Bucket = getBucketFromS3Uri(metaKey);
            String s3Key;
            if (s3Bucket != null)
              s3Key = getKeyFromS3Uri(metaKey);
            else
              s3Key = metaKey;
            return (T) createInput(s3Bucket, s3Key, metaEntry.getValue().byteSize, metaEntry.getValue().compressed);
          });

      // Apply pagination if nextPageToken is provided
      if (nextPageToken != null) {
        // Skip elements until we reach the token position
        long skipCount = Long.parseLong(nextPageToken);
        inputs = inputs.skip(skipCount);
      }

      return (maxReturnSize > 0 ? inputs.unordered().limit(maxReturnSize) : inputs).toList();
    }
    catch (IOException | S3Exception ignore) {}

    final Page<T> inputs = loadInputsInParallel(defaultBucket(), inputS3Prefix(jobId, setName), maxReturnSize, nextPageToken, inputType);
    //Only write metadata of jobs which are submitted already
    if (inputs != null && inputs.size() > 0 && inputsCacheActive.contains(jobId) && nextPageToken == null)
      storeMetadata(jobId, (List<Input>) inputs, setName);

    return inputs;
  }

  public static final S3Uri loadResolvedUserInputPrefixUri(String jobId, String setName) {
    Optional<InputsMetadata> userInputsMetadata = loadMetadataIfExists(jobId, setName);
    if (userInputsMetadata.isPresent())
      return userInputsMetadata.get().scannedFrom;
    return new S3Uri(defaultBucket(), inputS3Prefix(jobId, setName));
  }

  static List<String> loadAllInputSetNames(String jobId) {
    return S3Client.getInstance().scanFolder(inputMetaS3Prefix(jobId)).stream()
        .map(s3ObjectSummary -> s3ObjectSummary.key().substring(0, s3ObjectSummary.key().lastIndexOf(".json")))
        .toList();
  }

  private static Optional<InputsMetadata> loadMetadataIfExists(String jobId, String setName) {
    try {
      return Optional.of(loadMetadata(jobId, setName));
    }
    catch (IOException | S3Exception e) {
      return Optional.empty();
    }
  }

  static final InputsMetadata loadMetadata(String jobId, String setName) throws IOException, S3Exception {
    InputsMetadata metadata = getFromMetadataCache(jobId, setName);
    if (metadata != null)
      return metadata;

    logger.info("Loading metadata from S3 for job {} ...", jobId);
    long t1 = Core.currentTimeMillis();
    metadata = XyzSerializable.deserialize(S3Client.getInstance().loadObjectContent(inputMetaS3Key(jobId, setName)),
        InputsMetadata.class);
    logger.info("Loaded metadata for job {}. Took {}ms ...", jobId, Core.currentTimeMillis() - t1);
    if (inputsCacheActive.contains(jobId))
      putToMetadataCache(jobId, setName, metadata);

    return metadata;
  }

  static final void addInputReferences(String referencedJobId, String referencingJobId, String setName) throws IOException,
          S3Exception {
    InputsMetadata referencedMetadata = loadMetadata(referencedJobId, setName);
    //Add the referencing job to the list of jobs referencing the metadata
    referencedMetadata.referencingJobs().add(referencingJobId);
    storeMetadata(referencedJobId, referencedMetadata, setName);
  }

  static final void storeMetadata(String jobId, InputsMetadata metadata, String setName) {
    try {
      if (inputsCacheActive.contains(jobId))
        putToMetadataCache(jobId, setName, metadata);
      S3Client.getInstance().putObject(inputMetaS3Key(jobId, setName), "application/json", metadata.serialize());
    }
    catch (IOException e) {
      logger.error("Error writing inputs metadata file for job {}.", jobId, e);
      //NOTE: Next call to this method will try it again
    }
  }

  private static void storeMetadata(String jobId, List<Input> inputs, String setName) {
    storeMetadata(jobId, inputs, null, setName);
  }

  static final void storeMetadata(String jobId, List<Input> inputs, String referencedJobId, String setName) {
    storeMetadata(jobId, inputs, referencedJobId, new S3Uri(defaultBucket(), inputS3Prefix(jobId, setName)), setName);
  }

  static final void storeMetadata(String jobId, List<Input> inputs, String referencedJobId, S3Uri scannedFrom, String setName) {
    logger.info("Storing inputs metadata for job {} ...", jobId);
    Map<String, InputMetadata> metadata = inputs.stream()
        .collect(Collectors.toMap(input -> (input.s3Bucket == null ? "" : "s3://" + input.s3Bucket + "/") + input.s3Key,
            input -> new InputMetadata(input.byteSize, input.compressed)));
    storeMetadata(jobId, new InputsMetadata(metadata, new HashSet<>(Set.of(jobId)), referencedJobId, scannedFrom), setName);
  }

  static final List<Input> loadInputsInParallel(String bucketName, String inputS3Prefix) {
    return loadInputsInParallel(bucketName, inputS3Prefix, -1, Input.class);
  }

  static final <T extends Input> List<T> loadInputsInParallel(String bucketName, String inputS3Prefix, int maxReturnSize, Class<T> inputType) {
    return loadInputsInParallel(bucketName, inputS3Prefix, maxReturnSize, null, inputType);
  }

  static final <T extends Input> Page<T> loadInputsInParallel(String bucketName, String inputS3Prefix, int maxReturnSize, String nextPageToken, Class<T> inputType) {
    logger.info("Scanning inputs from bucket {} and prefix {} ...", bucketName, inputS3Prefix);
    long t1 = Core.currentTimeMillis();
    ForkJoinPool tmpPool = new ForkJoinPool(10);
    Page<T> inputs = null;
    try {
      inputs = tmpPool.submit(() -> loadAndTransformInputs(bucketName, inputS3Prefix, maxReturnSize, nextPageToken, inputType)).get();
    }
    catch (InterruptedException ignore) {}
    catch (ExecutionException e) {
      if(e.getCause() instanceof AmazonServiceException ase)
        throw new IllegalArgumentException("Unable to access the bucket resources. Reason: " + ase.getErrorMessage());
      else
        throw new RuntimeException(e);
    }
    finally {
      tmpPool.shutdown();
    }
    logger.info("Scanned {} inputs from bucket {} and prefix {}. Took {}ms.", inputs.size(), bucketName, inputS3Prefix, Core.currentTimeMillis() - t1);
    return inputs;
  }

  public static int currentInputsCount(String jobId, Class<? extends Input> inputType, String setName) {
    return (int) loadInputs(jobId, setName).getItems().stream().filter(input -> inputType.isAssignableFrom(input.getClass())).count();
  }

  public static <T extends Input> List<T> loadInputsSample(String jobId, String setName, int maxSampleSize, Class<T> inputType) {
    return loadInputsAndWriteMetadata(jobId, setName, maxSampleSize, inputType);
  }

  private static <T extends Input> Page<T> loadAndTransformInputs(String bucketName, String inputS3Prefix, int maxReturnSize, Class<T> inputType) {
    return loadAndTransformInputs(bucketName, inputS3Prefix, maxReturnSize, null, inputType);
  }

  private static <T extends Input> Page<T> loadAndTransformInputs(String bucketName, String inputS3Prefix, int maxReturnSize, String nextPageToken, Class<T> inputType) {
    Page<S3ObjectSummary> page = S3Client.getInstance(bucketName).scanFolder(inputS3Prefix, nextPageToken, maxReturnSize);
    Stream<Input> inputsStream = page.getItems()
        .parallelStream()
        .map(s3ObjectSummary -> createInput(defaultBucket().equals(bucketName) ? null : bucketName, s3ObjectSummary.key(),
            s3ObjectSummary.size(), inputIsCompressed(s3ObjectSummary)))
        .filter(input -> input.getByteSize() > 0 && inputType.isAssignableFrom(input.getClass()));

    if (maxReturnSize > 0 && nextPageToken == null)
      inputsStream = inputsStream.unordered().limit(maxReturnSize);

    return new Page<>((List<T>) inputsStream.collect(Collectors.toList()), page.getNextPageToken(), page.getTotalItems());
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
    //TODO: Parallelize
    loadAllInputSetNames(owningJobId).forEach(setName -> deleteInputs(owningJobId, referencingJob, setName));
  }

  private static void deleteInputs(String owningJobId, String referencingJob, String setName) {
    InputsMetadata metadata = null;
    try {
      metadata = loadMetadata(owningJobId, setName);
      metadata.referencingJobs().remove(referencingJob);
    }
    catch (S3Exception | IOException ignore) {}

    //Only delete the inputs if no other job is referencing them anymore
    if (metadata == null || metadata.referencingJobs().isEmpty()) {
      if (metadata != null && metadata.referencedJob() != null)
        /*
        The owning job referenced the inputs of another job, remove the owning job from the list of referencing jobs
        and check whether the referenced inputs may be deleted now.
         */
        deleteInputs(metadata.referencedJob(), owningJobId);

      S3Client.getInstance().deleteFolder(inputS3Prefix(owningJobId, setName));
    }
    else if (metadata != null)
      storeMetadata(owningJobId, metadata, setName);
  }

  private static Input createInput(String s3Bucket, String s3Key, long byteSize, boolean compressed) {
    //TODO: Support ModelBasedInputs
    return new UploadUrl()
        .withS3Bucket(s3Bucket)
        .withS3Key(s3Key)
        .withByteSize(byteSize)
        .withCompressed(compressed);
  }

  private static boolean inputIsCompressed(S3ObjectSummary objectSummary) {
    if (objectSummary.key().endsWith(".gz"))
      return true;
    if (!objectSummary.bucket().equals(defaultBucket()))
      return false;
    /*
    NOTE:
    For files that have been uploaded to the default bucket by the user without the compressed flag,
    the metadata still has to be loaded for now.
     */
    //
    HeadObjectResponse metadata = S3Client.getInstance(objectSummary.bucket()).loadMetadata(objectSummary.key());
    return metadata.contentEncoding() != null && metadata.contentEncoding().equalsIgnoreCase("gzip");
  }

  public static void activateInputsCache(String jobId) {
    inputsCacheActive.add(jobId);
  }

  public static void clearInputsCache(String jobId) {
    inputsCacheActive.remove(jobId);
    inputsCache.remove(jobId);
    metadataCache.remove(jobId);
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
      @JsonProperty String referencedJob, @JsonProperty S3Uri scannedFrom) implements XyzSerializable {}
}
