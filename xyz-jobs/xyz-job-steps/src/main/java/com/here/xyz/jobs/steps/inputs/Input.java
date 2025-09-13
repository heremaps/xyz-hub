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

import static com.here.xyz.jobs.steps.Step.InputSet.DEFAULT_SET_GROUP;
import static com.here.xyz.jobs.steps.Step.InputSet.DEFAULT_SET_NAME;
import static com.here.xyz.jobs.util.S3Client.getBucketFromS3Uri;
import static com.here.xyz.jobs.util.S3Client.getKeyFromS3Uri;

import com.amazonaws.AmazonServiceException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.outputs.GroupSummary;
import com.here.xyz.jobs.steps.outputs.GroupedPayloadsPreview;
import com.here.xyz.jobs.steps.outputs.SetSummary;
import com.here.xyz.jobs.steps.payloads.StepPayload;
import com.here.xyz.jobs.util.S3Client;
import com.here.xyz.util.pagination.Page;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.aws.s3.S3ObjectSummary;
import com.here.xyz.util.service.aws.s3.S3Uri;
import java.io.IOException;
import java.util.HashMap;
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
  private static Map<String, Map<String, Map<String, InputsMetadata>>> metadataCache = new WeakHashMap<>();
  private static Map<String, Map<String, Map<String, List<Input>>>> inputsCache = new WeakHashMap<>(); //TODO: Expire keys after <24h
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

  public static GroupedPayloadsPreview previewInputs(String jobId) {
    ensureInputsLoaded(jobId);
    Map<String, Map<String, InputsMetadata>> cachedGroups = metadataCache.getOrDefault(jobId, Map.of());

    Map<String, GroupSummary> responseGroups = new ConcurrentHashMap<>();
    cachedGroups.forEach((groupName, metadataMap) -> {
      Map<String, SetSummary> sets = summarizeSets(metadataMap);
      responseGroups.put(groupName, groupSummaryOf(sets));
    });

    long totalBytes = responseGroups.values().stream().mapToLong(GroupSummary::getByteSize).sum();
    long totalGroups = responseGroups.values().stream().mapToLong(it -> it.getItems().size()).sum();
    return new GroupedPayloadsPreview()
        .withItems(responseGroups)
        .withByteSize(totalBytes)
        .withItemCount(totalGroups);
  }

  public static GroupSummary previewInputGroups(String jobId, String outputSetGroup) {
    ensureInputsLoaded(jobId);
    Map<String, Map<String, InputsMetadata>> cachedGroups = metadataCache.get(jobId);

    Map<String, InputsMetadata> group = cachedGroups.get(outputSetGroup);
    if (group == null) {
      return emptyGroupSummary();
    }

    Map<String, SetSummary> sets = summarizeSets(group);
    return groupSummaryOf(sets);
  }

  private static void ensureInputsLoaded(String jobId) {
    if (!inputsCacheActive.contains(jobId) || !metadataCache.containsKey(jobId)) {
      loadInputs(jobId, DEFAULT_SET_NAME);
    }
  }

  private static Map<String, SetSummary> summarizeSets(Map<String, InputsMetadata> metadataMap) {
    Map<String, SetSummary> result = new ConcurrentHashMap<>();
    metadataMap.forEach((setName, metadata) -> {
      long totalSize = metadata.inputs.values().stream()
          .mapToLong(im -> im.byteSize)
          .sum();
      int totalItems = metadata.inputs.size();
      result.put(setName, new SetSummary()
          .withItemCount(totalItems)
          .withByteSize(totalSize));
    });
    return result;
  }

  private static GroupSummary groupSummaryOf(Map<String, SetSummary> sets) {
    long byteSize = sets.values().stream().mapToLong(SetSummary::getByteSize).sum();
    int itemCount = sets.size();
    return new GroupSummary()
        .withItems(sets)
        .withByteSize(byteSize)
        .withItemCount(itemCount);
  }

  private static GroupSummary emptyGroupSummary() {
    return new GroupSummary()
        .withItems(new ConcurrentHashMap<>())
        .withByteSize(0L)
        .withItemCount(0);
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

  public static List<Input> loadInputs(String jobId, String setName) {
    return loadInputs(jobId, setName, DEFAULT_SET_GROUP, null, -1).getItems();
  }

  public static Page<Input> loadInputs(String jobId, String setName, String outputSetGroup, String nextPageToken, int limit) {
    // Only cache inputs of jobs which are submitted already
    // we will cache data only when no pagination is requested
    if (inputsCacheActive.contains(jobId) && nextPageToken == null && limit == -1) {
      List<Input> inputs = getFromInputCache(jobId, setName, outputSetGroup);
      if (inputs == null) {
        inputs = loadInputsAndWriteMetadata(jobId, setName, outputSetGroup,-1, null, Input.class).getItems();
        putToInputCache(jobId, setName, outputSetGroup, inputs);
      }
      return new Page<>(inputs);
    }
    return loadInputsAndWriteMetadata(jobId, setName, outputSetGroup, limit, nextPageToken, Input.class);
  }

  private synchronized static void putToInputCache(String jobId, String setName, String outputSetGroup, List<Input> inputs) {
    Map<String, Map<String, List<Input>>> cachedInputs = inputsCache.get(jobId);
    if (cachedInputs == null)
      cachedInputs = new ConcurrentHashMap<>();
    cachedInputs.computeIfAbsent(setName, k -> new HashMap<>()).put(outputSetGroup, inputs);
    inputsCache.put(jobId, cachedInputs);
  }

  private static List<Input> getFromInputCache(String jobId, String setName, String outputSetGroup) {
    Map<String, Map<String, List<Input>>> groups = inputsCache.get(jobId);
    if (groups == null) {
      return null;
    }
    Map<String, List<Input>> inputs = groups.get(outputSetGroup);
    return inputs == null ? null : inputs.get(setName);
  }

  private synchronized static void putToMetadataCache(String jobId, String setName, String outputSetGroup, InputsMetadata metadata) {
    Map<String, Map<String, InputsMetadata>> cachedGroups = metadataCache.get(jobId);
    if(cachedGroups == null)
      cachedGroups = new ConcurrentHashMap<>();
    cachedGroups.computeIfAbsent(outputSetGroup, k -> new ConcurrentHashMap<>()).put(setName, metadata);
    metadataCache.put(jobId, cachedGroups);
  }

  private static InputsMetadata getFromMetadataCache(String jobId, String setName, String outputSetGroup) {
    Map<String, Map<String, InputsMetadata>> groups = metadataCache.get(jobId);
    if (groups == null) {
      return null;
    }
    if (DEFAULT_SET_GROUP.equals(outputSetGroup)) {
      for (Map<String, InputsMetadata> group : groups.values()) {
        InputsMetadata metadata = group.get(setName);
        if (metadata != null) {
          return metadata;
        }
      }
    }
    Map<String, InputsMetadata> metadata = groups.get(outputSetGroup);
    return metadata == null ? null : metadata.get(setName);
  }

  private static <T extends Input> Page<T> loadInputsAndWriteMetadata(String jobId, String setName, String outputSetGroup, int limit, String nextPageToken, Class<T> inputType) {
    try {
      InputsMetadata metadata = loadMetadata(jobId, setName, outputSetGroup);
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

      return new Page<T>((limit > 0 ? inputs.unordered().limit(limit) : inputs).toList());
    }
    catch (IOException | S3Exception ignore) {}

    final Page<T> inputs = loadInputsInParallel(defaultBucket(), inputS3Prefix(jobId, setName), limit, nextPageToken, inputType);
    //Only write metadata of jobs which are submitted already
    if (inputs != null && inputs.size() > 0 && inputsCacheActive.contains(jobId) && nextPageToken == null)
      storeMetadata(jobId, (List<Input>) inputs.getItems(), setName);

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
    return loadMetadataIfExists(jobId, setName, DEFAULT_SET_GROUP);
  }

  private static Optional<InputsMetadata> loadMetadataIfExists(String jobId, String setName, String outputSetGroup) {
    try {
      return Optional.of(loadMetadata(jobId, setName, outputSetGroup));
    }
    catch (IOException | S3Exception e) {
      return Optional.empty();
    }
  }

  static final InputsMetadata loadMetadata(String jobId, String setName) throws IOException, S3Exception {
    return loadMetadata(jobId, setName, DEFAULT_SET_GROUP);
  }

  static final InputsMetadata loadMetadata(String jobId, String setName, String outputSetGroup) throws IOException, S3Exception {
    InputsMetadata metadata = getFromMetadataCache(jobId, setName, outputSetGroup);
    if (metadata != null)
      return metadata;

    logger.info("Loading metadata from S3 for job {} ...", jobId);
    long t1 = Core.currentTimeMillis();
    metadata = XyzSerializable.deserialize(S3Client.getInstance().loadObjectContent(inputMetaS3Key(jobId, setName)),
        InputsMetadata.class);
    logger.info("Loaded metadata for job {}. Took {}ms ...", jobId, Core.currentTimeMillis() - t1);
    if (inputsCacheActive.contains(jobId))
      putToMetadataCache(jobId, setName, outputSetGroup, metadata);

    return metadata;
  }

  static final void addInputReferences(String referencedJobId, String referencingJobId, String setName) throws IOException,
          S3Exception {
    InputsMetadata referencedMetadata = loadMetadata(referencedJobId, setName);
    //Add the referencing job to the list of jobs referencing the metadata
    referencedMetadata.referencingJobs().add(referencingJobId);
    storeMetadata(referencedJobId, referencedMetadata, setName, DEFAULT_SET_GROUP);
  }

  static final void storeMetadata(String jobId, InputsMetadata metadata, String setName, String outputSetGroup) {
    try {
      if (inputsCacheActive.contains(jobId))
        putToMetadataCache(jobId, setName, outputSetGroup, metadata);
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
    storeMetadata(jobId, inputs, referencedJobId, new S3Uri(defaultBucket(), inputS3Prefix(jobId, setName)), setName, DEFAULT_SET_GROUP);
  }

  static final void storeMetadata(String forJob, List<Input> inputs, String referencedJobId, S3Uri s3Uri, String setName) {
    storeMetadata(forJob, inputs, referencedJobId, s3Uri, setName, DEFAULT_SET_GROUP);
  }

  static final void storeMetadata(String jobId, List<Input> inputs, String referencedJobId, S3Uri scannedFrom, String setName, String outputSetGroup) {
    logger.info("Storing inputs metadata for job {} ...", jobId);
    Map<String, InputMetadata> metadata = inputs.stream()
        .collect(Collectors.toMap(input -> (input.s3Bucket == null ? "" : "s3://" + input.s3Bucket + "/") + input.s3Key,
            input -> new InputMetadata(input.byteSize, input.compressed)));
    storeMetadata(jobId, new InputsMetadata(metadata, new HashSet<>(Set.of(jobId)), referencedJobId, scannedFrom), setName, outputSetGroup);
  }

  static final List<Input> loadInputsInParallel(String bucketName, String inputS3Prefix) {
    return loadInputsInParallel(bucketName, inputS3Prefix, -1, null, Input.class).getItems();
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
    return (int) loadInputs(jobId, setName).stream().filter(input -> inputType.isAssignableFrom(input.getClass())).count();
  }

  public static <T extends Input> List<T> loadInputsSample(String jobId, String setName, int limit, Class<T> inputType) {
    // requesting the first page and unwrap to list
    return loadInputsAndWriteMetadata(jobId, setName, DEFAULT_SET_GROUP, limit, null, inputType).getItems();
  }

  private static <T extends Input> Page<T> loadAndTransformInputs(String bucketName, String inputS3Prefix, int limit, String nextPageToken, Class<T> inputType) {
    Page<S3ObjectSummary> page = S3Client.getInstance(bucketName).scanFolder(inputS3Prefix, nextPageToken, limit);
    Stream<Input> inputsStream = page.getItems()
        .parallelStream()
        .map(s3ObjectSummary -> createInput(defaultBucket().equals(bucketName) ? null : bucketName, s3ObjectSummary.key(),
            s3ObjectSummary.size(), inputIsCompressed(s3ObjectSummary)))
        .filter(input -> input.getByteSize() > 0 && inputType.isAssignableFrom(input.getClass()));

    if (limit > 0 && nextPageToken == null)
      inputsStream = inputsStream.unordered().limit(limit);

    return new Page<>((List<T>) inputsStream.collect(Collectors.toList()), page.getNextPageToken());
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
      storeMetadata(owningJobId, metadata, setName, DEFAULT_SET_GROUP);
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
