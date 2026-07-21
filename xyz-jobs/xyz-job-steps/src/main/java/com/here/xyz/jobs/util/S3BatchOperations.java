/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.jobs.util;

import com.here.xyz.jobs.steps.Config;
import com.here.xyz.util.ARN;
import com.here.xyz.util.service.aws.AwsClientFactoryBase;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3control.model.CreateJobRequest;
import software.amazon.awssdk.services.s3control.model.CreateJobResponse;
import software.amazon.awssdk.services.s3control.model.JobManifest;
import software.amazon.awssdk.services.s3control.model.JobManifestFieldName;
import software.amazon.awssdk.services.s3control.model.JobManifestFormat;
import software.amazon.awssdk.services.s3control.model.JobManifestLocation;
import software.amazon.awssdk.services.s3control.model.JobManifestSpec;
import software.amazon.awssdk.services.s3control.model.JobOperation;
import software.amazon.awssdk.services.s3control.model.JobReport;
import software.amazon.awssdk.services.s3control.model.JobReportFormat;
import software.amazon.awssdk.services.s3control.model.JobReportScope;
import software.amazon.awssdk.services.s3control.model.S3SetObjectTaggingOperation;
import software.amazon.awssdk.services.s3control.model.S3Tag;

/**
 * Helper for running <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops.html">S3 Batch Operations</a> jobs.
 */
public final class S3BatchOperations {

  private static final Logger logger = LogManager.getLogger();
  /**
   * Prefix (within the operated bucket) under which generated Batch Operations manifests are stored.
   */
  private static final String MANIFEST_PREFIX = "_batch-manifests/";
  /**
   * Prefix (within the operated bucket) under which Batch Operations completion reports are written.
   */
  private static final String REPORT_PREFIX = "_batch-reports";
  /**
   * Default S3 Batch Operations job priority (higher = more urgent).
   */
  private static final int JOB_PRIORITY = 10;
  /**
   * The tag applied to objects that should be removed by the bucket's lifecycle rule (the "DeleteScheduledForDeletion" rule in the
   * deployment template). Objects carrying this tag are expired by S3 (currently after 1 day).
   */
  public static final String SCHEDULED_FOR_DELETION_TAG_KEY = "ScheduledForDeletion";
  public static final String SCHEDULED_FOR_DELETION_TAG_VALUE = "true";

  private S3BatchOperations() {
  }

  /**
   * Schedules all objects under the given prefixes (in the jobs bucket) for deletion by tagging them with {@code ScheduledForDeletion=true}
   * via one S3 Batch Operations job. The bucket's lifecycle rule then expires them.
   *
   * @param jobId    The job whose resources are being scheduled for deletion (used for the idempotency token and job tag).
   * @param prefixes The S3 key prefixes (within the jobs bucket) whose contents should be scheduled for deletion.
   * @return The created Batch Operations job id, or {@link Optional#empty()} if there was nothing to schedule.
   */
  public static Optional<String> scheduleForDeletion(String jobId, List<String> prefixes) {
    final String bucket = Config.instance.JOBS_S3_BUCKET;
    List<String> keys = listKeys(bucket, prefixes);
    return createBatchJob(bucket, jobId, keys,
        tagObjects(Map.of(SCHEDULED_FOR_DELETION_TAG_KEY, SCHEDULED_FOR_DELETION_TAG_VALUE)),
        "Schedule " + keys.size() + " objects of job " + jobId + " for deletion",
        clientRequestToken("ScheduleForDeletion-" + jobId),
        List.of(S3Tag.builder().key("jobId").value(jobId).build()));
  }

  /**
   * Creates one S3 Batch Operations job that applies {@code operation} to every object in {@code keys}. Uploads a CSV manifest of the keys
   * to {@code bucket} (under {@value #MANIFEST_PREFIX}{@code <manifestScope>/}), then submits the job.
   *
   * @param manifestScope A folder segment (e.g. the job id) the generated manifest is grouped under.
   */
  private static Optional<String> createBatchJob(String bucket, String manifestScope, List<String> keys, JobOperation operation,
      String description, String clientRequestToken, List<S3Tag> jobTags) {
    if (keys == null || keys.isEmpty()) {
      logger.info("No objects to operate on in bucket {} - not creating a Batch Operations job ({}).", bucket, description);
      return Optional.empty();
    }

    final S3Client s3Client = S3Client.getInstance(bucket);

    //Build and upload a CSV manifest (one "bucket,key" row per object), then read back its ETag (required by CreateJob).
    String manifestKey = MANIFEST_PREFIX + manifestScope + "/" + UUID.randomUUID() + ".csv";
    try {
      s3Client.putObject(manifestKey, "text/csv", buildManifestCsv(bucket, keys).getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new RuntimeException("Unable to write S3 Batch Operations manifest " + manifestKey + " to bucket " + bucket, e);
    }
    //S3 Batch Operations rejects a quoted ETag, but S3 returns the ETag with quotes. Strip them before passing to CreateJob.
    String manifestETag = stripETagQuotes(s3Client.loadMetadata(manifestKey).eTag());

    CreateJobRequest request = buildCreateJobRequest(bucket, Config.instance.S3_BATCH_OPS_ROLE_ARN, operation, manifestKey,
        manifestETag, description, clientRequestToken, jobTags);

    if (AwsClientFactoryBase.isLocal()) {
      logger.info("Running locally - skipping S3 Batch Operations CreateJob ({} objects in bucket {}, {}). Manifest at s3://{}/{}",
          keys.size(), bucket, description, bucket, manifestKey);
      return Optional.empty();
    }

    CreateJobResponse response = AwsClientFactory.s3ControlClient().createJob(request);

    logger.info("Created S3 Batch Operations job {} over {} objects in bucket {} ({}).", response.jobId(), keys.size(),
        bucket, description);
    return Optional.of(response.jobId());
  }

  /**
   * Lists every object key under the given prefixes in {@code bucket}.
   */
  private static List<String> listKeys(String bucket, List<String> prefixes) {
    final S3Client s3Client = S3Client.getInstance(bucket);
    List<String> keys = new ArrayList<>();
    for (String prefix : prefixes) {
      keys.addAll(s3Client.listObjects(prefix));
    }
    return keys;
  }

  /**
   * Builds a {@code PutObjectTagging} operation that replaces each object's tag set with {@code tags}.
   */
  private static JobOperation tagObjects(Map<String, String> tags) {
    List<S3Tag> tagSet = tags.entrySet().stream()
        .map(entry -> S3Tag.builder().key(entry.getKey()).value(entry.getValue()).build())
        .collect(Collectors.toList());
    return JobOperation.builder()
        .s3PutObjectTagging(S3SetObjectTaggingOperation.builder().tagSet(tagSet).build())
        .build();
  }

  /**
   * Builds the CSV manifest body (one {@code bucket,key} row per object) for an S3 Batch Operations job.
   */
  private static String buildManifestCsv(String bucket, List<String> keys) {
    return keys.stream().map(key -> bucket + "," + key).collect(Collectors.joining("\n"));
  }

  /**
   * Builds the {@link CreateJobRequest} applying {@code operation} to the objects listed in the manifest.
   */
  private static CreateJobRequest buildCreateJobRequest(String bucket, String roleArn, JobOperation operation, String manifestKey,
      String manifestETag, String description, String clientRequestToken, List<S3Tag> jobTags) {
    CreateJobRequest.Builder builder = CreateJobRequest.builder()
        .accountId(accountIdOf(roleArn))
        .roleArn(roleArn)
        .priority(JOB_PRIORITY)
        //Run the job automatically (no manual confirmation step in the S3 console).
        .confirmationRequired(false)
        .clientRequestToken(clientRequestToken)
        .description(description)
        .operation(operation)
        .manifest(JobManifest.builder()
            .spec(JobManifestSpec.builder()
                .format(JobManifestFormat.S3_BATCH_OPERATIONS_CSV_20180820)
                .fields(JobManifestFieldName.BUCKET, JobManifestFieldName.KEY)
                .build())
            .location(JobManifestLocation.builder()
                .objectArn("arn:aws:s3:::" + bucket + "/" + manifestKey)
                .eTag(manifestETag)
                .build())
            .build())
        .report(JobReport.builder()
            .enabled(true)
            .bucket("arn:aws:s3:::" + bucket)
            .prefix(REPORT_PREFIX)
            .format(JobReportFormat.REPORT_CSV_20180820)
            //Only failed tasks are reported
            .reportScope(JobReportScope.FAILED_TASKS_ONLY)
            .build());

    if (jobTags != null && !jobTags.isEmpty()) {
      builder.tags(jobTags);
    }

    return builder.build();
  }

  /**
   * Strips the surrounding double quotes that S3 returns around an object ETag (e.g. {@code "abc123"} -> {@code abc123}). S3 Batch
   * Operations' manifest location expects the raw, unquoted ETag.
   */
  private static String stripETagQuotes(String eTag) {
    return eTag == null ? null : eTag.replace("\"", "");
  }

  /**
   * Deterministic idempotency token derived from jobId, so that a retried operation reuses the same token.
   */
  private static String clientRequestToken(String jobId) {
    return UUID.nameUUIDFromBytes(jobId.getBytes(StandardCharsets.UTF_8)).toString();
  }

  /**
   * Extracts the AWS account id from an IAM role ARN ({@code arn:aws:iam::<accountId>:role/<name>}), reusing the shared {@link ARN} parser.
   * S3 Batch Operations' {@code CreateJob} requires the account id to be passed explicitly (S3 Control is an account-scoped API).
   */
  private static String accountIdOf(String roleArn) {
    if (roleArn == null || !roleArn.contains(":")) {
      throw new IllegalArgumentException("Unable to derive AWS account id from S3_BATCH_OPS_ROLE_ARN: " + roleArn);
    }
    String accountId = new ARN(roleArn).getAccountId();
    if (accountId == null || accountId.isBlank()) {
      throw new IllegalArgumentException("Unable to derive AWS account id from S3_BATCH_OPS_ROLE_ARN: " + roleArn);
    }
    return accountId;
  }
}
