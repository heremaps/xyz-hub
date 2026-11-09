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

package com.here.xyz.jobs.service;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.PRECONDITION_FAILED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.here.xyz.jobs.steps.JobCompiler.CompilationError;
import com.here.xyz.jobs.steps.inputs.InputsFromS3.BucketAccessDeniedException;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.aws.s3.S3Uri;
import com.here.xyz.util.service.errors.DetailedHttpException;
import com.here.xyz.util.service.errors.ErrorManager;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.S3Exception;

class JobApiErrorNormalizationTest {

  private final JobApi api = new JobApi();

  @BeforeAll
  static void loadErrorDefinitions() {
    try {
      ErrorManager.loadErrors("job-errors.json");
    }
    catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (!(cause instanceof IllegalStateException) || !cause.getMessage().contains("already registered"))
        throw e;
    }
  }

  @Test
  void mapsCompilationErrorsToBadRequestWithJobId() {
    Throwable normalized = api.normalizeJobCreationError(new CompilationError("broken job"), "job-123");

    DetailedHttpException detailed = assertInstanceOf(DetailedHttpException.class, normalized);
    assertEquals(BAD_REQUEST.code(), detailed.status.code());
    assertEquals("E319002", detailed.errorDefinition.getCode());
    assertNotNull(detailed.errorDetails);
    assertEquals("job-123", detailed.errorDetails.get("jobId"));
  }

  @Test
  void mapsValidationErrorsToBadRequestWithJobId() {
    Throwable normalized = api.normalizeJobCreationError(new ValidationException("invalid payload"), "job-456");

    DetailedHttpException detailed = assertInstanceOf(DetailedHttpException.class, normalized);
    assertEquals(BAD_REQUEST.code(), detailed.status.code());
    assertEquals("E319003", detailed.errorDefinition.getCode());
    assertNotNull(detailed.errorDetails);
    assertEquals("job-456", detailed.errorDetails.get("jobId"));
  }

  @Test
  void mapsNestedClassCastErrorsToBadRequestWithJobId() {
    Throwable error = new RuntimeException(new ClassCastException("layerIds must be array"));

    Throwable normalized = api.normalizeJobCreationError(error, "job-789");

    DetailedHttpException detailed = assertInstanceOf(DetailedHttpException.class, normalized);
    assertEquals(BAD_REQUEST.code(), detailed.status.code());
    assertEquals("E319003", detailed.errorDefinition.getCode());
    assertNotNull(detailed.errorDetails);
    assertEquals("job-789", detailed.errorDetails.get("jobId"));
  }

  @Test
  void mapsBucketAccessDeniedDuringInputRegistrationToPreconditionFailed() {
    Throwable normalized = api.normalizeInputRegistrationError(bucketAccessDenied(), "job-input");
    DetailedHttpException detailed = assertInstanceOf(DetailedHttpException.class, normalized);
    assertEquals(PRECONDITION_FAILED.code(), detailed.status.code());
    assertEquals("E319011", detailed.errorDefinition.getCode());
    assertEquals("Service has no permissions to access the provided bucket.", detailed.getMessage());
    assertEquals(Map.of("jobId", "job-input"), detailed.errorDetails);
  }

  @Test
  void mapsWrappedBucketAccessDeniedDuringJobCreationToPreconditionFailed() {
    Throwable error = new RuntimeException(bucketAccessDenied());
    Throwable normalized = api.normalizeJobCreationError(error, "job-inline-input");
    DetailedHttpException detailed = assertInstanceOf(DetailedHttpException.class, normalized);
    assertEquals(PRECONDITION_FAILED.code(), detailed.status.code());
    assertEquals("E319011", detailed.errorDefinition.getCode());
    assertEquals("job-inline-input", detailed.errorDetails.get("jobId"));
  }

  @Test
  void keepsOtherS3ErrorsDuringInputRegistration() {
    S3Exception error = s3Exception(404, "NoSuchBucket");
    assertSame(error, api.normalizeInputRegistrationError(error, "job-input"));
  }

  @Test
  void mapsIllegalArgumentErrorsToBadRequestWithJobId() {
    Throwable normalized = api.normalizeJobCreationError(new IllegalArgumentException("bad argument"), "job-abc");

    HttpException http = assertInstanceOf(HttpException.class, normalized);
    assertEquals(BAD_REQUEST.code(), http.status.code());
    assertEquals(Map.of("jobId", "job-abc"), http.errorDetails);
  }

  private static BucketAccessDeniedException bucketAccessDenied() {
    return new BucketAccessDeniedException(new S3Uri("some-bucket", "some/prefix/"), s3Exception(403, "AccessDenied"));
  }

  private static S3Exception s3Exception(int statusCode, String errorCode) {
    return (S3Exception) S3Exception.builder()
        .statusCode(statusCode)
        .message("S3 error " + errorCode)
        .awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build())
        .build();
  }
}

