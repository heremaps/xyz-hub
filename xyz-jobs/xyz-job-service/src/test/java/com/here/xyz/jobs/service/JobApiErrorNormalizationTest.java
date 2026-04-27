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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.here.xyz.jobs.steps.JobCompiler.CompilationError;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.errors.DetailedHttpException;
import com.here.xyz.util.service.errors.ErrorManager;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
  void mapsIllegalArgumentErrorsToBadRequestWithJobId() {
    Throwable normalized = api.normalizeJobCreationError(new IllegalArgumentException("bad argument"), "job-abc");

    HttpException http = assertInstanceOf(HttpException.class, normalized);
    assertEquals(BAD_REQUEST.code(), http.status.code());
    assertEquals(Map.of("jobId", "job-abc"), http.errorDetails);
  }
}

