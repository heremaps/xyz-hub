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

import static com.here.xyz.jobs.RuntimeInfo.State.FAILED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo;
import com.here.xyz.jobs.RuntimeStatus;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class JobAdminApiEmrFailureTest {

  @Test
  void parsesNativeEmrSyncFailureCause() {
    JsonObject detail = new JsonObject().put("cause", new JsonObject()
        .put("ApplicationId", "application-1")
        .put("JobRunId", "job-run-1")
        .put("Name", "job-name")
        .put("State", "FAILED")
        .put("StateDetails", "fallback details")
        .encode());

    JobAdminApi.EmrFailureContext context = JobAdminApi.parseEmrFailureContext(detail);

    assertEquals("application-1", context.applicationId());
    assertEquals("job-run-1", context.jobRunId());
    assertEquals("job-name", context.jobRunName());
    assertEquals("fallback details", context.stateDetails());
  }

  @Test
  void ignoresNonEmrAndNonFailedCauses() {
    assertNull(JobAdminApi.parseEmrFailureContext(new JsonObject().put("cause", "not JSON")));
    assertNull(JobAdminApi.parseEmrFailureContext(new JsonObject().put("cause", new JsonObject()
        .put("ApplicationId", "application-1")
        .put("JobRunId", "job-run-1")
        .put("Name", "job-name")
        .put("State", "CANCELLED")
        .encode())));
  }

  @Test
  void appliesCauseWithoutChangingExistingErrorCode() {
    RuntimeInfo target = new RuntimeInfo()
        .withErrorCode("E319500")
        .withErrorCause("old cause");
    RuntimeInfo source = new RuntimeInfo()
        .withErrorCode("E316053")
        .withErrorMessage("EMR failed")
        .withErrorCause("E316053 [profile] Configuration not found for profile.");

    JobAdminApi.applyErrorInformation(target, source);

    assertEquals("E319500", target.getErrorCode());
    assertEquals("EMR failed", target.getErrorMessage());
    assertEquals("E316053 [profile] Configuration not found for profile.", target.getErrorCause());
  }

  @Test
  void notifiesFinalizationObserverOnceAndOnlyAfterSuccessfulStatusUpdate() {
    Job job = new Job()
        .withId("job-1")
        .withStatus(new RuntimeStatus().withState(FAILED));
    AtomicInteger calls = new AtomicInteger();
    Consumer<Job> observer = ignored -> calls.incrementAndGet();
    JobService.registerJobFinalizeObserver(observer);

    try {
      Promise<Void> update = Promise.promise();
      Future<Void> result = JobAdminApi.notifyFinalizationObserversAfterStatusUpdate(job, update.future());

      assertEquals(0, calls.get());
      update.complete();
      assertEquals(1, calls.get());
      assertTrue(result.succeeded());
    }
    finally {
      JobService.deregisterJobFinalizeObserver(observer);
    }
  }

  @Test
  void doesNotNotifyFinalizationObserverWhenStatusUpdateFails() {
    Job job = new Job()
        .withId("job-1")
        .withStatus(new RuntimeStatus().withState(FAILED));
    AtomicInteger calls = new AtomicInteger();
    Consumer<Job> observer = ignored -> calls.incrementAndGet();
    JobService.registerJobFinalizeObserver(observer);

    try {
      Future<Void> result = JobAdminApi.notifyFinalizationObserversAfterStatusUpdate(job,
          Future.failedFuture("status update failed"));

      assertEquals(0, calls.get());
      assertTrue(result.failed());
    }
    finally {
      JobService.deregisterJobFinalizeObserver(observer);
    }
  }
}
