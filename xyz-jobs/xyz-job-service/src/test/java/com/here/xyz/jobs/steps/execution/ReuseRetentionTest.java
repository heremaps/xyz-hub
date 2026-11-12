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

package com.here.xyz.jobs.steps.execution;

import static com.here.xyz.jobs.steps.execution.JobExecutor.MIN_REMAINING_TIME_FOR_REUSE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.xyz.jobs.Job;
import com.here.xyz.util.service.Core;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Checks that only jobs which are kept long enough are reused (see {@link JobExecutor#keptLongEnoughForReuse}).
 */
public class ReuseRetentionTest {

  @Test
  public void jobsThatExpireSoonAreNotReused() {
    long now = Core.currentTimeMillis();
    Job expired = job("expired", now - 1_000);
    Job expiringSoon = job("expiringSoon", now + MIN_REMAINING_TIME_FOR_REUSE - TimeUnit.HOURS.toMillis(1));
    Job keptLongEnough = job("keptLongEnough", now + MIN_REMAINING_TIME_FOR_REUSE + TimeUnit.HOURS.toMillis(1));

    List<Job> reusable = JobExecutor.keptLongEnoughForReuse(List.of(expired, expiringSoon, keptLongEnough));

    assertEquals(List.of("keptLongEnough"), reusable.stream().map(Job::getId).toList());
  }

  private static Job job(String id, long keepUntil) {
    return new Job().withId(id).withKeepUntil(keepUntil);
  }
}
