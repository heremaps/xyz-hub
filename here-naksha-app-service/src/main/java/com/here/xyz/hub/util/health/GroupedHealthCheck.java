/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.xyz.hub.util.health;

import static com.here.xyz.hub.util.health.schema.Status.Result.CRITICAL;
import static com.here.xyz.hub.util.health.schema.Status.Result.OK;
import static com.here.xyz.hub.util.health.schema.Status.Result.UNKNOWN;

import com.here.xyz.hub.util.health.checks.ExecutableCheck;
import com.here.xyz.hub.util.health.schema.Response;
import com.here.xyz.hub.util.health.schema.Status;
import com.here.xyz.hub.util.health.schema.Status.Result;
import java.util.LinkedHashSet;
import java.util.Set;

public abstract class GroupedHealthCheck extends ExecutableCheck {

  protected Set<ExecutableCheck> checks = new LinkedHashSet<>();

  /**
   * Adds another check as dependency to this GroupedHealthCheck. If this GroupedHealthCheck already has commenced the added check will
   * automatically be commenced as well. Following responses of this GroupedHealthCheck will then include also the result of the new check.
   *
   * @param c The check to be added and checked from then on
   * @return This check for chaining
   */
  public GroupedHealthCheck add(ExecutableCheck c) {
    executorService.setCorePoolSize(executorService.getCorePoolSize() + 1);
    checks.add(c);
    if (commenced) {
      c.commence();
    }
    return this;
  }

  public GroupedHealthCheck remove(ExecutableCheck c) {
    executorService.setCorePoolSize(Math.max(executorService.getCorePoolSize() - 1, MIN_EXEC_POOL_SIZE));
    if (checks.contains(c)) {
      c.quit();
      checks.remove(c);
    }
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Status execute() {
    return collectStatus();
  }

  private Status collectStatus() {
    Status status = new Status();
    Response response = new Response();

    Result r = OK;
    for (ExecutableCheck check : checks) {
      Status checkStatus = check.getStatus();
      if (check.getEssential()) {
        if (checkStatus.getResult().compareTo(UNKNOWN) >= 0) {
          r = CRITICAL;
          break;
        } else {
          r = getWorseResult(r, checkStatus.getResult());
        }
      } else if (checkStatus.getResult() != OK) {
        r = getWorseResult(r, OK /*WARNING*/); // TODO: Remove this workaround once the hc-tool was fixed.
      }
    }

    // Report the new status
    status.setResult(r);

    // Collect all checks & their responses and report the new response
    response.getChecks().addAll(checks);
    setResponse(response);

    return status;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ExecutableCheck commence() {
    if (!commenced) {
      checks.forEach(ExecutableCheck::commence);
    }
    return super.commence();
  }
}
