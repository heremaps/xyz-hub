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
package com.here.xyz.hub.util.health.checks;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.util.health.schema.Response;
import com.here.xyz.hub.util.health.schema.Status;
import com.here.xyz.hub.util.health.schema.Status.Result;

public class MemoryHealthCheck extends ExecutableCheck {

  public MemoryHealthCheck() {
    setName("Memory");
    setTarget(Target.LOCAL);
  }

  @Override
  public Status execute() {
    Status s = new Status().withResult(Result.OK);
    Response r = new Response();
    attachMemoryInfo(r);
    setResponse(r);
    return s;
  }

  private void attachMemoryInfo(Response r) {
    try {
      r.setAdditionalProperty("zgc", Service.IS_USING_ZGC);
      r.setAdditionalProperty("usedMemoryBytes", Service.getUsedMemoryBytes());
      r.setAdditionalProperty("usedMemoryPercent", Service.getUsedMemoryPercent());
    } catch (Exception e) {
      r.setAdditionalProperty("usedMemoryBytes", "N/A");
      r.setAdditionalProperty("usedMemoryPercent", "N/A");
    }
  }
}
