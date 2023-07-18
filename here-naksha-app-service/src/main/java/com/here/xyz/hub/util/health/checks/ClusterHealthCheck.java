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

import static com.here.xyz.hub.util.health.schema.Status.Result.ERROR;
import static com.here.xyz.hub.util.health.schema.Status.Result.OK;

import com.here.xyz.hub.rest.admin.Node;
import com.here.xyz.hub.util.health.schema.Response;
import com.here.xyz.hub.util.health.schema.Status;

public class ClusterHealthCheck extends ExecutableCheck {

  public ClusterHealthCheck() {
    setName("Cluster");
    setTarget(Target.REMOTE);
  }

  @Override
  public Status execute() {
    Status s = new Status().withResult(OK);
    Response r = new Response();
    try {
      r.setAdditionalProperty("nodes", Node.getClusterNodes());
      s.setResult(OK);
    } catch (Exception e) {
      r.setMessage("Error: " + e.getMessage());
      s.setResult(ERROR);
    }
    setResponse(r);
    return s;
  }
}
