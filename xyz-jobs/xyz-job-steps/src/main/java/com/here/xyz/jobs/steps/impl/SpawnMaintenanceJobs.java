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

package com.here.xyz.jobs.steps.impl;

import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.jobs.util.StepWebClient;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.List;

import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.ExecutionMode.SYNC;

public class SpawnMaintenanceJobs extends SpaceBasedStep<SpawnMaintenanceJobs> {
  private static final Logger logger = LogManager.getLogger();

  @Override
  public ExecutionMode getExecutionMode() {
    return SYNC;
  }

  @Override
  public List<Load> getNeededResources() {
    return List.of();
  }

  @Override
  public int getTimeoutSeconds() {
    return 300;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    return 60;
  }

  @Override
  public String getDescription() {
    return "Spawn Maintenance Jobs for dependent Spaces of " + getSpaceId();
  }

  @Override
  public void execute(boolean resume) throws SQLException, TooManyResourcesClaimed, WebClientException {
    List<Space> spaces;
    try {
      Space space = HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadSpace(getSpaceId());
      if(space.getExtension() != null)
        return; // No need to maintain the space if it has an extension because it is maintained by the extension itself.

      spaces = HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadDependentSpaces(getSpaceId());
    } catch (XyzWebClient.WebClientException e) {
      if (e instanceof XyzWebClient.ErrorResponseException errorResponse && errorResponse.getStatusCode() == 404) {
        logger.info("[{}] No dependent spaces found for key {}", getGlobalStepId() , getSpaceId());
        return;
      }
      throw e;
    }

    for (Space space : spaces) {
      logger.info("[{}] Need Maintenance Child-Job for space {} ", getGlobalStepId() , space.getId());
      StepWebClient.getInstance(Config.instance.JOB_API_ENDPOINT.toString()).createJob(
              new JsonObject("""              
                    {
                        "description": "Maintain indices for the space $SPACE_ID",
                        "source": {
                            "type": "Space",
                            "id": "$SPACE_ID"
                        },
                        "process": {
                            "type": "Maintain"
                        }
                    }
                    """.replaceAll("\\$SPACE_ID", space.getId()))
      );
    }
  }
}
