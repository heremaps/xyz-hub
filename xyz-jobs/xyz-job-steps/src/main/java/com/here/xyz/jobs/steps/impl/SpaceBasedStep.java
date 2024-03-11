/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.getTableNameFromSpaceParamsOrSpaceId;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.execution.db.DatabaseBasedStep;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.HubWebClient.HubWebClientException;

@JsonSubTypes({
    @JsonSubTypes.Type(value = CreateIndex.class),
    @JsonSubTypes.Type(value = ImportFilesToSpace.class),
    @JsonSubTypes.Type(value = DropIndexes.class),
    @JsonSubTypes.Type(value = AnalyzeSpaceTable.class),
    @JsonSubTypes.Type(value = MarkForMaintenance.class)
})
public abstract class SpaceBasedStep<T extends SpaceBasedStep> extends DatabaseBasedStep<T> {
  private String spaceId;

  public String getSpaceId() {
    return spaceId;
  }

  public void setSpaceId(String spaceId) {
    this.spaceId = spaceId;
  }

  public T withSpaceId(String spaceId) {
    setSpaceId(spaceId);
    return (T) this;
  }

  protected final String getRootTableName(String spaceId) throws HubWebClientException {
    return getRootTableName(loadSpace(spaceId));
  }

  protected final String getRootTableName(Space space) throws HubWebClientException {
    return getTableNameFromSpaceParamsOrSpaceId(space.getStorage().getParams(), space.getId(),
        ConnectorParameters.fromMap(hubWebClient().loadConnector(space.getStorage().getId()).params).isEnableHashedSpaceId());
  }

  protected void validateSpaceExists() throws ValidationException {
    try {
      //Check if the space is actually existing
      loadSpace(getSpaceId());
    }
    catch (HubWebClientException e) {
      throw new ValidationException("Error loading resource " + getSpaceId(), e);
    }
  }

  protected Space loadSpace(String spaceId) throws HubWebClientException {
    return hubWebClient().loadSpace(spaceId);
  }

  protected StatisticsResponse loadSpaceStatistics(String spaceId, SpaceContext context) throws HubWebClientException {
    return hubWebClient().loadSpaceStatistics(spaceId, context);
  }

  protected HubWebClient hubWebClient() {
    return HubWebClient.getInstance(Config.instance.HUB_ENDPOINT);
  }

  @Override
  public boolean validate() throws ValidationException {
    validateSpaceExists();
    //Return true as no user inputs are needed
    return true;
  }
}
