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

import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.WRITER;
import static com.here.xyz.jobs.steps.execution.db.Database.loadDatabase;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.getTableNameFromSpaceParamsOrSpaceId;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.execution.db.Database;
import com.here.xyz.jobs.steps.execution.db.DatabaseBasedStep;
import com.here.xyz.jobs.steps.impl.transport.CopySpace;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@JsonSubTypes({
    @JsonSubTypes.Type(value = CreateIndex.class),
    @JsonSubTypes.Type(value = ExportSpaceToFiles.class),
    @JsonSubTypes.Type(value = ImportFilesToSpace.class),
    @JsonSubTypes.Type(value = DropIndexes.class),
    @JsonSubTypes.Type(value = AnalyzeSpaceTable.class),
    @JsonSubTypes.Type(value = MarkForMaintenance.class),
    @JsonSubTypes.Type(value = CopySpace.class)
})
public abstract class SpaceBasedStep<T extends SpaceBasedStep> extends DatabaseBasedStep<T> {
  private static final Logger logger = LogManager.getLogger();

  @JsonView({Internal.class, Static.class})
  private String spaceId;

  @JsonIgnore
  private Database db;

  @JsonIgnore
  private Space space;

  @JsonIgnore
  protected Space superSpace;

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

  @Override
  public void init() throws WebClientException {
    checkScripts(db());
  }

  protected final String getRootTableName(Space space) throws WebClientException {
    return getTableNameFromSpaceParamsOrSpaceId(space.getStorage().getParams(), space.getId(),
        ConnectorParameters.fromMap(hubWebClient().loadConnector(space.getStorage().getId()).params).isEnableHashedSpaceId());
  }

  protected final boolean isEnableHashedSpaceIdActivated(Space space) throws WebClientException {
    return ConnectorParameters.fromMap(hubWebClient().loadConnector(space.getStorage().getId()).params).isEnableHashedSpaceId();
  }

  protected void validateSpaceExists() throws ValidationException {
    try {
      //Check if the space is actually existing
      if (getSpaceId() == null)
        throw new ValidationException("SpaceId is missing!");
      loadSpace(getSpaceId());
    }
    catch (WebClientException e) {
      throw new ValidationException("Error loading resource " + getSpaceId(), e);
    }
  }

  protected Space loadSpace(String spaceId) throws WebClientException {
    return hubWebClient().loadSpace(spaceId);
  }

  protected StatisticsResponse loadSpaceStatistics(String spaceId, SpaceContext context) throws WebClientException {
    return hubWebClient().loadSpaceStatistics(spaceId, context);
  }

  protected Tag loadTag(String spaceId, String tagId) throws WebClientException {
    return hubWebClient().loadTag(spaceId, tagId);
  }

  protected HubWebClient hubWebClient() {
    return HubWebClient.getInstance(Config.instance.HUB_ENDPOINT);
  }

  protected Database db() throws WebClientException {
    if (db == null) {
      logger.info("[{}] Loading database for space {}.", getGlobalStepId(), getSpaceId());
      db = loadDatabase(space().getStorage().getId(), WRITER);
    }
    return db;
  }

  protected Space space() throws WebClientException {
    if (space == null) {
      logger.info("[{}] Loading space config for space {}.", getGlobalStepId(), getSpaceId());
      space = loadSpace(getSpaceId());
    }
    return space;
  }

  protected Space superSpace() throws WebClientException {
    if (superSpace == null) {
      logger.info("[{}] Loading space config for super-space {}.", getGlobalStepId(), getSpaceId());
      if (space().getExtension() == null)
        throw new IllegalStateException("The space does not extend some other space. Could not load the super space.");
      superSpace = loadSpace(space().getExtension().getSpaceId());
    }
    return superSpace;
  }

  @Override
  public boolean validate() throws ValidationException {
    validateSpaceExists();
    //Return true as no user inputs are necessary
    return true;
  }
}
