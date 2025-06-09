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

import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.READER;
import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.WRITER;
import static com.here.xyz.jobs.steps.execution.db.Database.loadDatabase;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.getTableNameFromSpaceParamsOrSpaceId;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.execution.StepException;
import com.here.xyz.jobs.steps.execution.db.Database;
import com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole;
import com.here.xyz.jobs.steps.execution.db.DatabaseBasedStep;
import com.here.xyz.jobs.steps.impl.transport.CopySpace;
import com.here.xyz.jobs.steps.impl.transport.CopySpacePost;
import com.here.xyz.jobs.steps.impl.transport.CopySpacePre;
import com.here.xyz.jobs.steps.impl.transport.CountSpace;
import com.here.xyz.jobs.steps.impl.transport.ExportChangedTiles;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.ErrorResponseException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@JsonSubTypes({
    @JsonSubTypes.Type(value = CreateIndex.class),
    @JsonSubTypes.Type(value = ExportSpaceToFiles.class),
    @JsonSubTypes.Type(value = ExportChangedTiles.class),
    @JsonSubTypes.Type(value = ImportFilesToSpace.class),
    @JsonSubTypes.Type(value = DropIndexes.class),
    @JsonSubTypes.Type(value = AnalyzeSpaceTable.class),
    @JsonSubTypes.Type(value = MarkForMaintenance.class),
    @JsonSubTypes.Type(value = CopySpace.class),
    @JsonSubTypes.Type(value = CopySpacePre.class),
    @JsonSubTypes.Type(value = CopySpacePost.class),
    @JsonSubTypes.Type(value = CountSpace.class)
})
public abstract class SpaceBasedStep<T extends SpaceBasedStep> extends DatabaseBasedStep<T> {
  private static final Logger logger = LogManager.getLogger();

  @JsonView({Internal.class, Static.class})
  private String spaceId;

  @JsonIgnore
  private Map<String, Space> cachedSpaces = new ConcurrentHashMap<>();

  @JsonIgnore
  protected Space superSpace;

  @JsonIgnore
  protected StatisticsResponse spaceStatistics;

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

  protected final String getRootTableName(Space space) throws WebClientException {
    return getTableNameFromSpaceParamsOrSpaceId(space.getStorage().getParams(), space.getId(),
        ConnectorParameters.fromMap(loadConnector(space).params).isEnableHashedSpaceId());
  }

  protected final boolean isEnableHashedSpaceIdActivated(Space space) throws WebClientException {
    return ConnectorParameters.fromMap(loadConnector(space).params).isEnableHashedSpaceId();
  }

  protected Connector loadConnector(Space space) throws WebClientException {
    try {
      return hubWebClient().loadConnector(space.getStorage().getId());
    }
    catch (ErrorResponseException e) {
      return handleErrorResponse(e);
    }
  }

  protected void validateSpaceExists() throws ValidationException {
    try {
      //Check if the space is actually existing
      if (getSpaceId() == null)
        throw new ValidationException("SpaceId is missing!");
      space();
    }
    catch (WebClientException e) {
      throw new ValidationException("Error loading resource " + getSpaceId(), e);
    }
  }

  private Space loadSpace(String spaceId) throws WebClientException {
    try {
      logger.info("[{}] Loading space config for space {} ...", getGlobalStepId(), spaceId);
      return hubWebClient().loadSpace(spaceId);
    }
    catch (ErrorResponseException e) {
      return handleErrorResponse(e);
    }
  }

  protected StatisticsResponse loadSpaceStatistics(String spaceId, SpaceContext context) throws WebClientException {
    return loadSpaceStatistics(spaceId, context, false, true);
  }

  protected StatisticsResponse loadSpaceStatistics(String spaceId, SpaceContext context, boolean fastMode) throws WebClientException {
    return loadSpaceStatistics(spaceId, context, false, fastMode);
  }

  protected StatisticsResponse loadSpaceStatistics(String spaceId, SpaceContext context, boolean skipCache, boolean fastMode) throws WebClientException {
    try {
      logger.info("[{}] Loading statistics for space {} ...", getGlobalStepId(), getSpaceId());
      return hubWebClient().loadSpaceStatistics(spaceId, context, skipCache, fastMode);
    }
    catch (ErrorResponseException e) {
      return handleErrorResponse(e);
    }
  }

  protected Tag loadTag(String spaceId, String tagId) throws WebClientException {
    try {
      return hubWebClient().loadTag(spaceId, tagId);
    }
    catch (ErrorResponseException e) {
      return handleErrorResponse(e);
    }
  }

  protected HubWebClient hubWebClient() {
    return HubWebClient.getInstance(Config.instance.HUB_ENDPOINT);
  }

  /**
   * Provides the {@link Database} instance for the pre-defined space ID of this step. See: {@link #getSpaceId()}
   * The loading calls are cached; that means that later calls will not induce an actual REST request to Hub.
   * Also, the Database objects are cached. See: {@link Database#loadDatabase(String, DatabaseRole)}
   * @return The Database for the pre-defined space of this step and for the provided role
   * @throws WebClientException
   */
  protected Database db(DatabaseRole role) throws WebClientException {
    return loadDatabase(space().getStorage().getId(), role);
  }

  /**
   * Provides the default {@link Database} instance (WRITER) for the pre-defined space ID of this step. See: {@link #getSpaceId()}
   * The loading calls are cached; that means that later calls will not induce an actual REST request to Hub.
   * Also, the Database objects are cached. See: {@link Database#loadDatabase(String, DatabaseRole)}
   * @return The WRITER Database for the pre-defined space of this step
   * @throws WebClientException
   */
  protected Database db() throws WebClientException {
    return db(WRITER);
  }

  /**
   * Provides the READER {@link Database} instance for the pre-defined space ID of this step. See: {@link #getSpaceId()}
   * The loading calls are cached; that means that later calls will not induce an actual REST request to Hub.
   * Also, the Database objects are cached. See: {@link Database#loadDatabase(String, DatabaseRole)}
   * If no READER Database is found, the writer will be returned.
   * @return The READER Database for the pre-defined space of this step, or the WRITER Database if there is no READER
   * @throws WebClientException
   */
  protected Database dbReader() throws WebClientException {
    try {
      return db(READER);
    }
    catch (RuntimeException rt) {
      //TODO: Ensure that we always have a reader for all Databases and then - if there is none - it would be for a good reason, so we should not ignore that exception anymore
      if (!(rt.getCause() instanceof NoSuchElementException))
        throw rt;
    }

    return db(WRITER);
  }

  /**
   * Provides the space instance for the provided space ID.
   * The loading calls are cached; that means that later calls will not induce an actual REST request to Hub.
   * @param spaceId
   * @return
   * @throws WebClientException
   */
  protected Space space(String spaceId) throws WebClientException {
    Space space = cachedSpaces.get(spaceId);
    if (space == null)
      cachedSpaces.put(spaceId, space = loadSpace(spaceId));
    return space;
  }

  /**
   * Provides the space instance for the pre-defined space ID of this step. See: {@link #getSpaceId()}
   * The loading calls are cached; that means that later calls will not induce an actual REST request to Hub.
   * @return
   * @throws WebClientException
   */
  protected Space space() throws WebClientException {
    return space(getSpaceId());
  }

  protected StatisticsResponse spaceStatistics(SpaceContext context, boolean skipCache) throws WebClientException {
    if (spaceStatistics == null)
      spaceStatistics = loadSpaceStatistics(getSpaceId(), context, skipCache, true);
    return spaceStatistics;
  }

  protected Space superSpace() throws WebClientException {
    return space(space().getExtension().getSpaceId());
  }

  @Override
  public boolean validate() throws ValidationException {
    validateSpaceExists();
    //Return true as no user inputs are necessary
    return true;
  }

  protected <T> T handleErrorResponse(ErrorResponseException e) throws ErrorResponseException {
    if (e.getStatusCode() >= 500 || e.getStatusCode() == 429 || e.getStatusCode() == 403 || e.getStatusCode() == 401
        || e.getStatusCode() == 404)
      throw new StepException("Error requesting Hub Service" + (e.getParsedErrorResponse() != null ? ": " + e.getParsedErrorResponse().getError() + ": " + e.getParsedErrorResponse().getErrorMessage() : ""), e)
          .withCode("HTTP-" + e.getStatusCode())
          .withRetryable(true);
    throw e;
  }
}
