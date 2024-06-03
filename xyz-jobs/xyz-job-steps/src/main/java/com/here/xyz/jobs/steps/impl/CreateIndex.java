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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.WRITER;
import static com.here.xyz.jobs.steps.execution.db.Database.loadDatabase;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildSpaceTableIndexQuery;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.jobs.steps.execution.db.Database;
import com.here.xyz.jobs.steps.impl.tools.ResourceAndTimeCalculator;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper.Index;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CreateIndex extends SpaceBasedStep<CreateIndex> {
  private static final Logger logger = LogManager.getLogger();
  private Index index;
  private Space space;

  @JsonView({Internal.class, Static.class})
  private int estimatedSeconds = -1;

  @Override
  public List<Load> getNeededResources() {
    try{
      double acus = ResourceAndTimeCalculator.getInstance().calculateNeededIndexAcus(getUncompressedUploadBytesEstimation(), index);
      logger.info("[{}] {} neededACUs {}", getGlobalStepId(), index, acus);

      Database db = loadDatabase(loadSpace(getSpaceId()).getStorage().getId(), WRITER);
      return Collections.singletonList(new Load().withResource(db).withEstimatedVirtualUnits(acus));
    }
    catch (WebClientException e) {
      //TODO: log error
      //TODO: is the step failed? Retry later? It could be a retryable error as the prior validation succeeded, depending on the type of HubWebClientException
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getTimeoutSeconds() {
    int timeoutSeconds = ResourceAndTimeCalculator.getInstance().calculateIndexTimeoutSeconds(getUncompressedUploadBytesEstimation(), index);
    logger.info("[{}] {} timeoutSeconds {} ({})", getGlobalStepId(), index, timeoutSeconds, getUncompressedUploadBytesEstimation());
    return timeoutSeconds;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    if (estimatedSeconds < 0) {
      estimatedSeconds = ResourceAndTimeCalculator.getInstance().calculateIndexCreationTimeInSeconds(getSpaceId(), getUncompressedUploadBytesEstimation() , index);
      logger.info("[{}] {} estimatedSeconds {}", getGlobalStepId(), index, estimatedSeconds);
    }
    return estimatedSeconds;
  }

  @Override
  public String getDescription() {
    return "Creates the " + index + " index on space " + getSpaceId();
  }

  @Override
  public void execute() throws SQLException, TooManyResourcesClaimed, WebClientException {
    logger.info("Loading space config for space " + getSpaceId());
    Space space = loadSpace(getSpaceId());
    logger.info("Getting storage database for space " + getSpaceId());
    Database db = loadDatabase(space.getStorage().getId(), WRITER);
    logger.info("Creating the index " + index + " for space " + getSpaceId() + " ...");
    runWriteQueryAsync(buildSpaceTableIndexQuery(getSchema(db), getRootTableName(space), index), db,
            ResourceAndTimeCalculator.getInstance().calculateNeededIndexAcus(getUncompressedUploadBytesEstimation(), index));
  }

  @Override
  public void resume() throws Exception {
    /*
    No cleanup needed, in any case, sending the index creation query again will work
    as it is using the "CREATE INDEX IF NOT EXISTS" semantics
     */
    execute();
  }

  public Index getIndex() {
    return index;
  }

  public void setIndex(Index index) {
    this.index = index;
  }

  public CreateIndex withIndex(Index index) {
    setIndex(index);
    return this;
  }
}
