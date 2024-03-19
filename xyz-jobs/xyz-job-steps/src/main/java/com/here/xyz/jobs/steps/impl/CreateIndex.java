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

import com.here.xyz.jobs.steps.execution.db.Database;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper.Index;
import com.here.xyz.util.web.HubWebClient.HubWebClientException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CreateIndex extends SpaceBasedStep<CreateIndex> {
  private static final Logger logger = LogManager.getLogger();
  private Index index;
  private Space space;

  @Override
  public List<Load> getNeededResources() {
    try {
      StatisticsResponse statistics = loadSpaceStatistics(getSpaceId(), EXTENSION);
      int acus = calculateNeededAcus(statistics.getCount().getValue(), statistics.getDataSize().getValue());
      Database db = loadDatabase(loadSpace(getSpaceId()).getStorage().getId(), WRITER);

      return Collections.singletonList(new Load().withResource(db).withEstimatedVirtualUnits(acus));
    }
    catch (HubWebClientException e) {
      //TODO: log error
      //TODO: is the step failed? Retry later? It could be a retryable error as the prior validation succeeded, depending on the type of HubWebClientException
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getTimeoutSeconds() {
    return 5 * 3600; //TODO: Return value dependent on index type & feature count
  }

  @Override
  public String getDescription() {
    return "Creates the " + index + " index on space " + getSpaceId();
  }

  @Override
  public void deleteOutputs() {
    //Nothing to do here as no outputs are produced by this step
  }

  private int calculateNeededAcus(long featureCount, long byteSize) {
    //TODO: Also take into account the index type
    return 20;
  }

  @Override
  public void execute() throws SQLException, TooManyResourcesClaimed, HubWebClientException {
    logger.info("Loading space config for space " + getSpaceId());
    Space space = loadSpace(getSpaceId());
    StatisticsResponse spaceStatistics = loadSpaceStatistics(getSpaceId(), EXTENSION);
    long featureCount = spaceStatistics.getCount().getValue();
    long byteSize = spaceStatistics.getDataSize().getValue();
    logger.info("Getting storage database for space " + getSpaceId());
    Database db = loadDatabase(space.getStorage().getId(), WRITER);
    logger.info("Creating the index " + index + " for space " + getSpaceId() + " ...");
    runWriteQuery(buildSpaceTableIndexQuery(getSchema(db), getRootTableName(space), index), db, calculateNeededAcus(featureCount, byteSize));
  }

  @Override
  public void resume() throws Exception {
    /*
    No cleanup needed, in any case, sending the index creation query again will work
    as it is using the "CREATE SEQUENCE IF NOT EXISTS" semantics
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
