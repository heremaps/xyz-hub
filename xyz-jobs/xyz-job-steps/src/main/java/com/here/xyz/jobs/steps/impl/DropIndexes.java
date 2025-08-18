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

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildLoadSpaceTableIndicesQuery;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildSpaceTableDropIndexQueries;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.jobs.steps.execution.StepException;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper.Index;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper.OnDemandIndex;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper.SystemIndex;
import com.here.xyz.util.web.XyzWebClient.WebClientException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DropIndexes extends SpaceBasedStep<DropIndexes> {
  private static final Logger logger = LogManager.getLogger();

  @JsonView({Internal.class, Static.class})
  private boolean noIndicesFound = false;

  private boolean spaceDeactivation = true;
  //If not set, the step will drop all indexes of the space.
  private List<Index> indexWhiteList;

  public List<Index> getIndexWhiteList() {
    return indexWhiteList;
  }

  public void setIndexWhiteList(List<Index> indexWhiteList) {
    this.indexWhiteList = indexWhiteList;
  }

  public DropIndexes withIndexWhiteList(List<Index> indexWhiteList) {
    setIndexWhiteList(indexWhiteList);
    return this;
  }

  public boolean isSpaceDeactivation() {
    return spaceDeactivation;
  }

  public void setSpaceDeactivation(boolean spaceDeactivation) {
    this.spaceDeactivation = spaceDeactivation;
  }

  public DropIndexes withSpaceDeactivation(boolean spaceDeactivation) {
    setSpaceDeactivation(spaceDeactivation);
    return this;
  }

  @Override
  public List<Load> getNeededResources() {
    try {
      return Collections.singletonList(new Load().withResource(db()).withEstimatedVirtualUnits(calculateNeededAcus()));
    } catch (WebClientException e) {
      //TODO: log error
      //TODO: is the step failed? Retry later? It could be a retryable error as the prior validation succeeded, depending on the type of HubWebClientException
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getTimeoutSeconds() {
    return 10 * 60;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    return 10;
  }

  @Override
  public String getDescription() {
    return "Drops all the indexes on space " + getSpaceId();
  }

  private int calculateNeededAcus() {
    return 0;
  }

  @Override
  public AsyncExecutionState getExecutionState() throws UnknownStateException {
    if (noIndicesFound)
      return AsyncExecutionState.SUCCEEDED;
    return super.getExecutionState();
  }

  @Override
  public void execute(boolean resume) throws SQLException, TooManyResourcesClaimed, WebClientException {
    try {
      logger.info("Gathering indices of space " + getSpaceId());

      //Get the list of existing indexes from database.
      List<Index> indexes = runReadQuerySync(
              buildLoadSpaceTableIndicesQuery(getSchema(db()), getRootTableName(space())), db(), calculateNeededAcus(),
              DropIndexes::getIndicesFromResultSet);

      if (indexes.isEmpty()) {
        noIndicesPresent();
      } else {
        /* Shift to */
        if (spaceDeactivation) {
          logger.info("[{}] Deactivating the space {} ...", getGlobalStepId(), getSpaceId());
          hubWebClient().patchSpace(getSpaceId(), Map.of("active", false));
        }
        String rootTableName = getRootTableName(space());

        //List of all index names excluding whitelisted indexes
        //This will drop all the indexes (system and on-demand) except for the white-listed ones
        List<String> idxNames = indexes.stream()
                .filter(index -> index != null && (indexWhiteList == null || indexWhiteList.stream().noneMatch(excludedIndex -> isSameIndex(index, excludedIndex))))
                .map(index -> index instanceof SystemIndex ? index.getIndexName(rootTableName) : index.getIndexName())
                .toList();
        if (idxNames.isEmpty()) {
          noIndicesPresent();
          return;
        }

        List<SQLQuery> dropQueries = buildSpaceTableDropIndexQueries(getSchema(db()), idxNames);
        SQLQuery dropIndexesQuery = SQLQuery.join(dropQueries, ";");
        logger.info("[{}] Dropping the following indices for space {} : {} ", getGlobalStepId(), getSpaceId(), idxNames);
        runWriteQueryAsync(dropIndexesQuery, db(), calculateNeededAcus());
      }
    } catch (Exception e) {
      throw new StepException("Error while dropping indices for space " + getSpaceId(), e).withRetryable(true);
    }
  }

  public static List<Index> getIndicesFromResultSet(ResultSet rs) throws SQLException {
    List<Index> result = new ArrayList<>();
    while (rs.next()) {
      String idxName = rs.getString("idx_name");
      String idxPropertyPath = rs.getString("idx_property");
      Character src = rs.getString("src").charAt(0);

      if (src == 's')
        result.add(SystemIndex.fromString(idxPropertyPath));
      else if (src == 'm' || src == 'a')
        result.add(new OnDemandIndex().withIndexName(idxName).withPropertyPath(idxPropertyPath));
      else
        throw new StepException("Unknown index source '" + src + "' for index '" + idxName);
    }
    return result;
  }

  private void noIndicesPresent() {
    logger.info("[{}] No Indices got found for space {}", getGlobalStepId(), getSpaceId());
    noIndicesFound = true;
  }

  private boolean isSameIndex(Index index1, Index index2) {
    if (index1 instanceof SystemIndex systemIndex1 && index2 instanceof SystemIndex systemIndex2)
      return systemIndex1 == systemIndex2;
    if (index1 instanceof OnDemandIndex onDemandIndex1 && index2 instanceof OnDemandIndex onDemandIndex2)
      return onDemandIndex1.getPropertyPath().equals(onDemandIndex2.getPropertyPath());
    return false;
  }
}
