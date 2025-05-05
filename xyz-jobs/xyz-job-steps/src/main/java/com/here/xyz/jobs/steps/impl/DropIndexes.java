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

import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DropIndexes extends SpaceBasedStep<DropIndexes> {
  private static final Logger logger = LogManager.getLogger();

  @Override
  public List<Load> getNeededResources() {
    try {
      return Collections.singletonList(new Load().withResource(db()).withEstimatedVirtualUnits(calculateNeededAcus()));
    }
    catch (WebClientException e) {
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
  public void execute(boolean resume) throws SQLException, TooManyResourcesClaimed, WebClientException {
    logger.info("Gathering indices of space " + getSpaceId());
    List<String> indexes = runReadQuerySync(buildLoadSpaceTableIndicesQuery(getSchema(db()), getRootTableName(space())), db(), calculateNeededAcus(),
            rs -> {
              List<String> result = new ArrayList<>();
              while(rs.next())
                result.add(rs.getString(1));
              return result;
            });

    if (indexes.isEmpty()) {
      reportAsyncSuccess();
      logger.info("No indices to found. None will be dropped for space " + getSpaceId());
    }
    else {
      logger.info("[{}] Deactivating the space {} ...", getGlobalStepId(), getSpaceId());
      hubWebClient().patchSpace(getSpaceId(), Map.of("active", false));
      logger.info("Dropping the following indices for space " + getSpaceId() + ": " + indexes);
      List<SQLQuery> dropQueries = buildSpaceTableDropIndexQueries(getSchema(db()), indexes);
      SQLQuery dropIndexesQuery = SQLQuery.join(dropQueries, ";");
      runWriteQueryAsync(dropIndexesQuery, db(), calculateNeededAcus());
    }
  }
}
