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
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildLoadSpaceTableIndicesQuery;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildSpaceTableDropIndexQueries;

import com.here.xyz.jobs.steps.execution.db.Database;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DropIndexes extends SpaceBasedStep<DropIndexes> {
  private static final Logger logger = LogManager.getLogger();
  private Space space;

  @Override
  public List<Load> getNeededResources() {
    try {
      int acus = calculateNeededAcus();
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
  public void execute() throws SQLException, TooManyResourcesClaimed, WebClientException {
    logger.info("[{}] Loading space config for space {} ...", getGlobalStepId(), getSpaceId());
    Space space = loadSpace(getSpaceId());

    logger.info("[{}] Getting storage database for space {} ...", getGlobalStepId(), getSpaceId());
    Database db = loadDatabase(space.getStorage().getId(), WRITER);

    logger.info("[{}] Gathering indices of space {} ...", getGlobalStepId(), getSpaceId());
    String schema = getSchema(db);
    String rootTable = getRootTableName(space);
    List<String> indexNames = runReadQuerySync(buildLoadSpaceTableIndicesQuery(schema, rootTable), db, calculateNeededAcus(),
            rs -> {
              List<String> result = new ArrayList<>();
              while(rs.next())
                result.add(rs.getString(1));
              return result;
            });

    if (indexNames.isEmpty()) {
      reportAsyncSuccess();
      logger.info("[{}] No indices to found. None were dropped for space {}.", getGlobalStepId(), getSpaceId());
    }
    else {
      List dropQueries = new ArrayList();
      //Ensure to drop constraints
      dropQueries.add(buildDropConstraintQuery(schema, rootTable, rootTable + "_primKey"));
      dropQueries.add(buildDropConstraintQuery(schema, rootTable, rootTable + "_unique"));

      logger.info("[{}] Dropping the following indices for space {}: {} ...", getGlobalStepId(), getSpaceId(), indexNames);
      dropQueries.addAll(buildSpaceTableDropIndexQueries(schema, indexNames));
      SQLQuery dropIndexesQuery = SQLQuery.join(dropQueries, ";");
      runWriteQueryAsync(dropIndexesQuery, db, calculateNeededAcus());
    }
  }

  private static SQLQuery buildDropConstraintQuery(String schema, String rootTable, String constraintName) {
    return new SQLQuery("ALTER TABLE ${schema}.${rootTable} DROP CONSTRAINT ${constraintName}")
        .withVariable("schema", schema)
        .withVariable("rootTable", rootTable)
        .withVariable("constraintName", constraintName);
  }

  @Override
  public void resume() throws Exception {
    execute();
  }
}
