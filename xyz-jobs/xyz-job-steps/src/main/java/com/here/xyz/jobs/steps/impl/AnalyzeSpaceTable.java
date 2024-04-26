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

import com.here.xyz.jobs.steps.execution.db.Database;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AnalyzeSpaceTable extends SpaceBasedStep<AnalyzeSpaceTable> {
  private static final Logger logger = LogManager.getLogger();
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
    //TODO: Return an estimation based on the input data size
    return 24 * 3600;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    return 10;
  }

  @Override
  public String getDescription() {
    return "Analyzes the underlying PostgreSQL table of a space after major changes in the contained data "
        + "to ensure acceptable results of internal statistics calls";
  }

  @Override
  public void resume() throws Exception {

  }

  @Override
  public void deleteOutputs() {

  }

  private int calculateNeededAcus() {
    return 0;
  }

  @Override
  public void execute() {
    logger.info("Analyze table of space " + getSpaceId() + " ...");

    try {
      logger.info("Loading space config for space {}", getSpaceId());
      Space space = loadSpace(getSpaceId());
      logger.info("Getting storage database for space {}", getSpaceId());
      Database db = loadDatabase(space.getStorage().getId(), WRITER);
      runReadQueryAsync(buildAnalyseQuery(getSchema(db), getRootTableName(space)), db, calculateNeededAcus());
    }
    catch (SQLException | TooManyResourcesClaimed | WebClientException e) {
      //@TODO: ErrorHandling! <- Is it necessary here? Anything that should be catched / transformed?
      logger.warn("Error!",e); //TODO: Can be removed, no need to log here, as the framework will log all exceptions thrown by #execute()
      throw new RuntimeException(e); //TODO: If nothing should be handled here, better rethrow the original exception
    }
  }

  public SQLQuery buildAnalyseQuery(String schema, String table) {
    return new SQLQuery("ANALYSE ${schema}.${table}")
            .withVariable("schema", schema)
            .withVariable("table", table);
  }
}
