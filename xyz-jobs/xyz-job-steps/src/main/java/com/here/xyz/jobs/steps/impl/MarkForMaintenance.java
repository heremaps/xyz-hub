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

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MarkForMaintenance extends SpaceBasedStep<MarkForMaintenance> {
  private static final String XYZ_CONFIG_SCHEMA = "xyz_config";
  private static final String IDX_STATUS_TABLE = "xyz_idxs_status";
  private static final Logger logger = LogManager.getLogger();
  @JsonView({Internal.class, Static.class})
  private boolean idxCreationCompleted;

  public void setIdxCreationCompleted(boolean idxCreationCompleted) {
    this.idxCreationCompleted = idxCreationCompleted;
  }

  public MarkForMaintenance withIdxCreationCompleted(boolean idxCreationCompleted) {
    setIdxCreationCompleted(idxCreationCompleted);
    return this;
  }

  public boolean isIdxCreationCompleted() {
    return idxCreationCompleted;
  }

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
    return 30 * 60;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    return 5;
  }

  @Override
  public String getDescription() {
    return "Flag space " + getSpaceId() + " to be taken into account for next maintenance schedule.";
  }

  private int calculateNeededAcus() {
    return 0;
  }

  @Override
  public void execute(boolean resume) throws WebClientException, SQLException, TooManyResourcesClaimed {
    logger.info("Analyze table of space " + getSpaceId() + " ...");
    if (idxCreationCompleted && !space().isActive()) {
      logger.info("[{}] Re-activating the space and update contentUpdatedAt {} ...", getGlobalStepId(), getSpaceId());
      hubWebClient().patchSpace(getSpaceId(), Map.of(
              "active", true,
              "contentUpdatedAt", Core.currentTimeMillis()
      ));
    }

    runReadQueryAsync(buildMarkForMaintenanceQuery(getSchema(db()), getRootTableName(space())), db(), calculateNeededAcus());
  }

  public SQLQuery buildMarkForMaintenanceQuery(String schema, String table) {
    return new SQLQuery("UPDATE ${schema}.${table} "
            + "SET idx_creation_finished = #{markAs} "
            + "WHERE spaceid = #{spaceTable} AND schem = #{spaceSchema}")
            .withVariable("schema", XYZ_CONFIG_SCHEMA)
            .withVariable("table", IDX_STATUS_TABLE)
            .withNamedParameter("spaceSchema", schema)
            .withNamedParameter("spaceTable", table)
            .withNamedParameter("markAs", idxCreationCompleted);
  }
}
