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
import com.here.xyz.jobs.steps.execution.StepException;
import com.here.xyz.jobs.steps.impl.tools.ResourceAndTimeCalculator;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.pg.IndexHelper;
import com.here.xyz.util.db.pg.IndexHelper.Index;
import com.here.xyz.util.db.pg.IndexHelper.OnDemandIndex;
import com.here.xyz.util.db.pg.IndexHelper.SystemIndex;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.here.xyz.util.db.pg.IndexHelper.buildSpaceTableIndexQuery;

public class CreateIndex extends SpaceBasedStep<CreateIndex> {
  private static final Logger logger = LogManager.getLogger();
  private Index index;

  @JsonView({Internal.class, Static.class})
  private int estimatedSeconds = -1;

  @Override
  public List<Load> getNeededResources() {
    try {
      double acus = ResourceAndTimeCalculator.getInstance().calculateNeededIndexAcus(getUncompressedUploadBytesEstimation(), index);
      logger.info("[{}] {} neededACUs {}", getGlobalStepId(), index, acus);

      return Collections.singletonList(new Load().withResource(db()).withEstimatedVirtualUnits(acus));
    }
    catch (WebClientException e) {
      //TODO: log error
      //TODO: is the step failed? Retry later? It could be a retryable error as the prior validation succeeded, depending on the type of HubWebClientException
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getTimeoutSeconds() {
    //int timeoutSeconds = ResourceAndTimeCalculator.getInstance().calculateIndexTimeoutSeconds(getSpaceId(), getUncompressedUploadBytesEstimation(), index);
    //logger.info("[{}] {} timeoutSeconds {} ({})", getGlobalStepId(), index, timeoutSeconds, getUncompressedUploadBytesEstimation());
    //return timeoutSeconds;
    return 24 * 3600;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    if (estimatedSeconds < 0) {
      estimatedSeconds = ResourceAndTimeCalculator.getInstance()
          .calculateIndexCreationTimeInSeconds(getSpaceId(), getUncompressedUploadBytesEstimation(), index);
      logger.info("[{}] {} estimatedSeconds {}", getGlobalStepId(), index, estimatedSeconds);
    }
    return estimatedSeconds;
  }

  @Override
  public String getDescription() {
    return "Creates the " + index + " index on space " + getSpaceId();
  }

  @Override
  public boolean validate() throws ValidationException {
    if (!(index instanceof SystemIndex || index instanceof OnDemandIndex))
      throw new StepException("Creating index " + index + " is not supported. Invalid index-type: " + index.getClass().getSimpleName());
    return super.validate();
  }

  @Override
  public void execute(boolean resume) throws SQLException, TooManyResourcesClaimed, WebClientException {
    /*
    NOTE: In case of resume, no cleanup needed, in any case, sending the index creation query again will work
    as it is using the "CREATE INDEX IF NOT EXISTS" semantics
     */
    logger.info("[{}] Creating the index {} for space {} ...", getGlobalStepId(), index, getSpaceId());
    SQLQuery indexCreationQuery = null;
    if (index instanceof SystemIndex)
      indexCreationQuery = buildSpaceTableIndexQuery(getSchema(db()), getRootTableName(space()), index);
    else if (index instanceof OnDemandIndex onDemandIndex)
      indexCreationQuery = buildOnDemandIndexCreationQuery(onDemandIndex);

    runWriteQueryAsync(indexCreationQuery, db(), ResourceAndTimeCalculator.getInstance().calculateNeededIndexAcus(getUncompressedUploadBytesEstimation(), index));
  }

  private SQLQuery buildOnDemandIndexCreationQuery(OnDemandIndex onDemandIndex) throws WebClientException, SQLException,
      TooManyResourcesClaimed {
    //return buildAsyncOnDemandIndexQuery(getSchema(db()), getRootTableName(space()), onDemandIndex.getPropertyPath());
    /*
    TODO: Remove the following workaround once the following PostgreSQL bug was fixed:
     https://postgrespro.com/list/id/18959-f63b53b864bb1417@postgresql.org
     */
    String rootTableName = getRootTableName(space());
    String schema = getSchema(db());
    List<SQLQuery> indexCreationQueries = new ArrayList<>();

    indexCreationQueries.addAll(loadPartitionNamesOf(rootTableName).stream()
        .map(partitionTableName -> new SQLQuery(IndexHelper.buildOnDemandIndexCreationQuery(schema, partitionTableName, onDemandIndex.getPropertyPath(), true).toExecutableQueryString()))
        .toList());
    indexCreationQueries.add(new SQLQuery(IndexHelper.buildOnDemandIndexCreationQuery(schema, rootTableName, onDemandIndex.getPropertyPath(),true).toExecutableQueryString()));

    return SQLQuery.join(indexCreationQueries, ";");
  }

  private List<String> loadPartitionNamesOf(String rootTableName) throws WebClientException, SQLException, TooManyResourcesClaimed {
    return runReadQuerySync(new SQLQuery("""
        SELECT
          c.relname AS partition_name
        FROM
          pg_class c
        JOIN
          pg_namespace n ON n.oid = c.relnamespace
        WHERE
          c.oid IN (SELECT inhrelid::regclass
                FROM pg_inherits
                WHERE inhparent = '${schema}.${rootTableName}'::regclass);
        """)
        .withVariable("schema", getSchema(db()))
        .withVariable("rootTableName", rootTableName), db(), 0, rs -> {
      List<String> partitionNames = new ArrayList<>();
      while (rs.next())
        partitionNames.add(rs.getString("partition_name"));
      return partitionNames;
    });
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
