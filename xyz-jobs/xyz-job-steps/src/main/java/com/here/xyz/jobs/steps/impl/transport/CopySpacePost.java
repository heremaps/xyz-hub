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

package com.here.xyz.jobs.steps.impl.transport;

import static com.here.xyz.jobs.steps.Step.Visibility.USER;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.ExecutionMode.SYNC;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_EXECUTE;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_RESUME;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.infoLog;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.inputs.InputFromOutput;
import com.here.xyz.jobs.steps.outputs.CreatedVersion;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.resources.IOResource;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.web.XyzWebClient.ErrorResponseException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This step fetches the next unused version from source space.
 *
 * @TODO - resume
 */
public class CopySpacePost extends SpaceBasedStep<CopySpacePost> {
  public static final String STATISTICS = "statistics";
  private static final Logger logger = LogManager.getLogger();

  @JsonView({Internal.class, Static.class})
  private long copiedByteSize = 0;

  {
    setOutputSets(List.of(new OutputSet(STATISTICS, USER, true)));
  }

  @Override
  public List<Load> getNeededResources() {
    try {
      Load expectedDbLoad = new Load()
          .withResource(db())
          .withEstimatedVirtualUnits(0.0);

      //Billing, reporting
      Load expectedIoLoad = new Load().withResource(IOResource.getInstance()).withEstimatedVirtualUnits(getCopiedByteSize());

      logger.info("[{}] getNeededResources {}", getGlobalStepId(), getSpaceId());

      return List.of(expectedDbLoad, expectedIoLoad);
    }
    catch (WebClientException e) {
      //TODO: log error
      //TODO: is the step failed? Retry later? It could be a retryable error as the prior validation succeeded, depending on the type of HubWebClientException
      throw new RuntimeException(e);
    }
  }

  public long getCopiedByteSize() {
    return copiedByteSize;
  }

  public void setCopiedByteSize(long copiedByteSize) {
    this.copiedByteSize = copiedByteSize;
  }

  @Override
  public int getTimeoutSeconds() {
    return 600;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    return 5;
  }

  @Override
  public String getDescription() {
    return "Provide copy-statistics and set contentUpdatedAt field of " + getSpaceId();
  }

  @Override
  public ExecutionMode getExecutionMode() {
    return SYNC;
  }

  @Override
  public void execute(boolean resume) throws Exception {
    if (resume)
      infoLog(STEP_RESUME, this, "resume was called");
    long fetchedVersion = _getCreatedVersion();

    infoLog(STEP_EXECUTE, this, String.format("Get stats for version %d - %s", fetchedVersion, getSpaceId()));

    FeatureStatistics statistics = getCopiedFeatures(fetchedVersion);

    infoLog(STEP_EXECUTE, this, "Job Statistics: bytes=" + statistics.getByteSize() + " rows=" + statistics.getFeatureCount());
    registerOutputs(List.of(statistics), STATISTICS);

    setCopiedByteSize(statistics.getByteSize());
    if (statistics.getFeatureCount() > 0)
      writeContentUpdatedAtTs();
  }

  //TODO: Remove that workaround once the 3 copy steps were properly merged into one step again
  long _getCreatedVersion() {
    for (InputFromOutput input : (List<InputFromOutput>) (List<?>) loadInputs(InputFromOutput.class))
      if (input.getDelegate() instanceof CreatedVersion f)
        return f.getVersion();
    return 0; //FIXME: Rather throw an exception here?
  }

  private FeatureStatistics getCopiedFeatures(long fetchedVersion) throws SQLException, TooManyResourcesClaimed, WebClientException {
    String targetSchema = getSchema(db()),
        targetTable = getRootTableName(space());

    SQLQuery incVersionSql = new SQLQuery(
        """
         select count(1), coalesce( sum( (coalesce(pg_column_size(jsondata),0) + coalesce(pg_column_size(geo),0))::bigint ), 0::bigint )
         from ${schema}.${table} 
         where version = ${{fetchedVersion}} 
        """)
        .withVariable("schema", targetSchema)
        .withVariable("table", targetTable)
        .withQueryFragment("fetchedVersion", "" + fetchedVersion);

    FeatureStatistics statistics = runReadQuerySync(incVersionSql, db(), 0, rs -> rs.next()
        ? new FeatureStatistics().withFeatureCount(rs.getLong(1)).withByteSize(rs.getLong(2))
        : new FeatureStatistics());

    return statistics;
  }

  private void writeContentUpdatedAtTs() throws WebClientException {
    try {
      hubWebClient().patchSpace(getSpaceId(), Map.of("contentUpdatedAt", Core.currentTimeMillis()));
    }
    catch (ErrorResponseException e) {
      handleErrorResponse(e);
    }
  }
}
