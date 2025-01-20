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

import static com.here.xyz.jobs.steps.Step.Visibility.SYSTEM;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.ExecutionMode.SYNC;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_EXECUTE;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_RESUME;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.infoLog;

import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.outputs.CreatedVersion;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.sql.SQLException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This step fetches the next unused version from source space.
 *
 * @TODO - resume
 */
public class CopySpacePre extends SpaceBasedStep<CopySpacePre> {
  public static final String VERSION = "version";
  private static final Logger logger = LogManager.getLogger();

  {
    setOutputSets(List.of(new OutputSet(VERSION, SYSTEM, true)));
  }

  @Override
  public List<Load> getNeededResources() {
    try {
      Load expectedLoad = new Load()
          .withResource(db())
          .withEstimatedVirtualUnits(0.0);

      logger.info("[{}] getNeededResources {}", getGlobalStepId(), getSpaceId());

      return List.of(expectedLoad);
    }
    catch (WebClientException e) {
      //TODO: log error
      //TODO: is the step failed? Retry later? It could be a retryable error as the prior validation succeeded, depending on the type of HubWebClientException
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getTimeoutSeconds() {
    return 120;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    return 5;
  }

  @Override
  public String getDescription() {
    return "Increment version sequence of space " + getSpaceId();
  }

  @Override
  public ExecutionMode getExecutionMode() {
    return SYNC;
  }

  @Override
  public void execute(boolean resume) throws Exception {
    if (resume)
      infoLog(STEP_RESUME, this, "resume was called");
    infoLog(STEP_EXECUTE, this, String.format("Fetch next version for %s%s", getSpaceId(), resume ? " - resumed" : "") );
    registerOutputs(List.of(new CreatedVersion().withVersion(setVersionToNextInSequence())), VERSION);
  }


  private long setVersionToNextInSequence() throws SQLException, TooManyResourcesClaimed, WebClientException {
    //TODO: Remove the following duplicated code by simply re-using GetNextVersion QueryRunner
    String targetSchema = getSchema(db()),
        targetTable = getRootTableName(space());

    SQLQuery incVersionSql = new SQLQuery("SELECT nextval('${schema}.${versionSequenceName}')")
        .withVariable("schema", targetSchema)
        .withVariable("versionSequenceName", targetTable + "_version_seq");

    long newVersion = runReadQuerySync(incVersionSql, db(), 0, rs -> {
      rs.next();
      return rs.getLong(1);
    });

    return newVersion;
  }
}
