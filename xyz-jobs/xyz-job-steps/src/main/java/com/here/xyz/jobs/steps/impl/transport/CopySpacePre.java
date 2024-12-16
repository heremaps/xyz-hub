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
import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.WRITER;
import static com.here.xyz.jobs.steps.execution.db.Database.loadDatabase;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_EXECUTE;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.infoLog;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.jobs.steps.execution.db.Database;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.outputs.CreatedVersion;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This step fetches the next unused version from source space.
 *
 * @TODO
 * - onStateCheck
 * - resume
 * - provide output
 * - add i/o report
 * - move out parsePropertiesQuery functions
 */
public class CopySpacePre extends SpaceBasedStep<CopySpacePre> {

  private static final Logger logger = LogManager.getLogger();
  public static final String VERSION = "version";

  @JsonView({Internal.class, Static.class})
  private double overallNeededAcus = -1;

  @JsonView({Internal.class, Static.class})
  private long estimatedSourceFeatureCount = -1;

  @JsonView({Internal.class, Static.class})
  private long estimatedSourceByteSize = -1;

  @JsonView({Internal.class, Static.class})
  private long estimatedTargetFeatureCount = -1;

  @JsonView({Internal.class, Static.class})
  private int estimatedSeconds = -1;

  {
    setOutputSets(List.of(new OutputSet(VERSION, SYSTEM, true)));
  }

  @Override
  public List<Load> getNeededResources() {
    try {
      List<Load> rList  = new ArrayList<>();
      Space sourceSpace = loadSpace(getSpaceId());

      rList.add( new Load().withResource(loadDatabase(sourceSpace.getStorage().getId(), WRITER))
                           .withEstimatedVirtualUnits(calculateNeededAcus()) );

      logger.info("[{}] IncVersion #{} {}", getGlobalStepId(),
                                                           rList.size(),
                                                           sourceSpace.getStorage().getId() );

      return rList;
    }
    catch (WebClientException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getTimeoutSeconds() { return 24 * 3600; }

  @Override
  public int getEstimatedExecutionSeconds() {
    return 5;
  }

  private double calculateNeededAcus() {
    return 0.0;
  }

  @Override
  public String getDescription() {
    return "Increment Version sequence space " + getSpaceId();
  }

  @Override
  public boolean validate() throws ValidationException {
    super.validate();

    return true;
  }

  private long setVersionToNextInSequence() throws SQLException, TooManyResourcesClaimed, WebClientException {
    //TODO: Remove the following duplicated code by simply re-using GetNextVersion QueryRunner
    Space targetSpace   = loadSpace(getSpaceId());
    Database targetDb   = loadDatabase(targetSpace.getStorage().getId(), WRITER);
    String targetSchema = getSchema( targetDb ),
            targetTable = getRootTableName(targetSpace);

    SQLQuery incVersionSql = new SQLQuery("SELECT nextval('${schema}.${versionSequenceName}')")
                                  .withVariable("schema", targetSchema)
                                  .withVariable("versionSequenceName", targetTable + "_version_seq");


    long newVersion = runReadQuerySync(incVersionSql, targetDb, 0, rs -> {
      rs.next();
      return rs.getLong(1);
    });

    return newVersion;
  }

  @Override
  public ExecutionMode getExecutionMode() {
   return SYNC;
  }



  @Override
  public void execute() throws Exception {
    infoLog(STEP_EXECUTE, this, String.format("fetch next version for %s", getSpaceId()));
    registerOutputs(List.of(new CreatedVersion().withVersion(setVersionToNextInSequence())), VERSION);
  }

  @Override
  protected void onAsyncSuccess() throws WebClientException,
          SQLException, TooManyResourcesClaimed, IOException {

    logger.info("[{}] AsyncSuccess IncVersion {} ", getGlobalStepId(), getSpaceId());

  }

  @Override
  protected void onStateCheck() {
    //@TODO: Implement
    logger.info("ImlCopy.Pre - onStateCheck");
    getStatus().setEstimatedProgress(0.2f);
  }

  @Override
  public void resume() throws Exception {
    //@TODO: Implement
    logger.info("ImlCopy.Pre - onAsyncSuccess");
  }
}
