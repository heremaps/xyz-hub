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

package com.here.xyz.jobs.steps.impl.maintenance;

import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.ExecutionMode.SYNC;

import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.models.hub.Branch.DeletedBranch;
import com.here.xyz.psql.query.branching.BranchManager;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DeleteBranchTable extends SpaceBasedStep<DeleteBranchTable> {
  private static final Logger logger = LogManager.getLogger();
  private DeletedBranch branch;

  @Override
  public List<Load> getNeededResources() {
    return List.of();
  }

  @Override
  public int getTimeoutSeconds() {
    return 30;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    return 5;
  }

  @Override
  public String getDescription() {
    return "Deletes the branch table of the specified branch.";
  }

  @Override
  public ExecutionMode getExecutionMode() {
    return SYNC;
  }

  public DeletedBranch getBranch() {
    return branch;
  }

  public void setBranch(DeletedBranch branch) {
    this.branch = branch;
  }

  public DeleteBranchTable withBranch(DeletedBranch branch) {
    setBranch(branch);
    return this;
  }

  private String branchTableName() throws WebClientException {
    return BranchManager.branchTableName(getRootTableName(space()), getBranch().getBranchPath().get(getBranch().getBranchPath().size() - 1),
        getBranch().getNodeId());
  }

  @Override
  public void execute(boolean resume) throws Exception {
    logger.info("[{}] Checking if the branch table can be dropped: space: {}, branch: {}, nodeId: {} ...", getGlobalStepId(), getSpaceId(),
        getBranch().getId(), getBranch().getNodeId());
    String rootTableName = getRootTableName(space());

    //Check if other branches are based on the branch node to be deleted
    BranchManager bm = new BranchManager(null, getGlobalStepId(), getSpaceId(), getSchema(db()), rootTableName);
    boolean hasBranches = runReadQuerySync(bm.buildHasBranchesQuery(getBranch().getNodeId()), db(), 0,
        rs -> rs.next());

    String branchTable = branchTableName();
    if (!hasBranches) {
      logger.info("[{}] Dropping branch table {} ...", getGlobalStepId(), branchTable);
      SQLQuery dropQuery = new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table} CASCADE;")
          .withVariable("schema", getSchema(db()))
          .withVariable("table", branchTable);
      runWriteQuerySync(dropQuery, db(), 0);

      //Erase the branch config from Hub
      HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).eraseDeletedBranch(getBranch().getUuid());
      logger.info("[{}] Branch table {} has been dropped and the according branch entry {} has been erased from Hub.",
          getGlobalStepId(), branchTable, getBranch().getUuid());
    }
    else
      logger.info("[{}] Branch table {} is still in use as base of other branches and can not be dropped. Keeping it."
          , getGlobalStepId(), branchTable);
  }
}
