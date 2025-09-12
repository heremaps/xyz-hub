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

package com.here.xyz.hub.config;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.dynamo.DynamoBranchConfigClient;
import com.here.xyz.models.hub.Branch;
import com.here.xyz.util.service.Initializable;
import io.vertx.core.Future;
import java.util.List;

public abstract class BranchConfigClient implements Initializable {
  public static BranchConfigClient getInstance() {
    return new DynamoBranchConfigClient(Service.configuration.BRANCHES_DYNAMODB_TABLE_ARN);
  }

  /**
   * Stores (creates / updates) a branch.
   * In case of an ID change, the method {@link #store(String, Branch, String)} must be used instead.
   *
   * NOTE: As this is a write operation that indirectly has influence on the cached space item, always make sure to invalidate the
   *  cache for the specified space afterward.
   *
   * @param spaceId The id of the space for which to create / update the branch
   * @param branch The branch to be stored
   * @return A future which will complete successfully when the operation was successfully performed
   */
  public Future<Void> store(String spaceId, Branch branch) {
    return store(spaceId, branch, branch.getId());
  }

  /**
   * Stores (creates / updates) a branch.
   * In case of an ID change, the second parameter has to be set to the old branch ID.
   * In all other cases, it should be simply the current branch ID.
   *
   * NOTE: As this is a write operation that indirectly has influence on the cached space item, always make sure to invalidate the
   *  cache for the specified space afterward.
   *
   * @param spaceId The id of the space for which to create / update the branch
   * @param branch The branch to be stored
   * @param branchId The old branch ID in case it was changed
   * @return A future which will complete successfully when the operation was successfully performed
   */
  public abstract Future<Void> store(String spaceId, Branch branch, String branchId);

  /**
   * Loads & returns all branches for the specified space.
   *
   * @param spaceId The id of the space for which to load all branches
   * @return All branches of the space
   */
  public abstract Future<List<Branch>> load(String spaceId);

  /**
   * Loads a specific branch of a space.
   *
   * @param spaceId The id of the space for which to load the specified branch
   * @param branchId The id of the branch to be loaded
   * @return The specified branch
   */
  public abstract Future<Branch> load(String spaceId, String branchId);

  /**
   * Loads & returns all branches having the specified branch ID.
   * NOTE: There could be multiple branches having the same ID but belonging to different spaces.
   * @param branchId The branch ID for which to find the branches
   * @return All branch existing objects that are having the specified branch ID
   */
  public abstract Future<List<Branch>> loadBranches(String branchId);

  /**
   * Deletes a specific branch of a space.
   *
   * NOTE: As this is a write operation that indirectly has influence on the cached space item, always make sure to invalidate the
   *  cache for the specified space afterward.
   *
   * @param spaceId The id of the space for which to delete the specified branch
   * @param branchId The id of the branch to be deleted
   * @return
   */
  public abstract Future<Void> delete(String spaceId, String branchId);
}
