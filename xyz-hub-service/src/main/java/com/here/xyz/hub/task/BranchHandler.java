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

package com.here.xyz.hub.task;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.events.ModifyBranchEvent.Operation.CREATE;
import static com.here.xyz.events.ModifyBranchEvent.Operation.DELETE;
import static com.here.xyz.events.ModifyBranchEvent.Operation.MERGE;
import static com.here.xyz.events.ModifyBranchEvent.Operation.REBASE;
import static com.here.xyz.hub.task.FeatureTask.getReferencedBranch;
import static com.here.xyz.models.hub.Branch.MAIN_BRANCH;
import static com.here.xyz.models.hub.Branch.State.IN_CONFLICT;
import static com.here.xyz.models.hub.Ref.HEAD;

import com.here.xyz.events.ModifyBranchEvent;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.BranchConfigClient;
import com.here.xyz.hub.connectors.RpcClient;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.FeatureQueryApi;
import com.here.xyz.models.hub.Branch;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.MergedBranchResponse;
import com.here.xyz.responses.ModifiedBranchResponse;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.service.Core;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class BranchHandler {
  private static final Logger logger = LogManager.getLogger();
  public static final String NODE_ID_PREFIX = "~";
  public static final char CONFLICTING_BRANCH_ID_PREFIX = '!';

  public static Future<List<Branch>> loadBranchesOfSpace(String spaceId) {
    return Service.branchConfigClient.load(spaceId);
  }

  public static Future<Branch> loadBranch(String spaceId, String branchId) {
    if (MAIN_BRANCH.getId().equals(branchId))
      return Future.succeededFuture(MAIN_BRANCH);
    return Service.branchConfigClient.load(spaceId, branchId);
  }

  public static Future<Branch> createBranch(Marker marker, String spaceId, Branch branch) {
    if (branch.getId().startsWith("!"))
      throw new IllegalArgumentException("A branch ID may not start with the reserved \"" + CONFLICTING_BRANCH_ID_PREFIX + "\" character.");
    return upsertBranch(marker, spaceId, branch.getId(), branch);
  }

  public static Future<Branch> upsertBranch(Marker marker, String spaceId, String branchId, Branch branchUpdate) {
    return loadBranch(spaceId, branchId)
        .compose(existingBranch -> resolveRef(marker, spaceId, branchUpdate.getBaseRef())
            .compose(resolvedRef -> {
              branchUpdate.setBaseRef(resolvedRef);
              return Future.succeededFuture(existingBranch);
            }))
        .compose(existingBranch -> {
          boolean update = true;
          if (existingBranch != null)
            //Update or partial update
            update = mergePartialUpdate(existingBranch, branchUpdate);

          if (!update)
            //Nothing to do as the update would not change anything in the branch
            return Future.succeededFuture();

          if (!isValid(branchUpdate))
            return Future.failedFuture(new IllegalArgumentException("Invalid branch description. (Mandatory fields: id, baseRef) "
                + "The baseRef must point to a single version number or HEAD"));

          Future<Void> stored;
          if (existingBranch != null && existingBranch.getBaseRef().equals(branchUpdate.getBaseRef()))
            //BaseRef was not changed, so no connector call is necessary, store the updated branch
            stored = storeBranch(spaceId, branchUpdate, branchId, false);
          else
            stored = Space.resolveSpace(marker, spaceId)
                .compose(space -> space == null ? Future.failedFuture("Branch " + branchId + " cannot be created as resource "
                    + spaceId + " is not existing.") : Future.succeededFuture(space))
                //Validate that the space is no composite space, because no branches can be created on composite spaces!
                .compose(space -> space.getExtension() != null ? Future.failedFuture("Branch " + branchId
                    + " cannot be created on composite resource " + spaceId) : Future.succeededFuture(space))
                .compose(space -> Space.resolveConnector(marker, space.getStorage().getId()))
                .compose(storage -> eventForUpdate(spaceId, existingBranch, branchUpdate)
                    .compose(event -> sendEvent(marker, event, storage)))
                .compose(branchModifiedResponse -> handleBranchModifiedResponse(spaceId, branchId, branchUpdate, branchModifiedResponse));

          return stored
              //Invalidate the space to ensure the new branch will be updated inside
              .onSuccess(v -> Service.spaceConfigClient.invalidateCache(spaceId))
              .map(branchUpdate);
        });
  }

  public static Future<Branch> mergeBranch(Marker marker, String spaceId, String branchId, String targetBranchId) {
    return loadBranch(spaceId, branchId)
        .compose(sourceBranch -> loadBranch(spaceId, targetBranchId)
            .compose(targetBranch -> Space.resolveSpace(marker, spaceId)
                .compose(space -> space.resolveConnector(marker, space.getStorage().getId()))
                .compose(storage -> eventForMerge(spaceId, sourceBranch, targetBranch)
                    .compose(event -> sendEvent(marker, event, storage)))
                .compose(mergeResponse -> handleMergeResponse(spaceId, branchId, sourceBranch, targetBranch, mergeResponse)))
            .map(sourceBranch))
        .onSuccess(v -> Service.spaceConfigClient.invalidateCache(spaceId));
  }

  private static Future<Void> handleBranchModifiedResponse(String spaceId, String branchId, Branch branchUpdate,
      ModifiedBranchResponse branchModifiedResponse) {
    List<Future<Void>> futures = new ArrayList<>();
    if (branchModifiedResponse.isConflicting()) {
      //Create a new temporary conflict branch that can be used by the client to solve the conflicts
      Branch conflictingBranch = new Branch()
          .withId(CONFLICTING_BRANCH_ID_PREFIX + branchId)
          .withNodeId(branchModifiedResponse.getNodeId())
          .withBaseRef(branchModifiedResponse.getBaseRef())
          .withDescription("The branch to be used to solve conflicts of branch: " + branchId);
      Future<Void> conflictingBranchStored = storeBranch(spaceId, conflictingBranch, conflictingBranch.getId(), true);
      futures.add(conflictingBranchStored);

      /*
      Update the original branch to be in conflict state if there was a conflict during the branch operation.
      In that case, it won't be writable and no other operations can be executed on it until all conflicts have been solved.
       */
      branchUpdate.withState(branchModifiedResponse.isConflicting() ? IN_CONFLICT : null);
      branchUpdate.setConflictSolvingBranch(CONFLICTING_BRANCH_ID_PREFIX + branchId);

      Future<Void> originalBranchUpdate = storeBranch(spaceId, branchUpdate, branchId, false);
      futures.add(originalBranchUpdate);
    }
    else {
      boolean resolvePath = branchUpdate.getNodeId() != branchModifiedResponse.getNodeId();
      futures.add(storeBranch(spaceId, branchUpdate
              .withNodeId(branchModifiedResponse.getNodeId()), branchId, resolvePath));
    }

    return Future.all(futures).mapEmpty();
  }

  private static Future<Void> handleMergeResponse(String spaceId, String branchId, Branch sourceBranch, Branch targetBranch,
      ModifiedBranchResponse branchModifiedResponse) {
    if (!(branchModifiedResponse instanceof MergedBranchResponse mergeResponse))
      throw new RuntimeException("Unexpected response for merge from storage connector.");

    if (!mergeResponse.isConflicting())
      sourceBranch.addMerge(mergeResponse.getMergedSourceVersion(),
          Ref.fromBranchId(targetBranch.getId(), mergeResponse.getResolvedMergeTargetRef().getVersion()));

    return handleBranchModifiedResponse(spaceId, branchId, sourceBranch, branchModifiedResponse);
  }

  private static Future<Ref> resolveRef(Marker marker, String spaceId, Ref ref) {
    if (ref.isTag()) {
      //TODO: Also support tags
      //The ref was parsed as a tag, but it still could be depicting a branch ID, trying to resolve it ...
      Ref branchRef = Ref.fromBranchId(ref.getTag());
      return getReferencedBranch(spaceId, branchRef)
              .compose(branch -> resolveRefHeadVersion(marker, spaceId, branchRef));
    }
    return resolveRefHeadVersion(marker, spaceId, ref);
  }

  private static Future<Ref> resolveRefHeadVersion(Marker marker, String spaceId, Ref ref) {
    if (ref.isHead())
      return FeatureQueryApi.getStatistics(marker, spaceId, EXTENSION, ref, true, false)
              .map(statistics -> new Ref(ref.getBranch() + ":" + statistics.getMaxVersion().getValue()));
    else
      return Future.succeededFuture(ref);
  }

  private static Future<Void> storeBranch(String spaceId, Branch branch, String branchId, boolean resolvePath) {
    long currentTs = Core.currentTimeMillis();
    if (branch.getCreatedAt() == 0) {
      branch.setCreatedAt(currentTs);
      branch.setContentUpdatedAt(currentTs);
    }
    branch.setUpdatedAt(currentTs);
    return (resolvePath ? resolveBranchPath(spaceId, branch) : Future.succeededFuture(branch))
            .compose(resolvedBranch -> BranchConfigClient.getInstance().store(spaceId, resolvedBranch, branchId));
  }

  private static Future<Branch> resolveBranchPath(String spaceId, Branch branch) {

    Future<List<Ref>> branchPath;

    if (branch.getBaseRef().isMainBranch())
      branchPath = Future.succeededFuture(List.of(resolveToNodeIdRef(MAIN_BRANCH, branch.getBaseRef())));
    else
      branchPath = BranchConfigClient.getInstance().load(spaceId, branch.getBaseRef().getBranch())
              .compose(baseBranch -> {
                List<Ref> resolvedBranchPath = new ArrayList<>(baseBranch.getBranchPath());
                resolvedBranchPath.add(resolveToNodeIdRef(baseBranch, branch.getBaseRef()));
                return Future.succeededFuture(resolvedBranchPath);
              });

    return branchPath.compose(resolveBranchPath -> Future.succeededFuture(branch.withBranchPath(resolveBranchPath)));
  }

  private static Ref resolveToNodeIdRef(Branch branch, Ref ref) {
    if (!ref.getBranch().equals(branch.getId()))
      throw new IllegalArgumentException("The specified ref does not point to the specified branch.");

    return new Ref("~" + branch.getNodeId() + ":" + ref.getVersion()); //TODO: Implement constructor Ref(branchId, version)
  }

  public static Future<Void> deleteBranch(Marker marker, String spaceId, String branchId) {
    return loadBranch(spaceId, branchId)
        .compose(branch -> {
          if (branch == null)
            //Nothing to delete as the branch does not exist
            return Future.succeededFuture();

          return Service.branchConfigClient.delete(spaceId, branchId)
              .onSuccess(v -> {
                //Invalidate the space to ensure the deleted branch will not be listed inside anymore
                Service.spaceConfigClient.invalidateCache(spaceId);
                Space.resolveSpace(marker, spaceId)
                    .compose(space -> Space.resolveConnector(marker, space.getStorage().getId()))
                    .compose(storage -> sendEvent(marker, eventForDelete(spaceId, branch), storage))
                    .onFailure(t -> logger.error(marker, "Error updating storage after branch deletion:", t));
              });
        });
  }

  /**
   * Updates the branch structures in the storage by sending a {@link ModifyBranchEvent} to the storage connector.
   * In case of success, the response contains the new node ID of the branch which will be returned.
   *
   * @param marker A marker for logging purposes
   * @param mbe The {@link ModifyBranchEvent}
   * @param storage The storage connector which is to be contacted
   * @return A future, which holds the new node ID in case of success
   */
  private static Future<ModifiedBranchResponse> sendEvent(Marker marker, ModifyBranchEvent mbe, Connector storage) {
    Promise<XyzResponse> promise = Promise.promise();
    RpcClient.getInstanceFor(storage).execute(marker, mbe, promise);
    return promise.future()
        .compose(response -> response instanceof ErrorResponse errorResponse ? Future.failedFuture(handleConnectorError(errorResponse))
            : Future.succeededFuture((ModifiedBranchResponse) response));
  }

  private static Future<ModifyBranchEvent> eventForUpdate(String spaceId, Branch existingBranch, Branch branchUpdate) {
    return existingBranch == null
        //Branch creation
        ? loadBaseBranch(spaceId, branchUpdate)
            .compose(baseBranch -> Future.succeededFuture(eventForCreate(spaceId, baseBranch, branchUpdate)))
        //Rebase
        : loadBaseBranch(spaceId, existingBranch)
            .compose(oldBaseBranch -> loadBaseBranch(spaceId, branchUpdate)
                .compose(newBaseBranch -> Future.succeededFuture(eventForRebase(spaceId, oldBaseBranch, newBaseBranch, existingBranch, branchUpdate))));
  }

  /**
   * Loads & returns the base branch of the specified branch.
   *
   * @param branch The branch of which to load the base branch for
   * @return The loaded base branch
   */
  private static Future<Branch> loadBaseBranch(String spaceId, Branch branch) {
    return branch.getBaseRef().isMainBranch()
        ? Future.succeededFuture(MAIN_BRANCH)
        : loadBranch(spaceId, branch.getBaseRef().getBranch());
  }

  private static ModifyBranchEvent eventForCreate(String spaceId, Branch baseBranch, Branch branchToCreate) {
    return new ModifyBranchEvent()
        .withSpace(spaceId)
        .withOperation(CREATE)
        .withBaseRef(createStorageBaseRef(baseBranch, branchToCreate));
  }

  private static ModifyBranchEvent eventForRebase(String spaceId, Branch baseBranch, Branch newBaseBranch, Branch existingBranch, Branch branchUpdate) {
    return new ModifyBranchEvent()
        .withSpace(spaceId)
        .withOperation(REBASE)
        .withNodeId(branchUpdate.getNodeId())
        .withBaseRef(createStorageBaseRef(baseBranch, existingBranch))
        .withNewBaseRef(createStorageBaseRef(newBaseBranch, branchUpdate));
  }

  private static Future<ModifyBranchEvent> eventForMerge(String spaceId, Branch sourceBranch, Branch targetBranch) {
    return loadBaseBranch(spaceId, sourceBranch)
        .map(baseBranch -> new ModifyBranchEvent()
            .withSpace(spaceId)
            .withOperation(MERGE)
            .withNodeId(sourceBranch.getNodeId())
            .withBaseRef(createStorageBaseRef(baseBranch, sourceBranch))
            .withMergeTargetNodeId(targetBranch.getNodeId()));
  }

  private static ModifyBranchEvent eventForDelete(String spaceId, Branch branch) {
    return new ModifyBranchEvent()
        .withSpace(spaceId)
        .withOperation(DELETE)
        .withNodeId(branch.getNodeId());
  }

  private static Ref createStorageBaseRef(Branch baseBranch, Branch branch) {
    if (baseBranch.getNodeId() == -1)
      throw new IllegalArgumentException("The specified branch can not be used as base branch as it does not have a node ID assigned.");

    return new Ref(NODE_ID_PREFIX + baseBranch.getNodeId()
        + ":" + (branch.getBaseRef().isHead() ? HEAD : branch.getBaseRef().getVersion()));
  }

  private static Exception handleConnectorError(ErrorResponse errorResponse) {
    //TODO: Handle TIMEOUT (add check-token to response) and CONFLICT, all others => ISE
    return null;
  }

  private static boolean isValid(Branch branchToStore) {
    return branchToStore.getId() != null && branchToStore.getBaseRef() != null && branchToStore.getBaseRef().isSingleVersion();
  }

  /**
   * Transforms a potentially partial branch update object into a full branch object and determines whether the changed fields
   * should be leading to a storage update (connector call).
   *
   * @param existing The existing branch object
   * @param partialUpdate The branch object with the values to be updated
   * @return Whether the changed fields induce a storage-connector update.
   */
  private static boolean mergePartialUpdate(Branch existing, Branch partialUpdate) {
    boolean update = false;

    if (partialUpdate.getDescription() == null)
      partialUpdate.setDescription(existing.getDescription());
    else if (!partialUpdate.getDescription().equals(existing.getDescription()))
      update = true;

    if (partialUpdate.getId() == null)
      partialUpdate.setId(existing.getId());
    else if (!partialUpdate.getId().equals(existing.getId()))
      update = true;

    if (partialUpdate.getBaseRef() == null)
      partialUpdate.setBaseRef(existing.getBaseRef());
    else if (!partialUpdate.getBaseRef().equals(existing.getBaseRef()))
      update = true;

    partialUpdate.setNodeId(existing.getNodeId());
    partialUpdate.setBranchPath(existing.getBranchPath());

    return update;
  }
}
