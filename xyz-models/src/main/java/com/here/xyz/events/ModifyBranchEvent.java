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

package com.here.xyz.events;

import com.here.xyz.models.hub.Ref;

public class ModifyBranchEvent extends Event<ModifyBranchEvent> {
  private Operation operation;

  /**
   * The node ID of the branch on which to execute the operation.
   * NOTE: For the CREATE operation, the node ID cannot be set as it is not existing yet.
   *  Instead, it will be provided as part of the {@link com.here.xyz.responses.ModifiedBranchResponse} after creation.
   */
  private int nodeId;

  /**
   * A ref of which the branch part has been resolved to the according base node ID prefixed by "~".
   * The base ref points to the node ID of the base branch this branch is branched from
   * and the branching version within that base branch.
   */
  private Ref baseRef;

  /**
   * For the REBASE operation: The new base the branch should be rebased to.
   * A ref of which the branch part has been resolved to the according base node ID prefixed by "~".
   * The new base ref points to the node ID of the base branch this branch will be branching from
   * and the branching version within that base branch.
   */
  private Ref newBaseRef;

  /**
   * For the MERGE operation: The node ID of the target branch into which to add the changes.
   */
  private int mergeTargetNodeId = -1;

  public Operation getOperation() {
    return operation;
  }

  public void setOperation(Operation operation) {
    this.operation = operation;
  }

  public ModifyBranchEvent withOperation(Operation operation) {
    setOperation(operation);
    return this;
  }

  public int getNodeId() {
    return nodeId;
  }

  public void setNodeId(int nodeId) {
    this.nodeId = nodeId;
  }

  public ModifyBranchEvent withNodeId(int nodeId) {
    setNodeId(nodeId);
    return this;
  }

  public Ref getBaseRef() {
    return baseRef;
  }

  public void setBaseRef(Ref baseRef) {
    this.baseRef = baseRef;
  }

  public ModifyBranchEvent withBaseRef(Ref baseRef) {
    setBaseRef(baseRef);
    return this;
  }

  public Ref getNewBaseRef() {
    return newBaseRef;
  }

  public void setNewBaseRef(Ref newBaseRef) {
    this.newBaseRef = newBaseRef;
  }

  public ModifyBranchEvent withNewBaseRef(Ref newBaseRef) {
    setNewBaseRef(newBaseRef);
    return this;
  }

  public int getMergeTargetNodeId() {
    return mergeTargetNodeId;
  }

  public void setMergeTargetNodeId(int mergeTargetNodeId) {
    this.mergeTargetNodeId = mergeTargetNodeId;
  }

  public ModifyBranchEvent withMergeTargetNodeId(int mergeTargetNodeId) {
    setMergeTargetNodeId(mergeTargetNodeId);
    return this;
  }

  public enum Operation {
    CREATE, DELETE, REBASE, MERGE
  }
}
