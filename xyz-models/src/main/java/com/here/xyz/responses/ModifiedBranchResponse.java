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

package com.here.xyz.responses;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.xyz.models.hub.Ref;

@JsonSubTypes({
    @JsonSubTypes.Type(value = MergedBranchResponse.class, name = "MergedBranchResponse")
})
public class ModifiedBranchResponse extends XyzResponse<ModifiedBranchResponse> {
  private int nodeId;
  private Ref baseRef;
  private boolean conflicting;

  public int getNodeId() {
    return nodeId;
  }

  public void setNodeId(int nodeId) {
    this.nodeId = nodeId;
  }

  public ModifiedBranchResponse withNodeId(int nodeId) {
    setNodeId(nodeId);
    return this;
  }

  public Ref getBaseRef() {
    return baseRef;
  }

  public void setBaseRef(Ref baseRef) {
    this.baseRef = baseRef;
  }

  public ModifiedBranchResponse withBaseRef(Ref baseRef) {
    setBaseRef(baseRef);
    return this;
  }

  public boolean isConflicting() {
    return conflicting;
  }

  public void setConflicting(boolean conflicting) {
    this.conflicting = conflicting;
  }

  public ModifiedBranchResponse withConflicting(boolean conflicting) {
    setConflicting(conflicting);
    return this;
  }
}
