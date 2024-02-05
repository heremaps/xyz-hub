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

package com.here.xyz.models.hub;

import static com.here.xyz.models.hub.Ref.MAIN;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.XyzSerializable;
import java.util.HashMap;
import java.util.Map;

@JsonInclude(Include.NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Branch implements XyzSerializable {
  public static final Branch MAIN_BRANCH = new Branch().withId(MAIN).withNodeId(0);
  @JsonView({Public.class, Static.class})
  private String id;
  @JsonView({Public.class, Static.class})
  private Ref baseRef;
  @JsonView({Public.class, Static.class})
  private String description;
  @JsonView({Static.class})
  private int nodeId = -1;
  @JsonView({Public.class, Static.class})
  private State state;
  @JsonView({Public.class, Static.class})
  private Map<Long, Ref> merges;
  @JsonView({Public.class, Static.class})
  private String conflictSolvingBranch;

  public String getId() {
    return id;
  }

  public Branch setId(String id) {
    this.id = id;
    return this;
  }

  public Branch withId(String id) {
    setId(id);
    return this;
  }

  public Ref getBaseRef() {
    return baseRef;
  }

  public Branch setBaseRef(Ref baseRef) {
    this.baseRef = baseRef;
    return this;
  }

  public Branch withBaseRef(Ref baseRef) {
    setBaseRef(baseRef);
    return this;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public Branch withDescription(String description) {
    setDescription(description);
    return this;
  }

  public int getNodeId() {
    return nodeId;
  }

  public Branch setNodeId(int nodeId) {
    this.nodeId = nodeId;
    return this;
  }

  public Branch withNodeId(int nodeId) {
    setNodeId(nodeId);
    return this;
  }

  public State getState() {
    return state;
  }

  public void setState(State state) {
    this.state = state;
  }

  public Branch withState(State state) {
    setState(state);
    return this;
  }

  public Map<Long, Ref> getMerges() {
    return merges;
  }

  public void setMerges(Map<Long, Ref> merges) {
    this.merges = merges;
  }

  public Branch withMerges(Map<Long, Ref> merges) {
    setMerges(merges);
    return this;
  }

  public Branch addMerge(long version, Ref targetRef) {
    if (getMerges() == null)
      setMerges(new HashMap<>());
    getMerges().put(version, targetRef);
    return this;
  }

  public String getConflictSolvingBranch() {
    return conflictSolvingBranch;
  }

  public void setConflictSolvingBranch(String conflictSolvingBranch) {
    this.conflictSolvingBranch = conflictSolvingBranch;
  }

  public Branch withConflictSolvingBranch(String conflictSolvingBranch) {
    setConflictSolvingBranch(conflictSolvingBranch);
    return this;
  }

  public enum State {
    IN_CONFLICT
  }
}
