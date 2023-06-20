/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.models.payload.events.space;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.models.payload.events.SpaceEvent;
import com.here.xyz.models.hub.pipelines.Space;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "ModifySpaceEvent")
public class ModifySpaceEvent extends SpaceEvent {

  private Operation operation;
  private Space spaceDefinition;

  @SuppressWarnings("unused")
  public Operation getOperation() {
    return this.operation;
  }

  @SuppressWarnings("WeakerAccess")
  public void setOperation(Operation operation) {
    this.operation = operation;
  }

  @SuppressWarnings("unused")
  public ModifySpaceEvent withOperation(Operation operation) {
    setOperation(operation);
    return this;
  }

  @SuppressWarnings("unused")
  public Space getSpaceDefinition() {
    return this.spaceDefinition;
  }

  public void setSpaceDefinition(Space spaceDefinition) {
    this.spaceDefinition = spaceDefinition;
  }

  @SuppressWarnings("unused")
  public ModifySpaceEvent withSpaceDefinition(Space space) {
    setSpaceDefinition(space);
    return this;
  }

  public enum Operation {
    CREATE, UPDATE, DELETE
  }
}
