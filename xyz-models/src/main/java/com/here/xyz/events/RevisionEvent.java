/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "RevisionEvent")
public class RevisionEvent extends Event<RevisionEvent> {
  private Operation operation;
  private PropertyQuery revision;

  public Operation getOperation() {
    return operation;
  }

  public void setOperation(Operation operation) {
    this.operation = operation;
  }

  public RevisionEvent withOperation(Operation operation) {
    setOperation(operation);
    return this;
  }

  public PropertyQuery getRevision() {
    return revision;
  }

  public void setRevision(PropertyQuery revision) {
    this.revision = revision;
  }

  public RevisionEvent withRevision(PropertyQuery revision) {
    setRevision(revision);
    return this;
  }

  public enum Operation {
    DELETE
  }
}
